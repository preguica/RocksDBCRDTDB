// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
package mvdb

import (
	"errors"
	"rockscrdtdb/common"
	"encoding/json"
	"rockscrdtdb/utils"
	"rockscrdtdb/opcrdts"
	"github.com/tecbot/gorocksdb"
	"sync"
	"fmt"
)

type CRDTMvDBPreferences struct {
	StoreLast bool
	BuildAny bool
	ThresholdForCreatingStable int
}

func (obj *CRDTMvDBPreferences)Serialize() ([]byte, bool) {
	b, err := json.Marshal(*obj)
	if err != nil {
		return nil, false
	} else {
		return b, true
	}
}


func (obj *CRDTMvDBPreferences)Unserialize( b []byte) (bool) {
	err := json.Unmarshal( b, obj)
	return err != nil
}

// Holds a reference to an open database. Depending on the options, this database may be able to retrieve
// only the latest version, or a version that satisfies the given timestamp
// OPTION: storeLast = true
// In this case, the database uses rocksdb merge operator to build the latest version internally.
// The object returned in a get includes a version vector that has, for each entry, the largest operation
// executed.
// When adding a new operation, the merge function of RocksDB is used.
// The object is stored in key:
// key = [MD5(key)][CRDT_RESERVED_LAST][crdt_type]
// OPTION: buildAny = true
// In this case, the database will reconstruct any version (besides version compatible with the latest) by
// replaying the list of operations from a stable version.
// When adding a new operation, the operation is stored in the following key:
// key = [MD5(key)][CRDT_RESERVED_OPS][8_byte_time][dc_id]
// The stable version is stored under key:
// key = [MD5(key)][CRDT_RESERVED_STABLE][8_byte_time][dc_id]
type CRDTMvDB struct {
	log *common.Log
	prefs *CRDTMvDBPreferences
	stableVV *utils.VersionVector
	stableMux sync.Mutex
}



// Opens a database stored in the given filename
// storeLast : true if the last version should be kept for fast access
// buildAny : true if it should be able to create any version
func OpenCreateIfNotCRDTMvDB( dbName string, storeLast bool, buildAny bool, thresholdForCreatingStable int) (*CRDTMvDB, error) {
	if storeLast == false && buildAny == false {
		return nil, errors.New("Invalid options for creating a database")
	}
	log, err := common.OpenLogExtended( dbName, func(options *gorocksdb.Options, blockOptions *gorocksdb.BlockBasedTableOptions) {
		options.SetCreateIfMissing(true)
		options.SetMergeOperator( NewMvDbMerger())
		options.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(KEY_PREFIX_SIZE))
		blockOptions.SetCacheIndexAndFilterBlocks( true)
		blockOptions.SetIndexType( gorocksdb.KHashSearchIndexType)
		blockOptions.SetFilterPolicy( gorocksdb.NewBloomFilter(10))
	})
	b,err := log.Get( []byte("__mvdb_preferences"));
	prefs := &CRDTMvDBPreferences{storeLast, buildAny, thresholdForCreatingStable}
	if b.Data() == nil || err != nil {
		bb,ok := prefs.Serialize()
		if ok == false {
			return nil, errors.New( "Cannot save database preferences.")
		}
		err = log.Put( []byte("__mvdb_preferences"), bb)
		if err != nil {
			return nil, errors.New( "Cannot save database preferences : ")
		}
	} else {
		prefs.Unserialize(b.Data())
		if prefs.BuildAny != buildAny || prefs.StoreLast != storeLast {
			return nil, errors.New( "Cannot change the preferences set when the database was created.")
		}
	}

	return &CRDTMvDB{log: log, prefs: prefs, stableVV: utils.NewVersionVector()}, err
}


// Opens a database stored in the given filename
func OpenCRDTMvDB( dbName string) (*CRDTMvDB, error) {
	log, err := common.OpenLog( dbName, false, NewMvDbMerger())
	if err != nil {
		return nil, errors.New( "Database does not exist")
	}
	b,err := log.Get( []byte("__mvdb_preferences"));
	prefs := &CRDTMvDBPreferences{}
	if b.Data() == nil || err != nil {
		return nil, errors.New( "Cannot retrieve database preferences.")
	} else {
		prefs.Unserialize(b.Data())
	}
	return &CRDTMvDB{log: log, prefs: prefs, stableVV: utils.NewVersionVector()}, err
}

// Closes an opened database
func (db *CRDTMvDB) Close() {
	defer db.log.Close()
}

// Set new stable version. No version prior to this one will ever be retrieved.
func (db *CRDTMvDB) setStableVersion( vv *utils.VersionVector) {
	db.stableMux.Lock()
	newStable := utils.NewVersionVectorVV( db.stableVV)
	newStable.PointwiseMax( vv)
	db.stableVV = newStable
	db.stableMux.Unlock()
}


// Returns the latest version. Assume key is preencoded
func (db *CRDTMvDB) getLatest( t byte, key []byte) (*MvDBCRDT,error) {
	key = createKeyWithBase( t, key)
	val,err := db.log.Get(key)
	if val.Data() == nil || err != nil {
		return nil, common.NewNoObjectError( string(key))
	}
	obj,ok := UnserializeMvDBCRDT( val.Data())
	if ok == false {
		return nil, errors.New("unserialize error")
	} else {
		return obj, nil
	}

}

// Returns the latest version. Assume key is preencoded
func (db *CRDTMvDB) getStable( t byte, key []byte) *MvDBCRDT {
	key = createStableKeyWithBase( t, key)
	val,err := db.log.Get(key)
	if val.Data() == nil || err != nil {
		o := opcrdts.FunCRDTNew[t]()
		obj := NewMvDBCRDT( o, utils.NewVersionVector())
		return obj
	} else {
		obj, ok := UnserializeMvDBCRDT( val.Data())
		if ! ok {
			o := opcrdts.FunCRDTNew[t]()
			obj = NewMvDBCRDT( o, utils.NewVersionVector())
		}
		return obj
	}
}

// Returns the latest version. Assume key is preencoded
func (db *CRDTMvDB) buildVersion( t byte, key []byte, vv *utils.VersionVector) (*MvDBCRDT, error) {
	obj := db.getStable( t, key)
	it := db.log.NewIterator()
	defer it.Close()
	search := createBaseOpKeyWithBase(key)
	count := 0
	db.stableMux.Lock()
	localStableVV := db.stableVV
	db.stableMux.Unlock()
	for it.Seek(search); it.ValidForPrefix( search); it.Next() {
		op, ok := UnserializeMvDBCRDTOperation( it.Value().Data())
		if ! ok {
			return nil,errors.New("Cannot unserialize operation : " + string(it.Key().Data()))
		}
		if (vv == nil || vv.IncludesTS( op.Ts)) && ! obj.Vv.IncludesTS(op.Ts) {
			obj.Obj.Apply(op.Op)
			obj.Vv.PointwiseMax( op.Vv)
		}
		if localStableVV.IncludesTS( op.Ts) {
			count++
		}
	}
	if count >= db.prefs.ThresholdForCreatingStable {
		if obj.Vv.SmallerOrEqual( localStableVV) {
			b,ok := obj.Serialize()
			if ok == true {
				fmt.Println( "Saving stable")
				key = createStableKeyWithBase( t, key)
				go func () {
					db.log.Put(key, b)
					it := db.log.NewIterator()
					defer it.Close()
					search := createBaseOpKeyWithBase(key)
					for it.Seek(search); it.ValidForPrefix( search); it.Next() {
						ets, eok := extractTimestamp( it.Key().Data())
						if eok && localStableVV.IncludesTS( ets) {
							db.log.Delete( it.Key().Data())
						}
					}
				}()
			}
		}
		//TODO: build stable version when built version is no good
		// the problem is to guarantee that there will be not multiple tries for
		// creating the stable version for the same object
	}
	return obj, nil
}



// Given a key and an object type, returns the latest version of the object stored
// in the database
func (db *CRDTMvDB) Get( t byte, key []byte) (*MvDBCRDT,error) {
	keybase := createKeyBase( key)
	if db.prefs.StoreLast {
		obj, err := db.getLatest( t, keybase)
		return obj, err
	}
	return db.buildVersion( t, keybase, nil)
}

// Given a key, an object type and a version vector, returns the version of the object stored
// in the database with the given version
func (db *CRDTMvDB) GetVersion( t byte, key []byte, vv *utils.VersionVector) (*MvDBCRDT,error) {
	keybase := createKeyBase( key)
	if db.prefs.StoreLast {
		obj, err := db.getLatest( t, keybase)
		if err != nil {
			return obj, nil
		}
		if obj.Vv.SmallerOrEqual( vv) {
			return obj, err
		}
	}
	return db.buildVersion( t, keybase, vv)
}


// Given a key and a opcrdts.CRDT object, stores the object in the database.
// NOTE: currently, the written object will overwrite previous versions.
func (db *CRDTMvDB) Put( key []byte, obj *MvDBCRDT) error {
	return db.putImpl( key, obj, db.log)
}

// Given a key and a opcrdts.CRDT object, stores the object in the database.
// NOTE: currently, the written object will overwrite previous versions.
func (db *CRDTMvDB) putImpl( key []byte, obj *MvDBCRDT, log common.LogBatchInterface) error {
	b,ok := obj.Serialize()
	if ok == false {
		return errors.New("serialize error")
	} else {
		key = createKey( obj.Obj.GetType(), key)
		return log.Put(key, b)
	}
}

// Write the given operation for the object key.
func (db *CRDTMvDB) PutOp( key []byte, op *MvDBCRDTOperation) error {
	if ! ( db.prefs.StoreLast && db.prefs.BuildAny) {
		return db.putOpImpl( key, op, db.log)
	} else {
		batch := db.log.WriteBatch()
		defer batch.Destroy()
		err := db.putOpImpl( key, op, batch)
		if err != nil {
			return err
		}
		return db.log.RunBatch( batch)

	}
}

// Write the given operation for the object key.
func (db *CRDTMvDB) putOpImpl( key []byte, op *MvDBCRDTOperation, log common.LogBatchInterface) error {
	key = createKeyBase( key)
	b, ok := op.Serialize()
	if ok == false {
		return errors.New("serialize error")
	}
	if db.prefs.StoreLast {
		dbkey := createKeyWithBase(op.GetCRDTType(), key)
		err := log.Merge(dbkey, b)
		if err != nil {
			return err
		}
	}
	if db.prefs.BuildAny {
		dbkey := createOpKeyWithBase(op.GetCRDTType(), op.Ts, key)
		err := log.Put(dbkey, b)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *CRDTMvDB) WriteBatch() *WriteBatch {
	batch := db.log.WriteBatch()
	return NewWriteBatch( batch, db)
}


//TODO: add delete operations
