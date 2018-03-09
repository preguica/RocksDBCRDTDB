package mvdb

import (
	"errors"
	"rockscrdtdb/common"
)

//Holds a reference to an open database
type CRDTMvDB struct {
	log *common.Log
}

// Opens a database stored in the given filename
func OpenCRDTMvDB( dbName string) (*CRDTMvDB, error) {
	log, err := common.OpenLog( dbName, NewMvDbMerger())
	return &CRDTMvDB{log}, err
}

// Closes an opened database
func (db *CRDTMvDB) Close() {
	defer db.log.Close()
}

// Given a key and an object type, returns the last version of the object stored
// in the database
func (db *CRDTMvDB) Get( t byte, key []byte) (*MvDBCRDT,error) {
	key = createKey( t, key)
	val,err := db.log.Get(key)
	if val.Data() == nil || err != nil {
		return nil, nil
	}
	obj,ok := UnserializeMvDBCRDT( val.Data())
	if ok == false {
		return nil, errors.New("unserialize error")
	} else {
		return obj, nil
	}
}

// Given a key and a opcrdts.CRDT object, stores the object in the database.
// NOTE: currently, the written object will overwrite previous versions.
func (db *CRDTMvDB) Put( key []byte, obj *MvDBCRDT) error {
	b,ok := obj.Serialize()
	if ok == false {
		return errors.New("serialize error")
	} else {
		key = createKey( obj.Obj.GetType(), key)
		return db.log.Put(key, b)
	}
}

// Write the given operation for the object key.
func (db *CRDTMvDB) PutOp( key []byte, op *MvDBCRDTOperation) error {
	b,ok := op.Serialize()
	if ok == false  {
		return errors.New("serialize error")
	}
	key = createKey( op.GetCRDTType(), key)
	return db.log.Merge(key, b)
}

