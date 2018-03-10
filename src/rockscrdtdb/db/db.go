package db

import (
	"errors"
	"rockscrdtdb/opcrdts"
	"rockscrdtdb/common"
)

//Holds a reference to an open database
type CRDTDB struct {
	log *common.Log
}

// Opens a database stored in the given filename
func OpenCRDTDB( dbName string) (*CRDTDB, error) {
	log, err := common.OpenLog( dbName, true, NewDbMerger())
	return &CRDTDB{log}, err
}

// Closes an opened database
func (db *CRDTDB) Close() {
	defer db.log.Close()
}

// Given a key and an object type, returns the last version of the object stored
// in the database
func (db *CRDTDB) Get( t byte, key []byte) (*opcrdts.CRDT,error) {
	key = createKey( t, key)
	val,err := db.log.Get(key)
	if val.Data() == nil {
		return nil, common.NewNoObjectError( string(key))
	}
	if err != nil {
		return nil,err
	} else {
		obj,ok := opcrdts.FunCRDTUnserializer[t]( val.Data())
		if ok == false {
			return nil, errors.New("unserialize error")
		} else {
			return &obj, nil
		}
	}
}

// Given a key and a opcrdts.CRDT object, stores the object in the database.
// NOTE: currently, the written object will overwrite previous versions.
func (db *CRDTDB) Put( key []byte, obj opcrdts.CRDT) error {
	b,ok := obj.Serialize()
	if ok == false {
		return errors.New("serialize error")
	} else {
		key = createKey( obj.GetType(), key)
		return db.log.Put(key, b)
	}
}

// Write the given operation for the object key.
func (db *CRDTDB) PutOp( key []byte, op opcrdts.CRDTOperation) error {
	b,ok := op.Serialize()
	if ok == false  {
		return errors.New("serialize error")
	} else {
		key = createKey( op.GetCRDTType(), key)
		return db.log.Merge(key, b)
	}
}

