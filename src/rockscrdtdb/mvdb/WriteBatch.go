package mvdb

import (
	"rockscrdtdb/common"
)

type WriteBatch struct {
	batch *common.LogBatchImpl
	db *CRDTMvDB
}

func NewWriteBatch( batch *common.LogBatchImpl, db *CRDTMvDB) *WriteBatch {
	return &WriteBatch{ batch, db}
}

// Given a key and a opcrdts.CRDT object, stores the object in the database.
// NOTE: currently, the written object will overwrite previous versions.
func (b *WriteBatch) Put( key []byte, obj *MvDBCRDT) error {
	return b.db.putImpl( key, obj, b.batch)
}

// Write the given operation for the object key.
func (b *WriteBatch) PutOp( key []byte, op *MvDBCRDTOperation) error {
	return b.db.putOpImpl( key, op, b.batch)
}

func (b *WriteBatch)Destroy() {
	b.batch.Destroy()
}
