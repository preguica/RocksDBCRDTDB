// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
package db

import (
	"rockscrdtdb/common"
	"rockscrdtdb/opcrdts"
)

type WriteBatch struct {
	batch *common.LogBatchImpl
	db *CRDTDB
}

func NewWriteBatch( batch *common.LogBatchImpl, db *CRDTDB) *WriteBatch {
	return &WriteBatch{ batch, db}
}

// Given a key and a opcrdts.CRDT object, stores the object in the database.
// NOTE: currently, the written object will overwrite previous versions.
func (b *WriteBatch) Put( key []byte, obj opcrdts.CRDT) error {
	return b.db.putImpl( key, obj, b.batch)
}

// Write the given operation for the object key.
func (b *WriteBatch) PutOp( key []byte, op opcrdts.CRDTOperation) error {
	return b.db.putOpImpl( key, op, b.batch)
}

func (b *WriteBatch)Destroy() {
	b.batch.Destroy()
}
