// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
package opcrdts

import (
	"encoding/binary"
	"rockscrdtdb/utils"
)

// Counter operation
type CounterOpAdd struct {
	Delta int64
}

func (m *CounterOpAdd) GetCRDTType() byte {
	return CRDT_COUNTER
}

func (m *CounterOpAdd) GetType() byte {
	return CRDT_COUNTER__INC
}

func (m *CounterOpAdd) Serialize()  ([]byte, bool) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(m.Delta))
	return buf,true
}

func UnserializeCounterOpAdd(b []byte) (CRDTOperation, bool) {
	return &CounterOpAdd{int64(binary.BigEndian.Uint64(b))},true
}

func (leftOp *CounterOpAdd) Merge( otherOp CRDTOperation) (CRDTOperation, bool) {
	rightOp, ok := otherOp.(*CounterOpAdd)
	if ok == false {
		return leftOp, false
	}
	leftOp.Delta += rightOp.Delta;
	return leftOp, true
}
func (cntOp *CounterOpAdd) Apply(obj CRDT) bool {
	cnt, ok := (obj).(*Counter)
	if ok == false {
		return false
	}
	cnt.Val = cnt.Val + cntOp.Delta
	return true
}

func NewCounterOpAdd( ts *utils.Timestamp, vv *utils.VersionVector, delta int64) *CounterOpAdd {
	return &CounterOpAdd{ delta}
}

