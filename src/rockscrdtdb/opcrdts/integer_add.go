// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
package opcrdts

import (
	"encoding/binary"
	"rockscrdtdb/utils"
)

// Integer operation
type IntegerOpAdd struct {
	Delta int64
}

func (m *IntegerOpAdd) GetCRDTType() byte {
	return CRDT_INTEGER
}

func (m *IntegerOpAdd) GetType() byte {
	return CRDT_INTEGER__INC
}

func (m *IntegerOpAdd) Serialize()  ([]byte, bool) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(m.Delta))
	return buf,true
}

func UnserializeIntegerOpAdd(b []byte) (CRDTOperation, bool) {
	return &IntegerOpAdd{int64(binary.BigEndian.Uint64(b))},true
}

func (leftOp *IntegerOpAdd) Merge( otherOp CRDTOperation) (CRDTOperation, bool) {
	rightOp, ok := otherOp.(*IntegerOpAdd)
	if ok == false {
		return leftOp, false
	}
	leftOp.Delta += rightOp.Delta;
	return leftOp, true
}
func (cntOp *IntegerOpAdd) Apply(obj CRDT) bool {
	cnt, ok := (obj).(*Integer)
	if ok == false {
		return false
	}
	cnt.Val = cnt.Val + cntOp.Delta
	return true
}

func NewIntegerOpAdd( ts *utils.Timestamp, vv *utils.VersionVector, delta int64) *IntegerOpAdd {
	return &IntegerOpAdd{ delta}
}

