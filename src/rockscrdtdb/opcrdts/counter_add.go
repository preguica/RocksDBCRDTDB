// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
package opcrdts

import "encoding/binary"

// Counter operation
type CounterOpAdd struct {
	delta int64
}

func NewCounterOpAdd( delta int64) *CounterOpAdd {
	return &CounterOpAdd{ delta}
}

func (m *CounterOpAdd) GetCRDTType() byte {
	return CRDT_OPCOUNTER
}

func (m *CounterOpAdd) GetType() byte {
	return CRDT_OPCOUNTER_INC
}

func (m *CounterOpAdd) Serialize()  ([]byte, bool) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(m.delta))
	return buf,true
}

func UnserializeCounterOpAdd(b []byte) (CRDTOperation, bool) {
	return &CounterOpAdd{int64(binary.BigEndian.Uint64(b))},true
}

func (leftOp *CounterOpAdd) Merge( otherOp CRDTOperation) bool {
	rightOp, ok := otherOp.(*CounterOpAdd)
	if ok == false {
		return false
	}
	leftOp.delta += rightOp.delta;
	return true
}
func (cntOp *CounterOpAdd) Apply(obj CRDT) bool {
	cnt, ok := (obj).(*Counter)
	if ok == false {
		return false
	}
	cnt.val = cnt.val + cntOp.delta
	return true
}

