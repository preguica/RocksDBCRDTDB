// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
package opcrdts

import (
	"encoding/binary"
	"fmt"
	"rockscrdtdb/utils"
)

// Operation-based Counter CRDT
type Counter struct {
	Val int64
}

// Serializes the contents of the counter
func (m *Counter) Serialize() ([]byte, bool) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64((*m).Val))
	return buf,true
}

// Returns the type of the CRDT
func (m *Counter) GetType() byte {
	return CRDT_COUNTER
}

func (m *Counter) ToString() string {
	return fmt.Sprint( m.Val)
}

// Unserializes the contents of the counter
func UnserializeCounter(b []byte) (CRDT, bool) {
	if len(b) != 8 {
		return nil,false
	}
	return &Counter{int64(binary.BigEndian.Uint64(b))},true
}

func (obj *Counter) Apply( op CRDTOperation) bool {
	return op.Apply( obj)
}

//================================================================================================================
// Application interface
// Returns the current value of the counter
func (m *Counter) Value() int64 {
	return m.Val
}

// Returns an operation to add Delta to the counter
func (m *Counter) Add( ts *utils.Timestamp, vv *utils.VersionVector, delta int64) CRDTOperation {
	return NewCounterOpAdd(ts, vv, delta)
}

// Create new counter with initial value 0
func NewCounter() *Counter {
	return &Counter{0}
}

