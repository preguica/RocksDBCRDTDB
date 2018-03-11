// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
package opcrdts

import (
	"fmt"
	"rockscrdtdb/utils"
	"encoding/json"
)

// Operation-based Integer CRDT
// for set operations, the last writer-wins
// on concurrent set and inc, inc wins
// Assumes timestamps are generated using hybrid clocks, in which a new timestamp value is always larger
// than an observer timestamp
type Integer struct {
	Val int64
	Base int64
	Ts *utils.Timestamp
}

// Serializes the contents of the integer
func (m *Integer) Serialize() ([]byte, bool) {
	b, err := json.Marshal(*m)
	if err != nil {
		return nil, false
	} else {
		return b, true
	}
}

// Returns the type of the CRDT
func (m *Integer) GetType() byte {
	return CRDT_INTEGER
}

func (m *Integer) ToString() string {
	return fmt.Sprint( m.Val)
}

// Unserializes the contents of the integer
func UnserializeInteger(b []byte) (CRDT, bool) {
	obj := Integer{}
	err := json.Unmarshal( b, &obj)
	if err != nil {
		return nil, false
	} else {
		return &obj, true
	}
}

func (obj *Integer) Apply( op CRDTOperation) bool {
	return op.Apply( obj)
}

//================================================================================================================
// Application interface
// Returns the current value of the integer
func (m *Integer) Value() int64 {
	return m.Val + m.Base
}

// Returns an operation to add Delta to the integer
func (m *Integer) Add( ts *utils.Timestamp, vv *utils.VersionVector, delta int64) CRDTOperation {
	return NewIntegerOpAdd(ts, vv, delta)
}

// Returns an operation to set the value to val
func (m *Integer) Set( ts *utils.Timestamp, vv *utils.VersionVector, val int64) CRDTOperation {
	return &IntegerOpSet{val - m.Value(), ts}
}

// Create new integer with initial value 0
func NewInteger() *Integer {
	return &Integer{0, 0,utils.NewTimestamp(utils.DCId(""),0)}
}

