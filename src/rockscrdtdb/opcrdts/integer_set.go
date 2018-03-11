// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
package opcrdts

import (
	"rockscrdtdb/utils"
	"encoding/json"
)

// Integer operation
type IntegerOpSet struct {
	Val int64
	Ts *utils.Timestamp
}

func (m *IntegerOpSet) GetCRDTType() byte {
	return CRDT_INTEGER
}

func (m *IntegerOpSet) GetType() byte {
	return CRDT_INTEGER__SET
}

func (m *IntegerOpSet) Serialize()  ([]byte, bool) {
	b, err := json.Marshal(*m)
	if err != nil {
		return nil, false
	} else {
		return b, true
	}
}

func UnserializeIntegerOpSet(b []byte) (CRDTOperation, bool) {
	obj := IntegerOpSet{}
	err := json.Unmarshal( b, &obj)
	if err != nil {
		return nil, false
	} else {
		return &obj, true
	}
}

func (leftOp *IntegerOpSet) Merge( otherOp CRDTOperation) (CRDTOperation, bool) {
	rightOp, ok := otherOp.(*IntegerOpSet)
	if ok == true {
		if leftOp.Ts.SmallerThan(rightOp.Ts) {
			leftOp.Ts = rightOp.Ts
			leftOp.Val = rightOp.Val
		}
		return leftOp, true
	}
	return nil, false
}
func (cntOp *IntegerOpSet) Apply(obj CRDT) bool {
	cnt, ok := (obj).(*Integer)
	if ok == false {
		return false
	}
	if cnt.Ts.SmallerThan( cntOp.Ts) {
		cnt.Base = cntOp.Val
		cnt.Ts = cntOp.Ts
	}
	return true
}

