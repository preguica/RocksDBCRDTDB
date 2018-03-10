// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
package opcrdts

import (
	"rockscrdtdb/utils"
	"encoding/json"
)

// AddWins operation
//proteus:generate
type SetAddWinsOpAddRmv struct {
	Adds map[string]*utils.VersionVector
	Rmvs map[string]*utils.VersionVector
}

func NewOpCRDTSetAddWinsOp() *SetAddWinsOpAddRmv {
	return &SetAddWinsOpAddRmv{}
}

func (op *SetAddWinsOpAddRmv) AddElem( ts *utils.Timestamp, vv *utils.VersionVector, val string) {
	if op.Adds == nil {
		op.Adds = make(map[string]*utils.VersionVector)
	}
	opVv, ok := op.Adds[val]
	if ok == false {
		opVv = utils.NewVersionVector()
		op.Adds[val] = opVv;
	}
	opVv.AddTS( ts)
}

func (op *SetAddWinsOpAddRmv) RmvElem( ts *utils.Timestamp, vv *utils.VersionVector, val string) {
	if op.Rmvs == nil {
		op.Rmvs = make(map[string]*utils.VersionVector)
	}
	opVv, ok := op.Rmvs[val]
	if ok == false {
		opVv = utils.NewVersionVector()
		op.Rmvs[val] = vv
	} else {
		opVv.PointwiseMax(vv)
	}
}


func (op *SetAddWinsOpAddRmv) GetCRDTType() byte {
	return CRDT_OPSET_ADDWINS
}

func (op *SetAddWinsOpAddRmv) GetType() byte {
	return CRDT_OPSET_ADDWINS_ADDRMV
}

func (op *SetAddWinsOpAddRmv) Serialize()  ([]byte, bool) {
	b, err := json.Marshal(*op)
	if err != nil {
		return nil, false
	} else {
		return b, true
	}
}

func UnserializeSetAddWinsOpAddRmv(b []byte) (CRDTOperation, bool) {
	obj := SetAddWinsOpAddRmv{}
	err := json.Unmarshal( b, &obj)
	if err != nil {
		return nil, false
	} else {
		return &obj, true
	}
}

func (setOp *SetAddWinsOpAddRmv) Apply(obj0 CRDT) bool {
	obj, ok := (obj0).(*SetAddWins)
	if ok == false {
		return false
	}
	if setOp.Adds != nil {
		for k, v := range setOp.Adds {
			vv, ok := obj.Vals[k]
			if ok == false {
				vv = utils.NewVersionVector()
				obj.Vals[k] = vv
			}
			vv.PointwiseMax( v)
		}
	}
	if setOp.Rmvs != nil {
		for k, v := range setOp.Rmvs {
			vv, ok := obj.Vals[k]
			if ok == true {
				vv.RemoveIfLargerOrEqual( v)
				if vv.IsEmpty() {
					delete(obj.Vals, k)
				}
			}
		}
	}
	return true
}



func (leftOp *SetAddWinsOpAddRmv) Merge( otherOp CRDTOperation) bool {
	rightOp, ok := otherOp.(*SetAddWinsOpAddRmv)
	if ok == false {
		return false
	}
	if leftOp.Adds != nil && rightOp.Adds != nil{
		for k, v := range rightOp.Adds {
			vv, ok := leftOp.Adds[k]
			if ok == false {
				leftOp.Adds[k] = v
			} else {
				vv.PointwiseMax(v)
			}
		}
	} else if leftOp.Adds == nil {
		leftOp.Adds = rightOp.Adds
	}
	if leftOp.Rmvs != nil && rightOp.Rmvs != nil{
		for k, v := range rightOp.Rmvs {
			vv, ok := leftOp.Rmvs[k]
			if ok == false {
				leftOp.Rmvs[k] = v
			} else {
				vv.PointwiseMax(v)
			}
		}
	} else if leftOp.Rmvs == nil {
		leftOp.Rmvs = rightOp.Rmvs
	}
	return true
}
