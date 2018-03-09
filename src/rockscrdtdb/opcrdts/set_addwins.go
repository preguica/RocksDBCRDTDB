package opcrdts

import (
	"encoding/json"
	"fmt"
	"rockscrdtdb/utils"
)

//proteus:generate
type SetAddWins struct {
	Vals map[string]*utils.VersionVector
}

func NewSetAddWins() *SetAddWins {
	obj := SetAddWins{}
	obj.Vals = make(map[string]*utils.VersionVector)
	return &obj
}

func OpCRDTSetAddWinsAdd( ts *utils.Timestamp, vv *utils.VersionVector, val []byte) CRDTOperation {
	op := NewOpCRDTSetAddWinsOp()
	op.AddElem( ts, vv, string(val))
	return op
}


func OpCRDTSetAddWinsRmv( ts *utils.Timestamp, vv *utils.VersionVector, val []byte) CRDTOperation {
	op := NewOpCRDTSetAddWinsOp()
	op.RmvElem( ts, vv, string(val))
	return op
}

func (obj *SetAddWins) Add( ts *utils.Timestamp, vv *utils.VersionVector, val []byte) CRDTOperation {
	return OpCRDTSetAddWinsAdd( ts, vv, val)
}

func (obj *SetAddWins) Rmv( ts *utils.Timestamp, vv *utils.VersionVector, val []byte) CRDTOperation {
	return OpCRDTSetAddWinsRmv( ts, vv, val)
}

func (obj *SetAddWins) Contains( val []byte) bool {
	_, ok := obj.Vals[string(val)]
	return ok
}

func (obj *SetAddWins) Apply(op CRDTOperation) bool {
	setOp, ok := (op).(*SetAddWinsOpAddRmv)
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

func (obj *SetAddWins) GetType() byte {
	return CRDT_OPSET_ADDWINS
}

func (obj *SetAddWins) ToString() string {
	return fmt.Sprint( obj)
}

func (obj *SetAddWins) Serialize()  ([]byte, bool) {
	b, err := json.Marshal(*obj)
	if err != nil {
		return nil, false
	} else {
		return b, true
	}
}

func UnserializeSetAddWins(b []byte) (CRDT, bool) {
	obj := SetAddWins{}
	err := json.Unmarshal( b, &obj)
	if err != nil {
		return nil, false
	} else {
		return &obj, true
	}
}


/*
func OpCRDT_Set_AddWins_FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	var val *SetAddWins
	obj, ok := UnserializeSetAddWins( existingValue)
	if ok {
		val, ok = obj.(*SetAddWins)
		if ok == false {
			return nil, false
		}
	} else {
		val = NewSetAddWins()
	}

	for _, opB := range operands {
		opObj, ok := UnserializeSetAddWinsOpAddRmv( opB)
		if ! ok {
			return nil,false
		}
		op, ok := opObj.(*SetAddWinsOpAddRmv)
		if ok == false {
			return nil, false
		}
		val.Apply(op)
	}
	objFinal, okFinal := val.Serialize()
	if ! okFinal {
		return nil, false
	} else {
		return objFinal, true
	}
}

func OpCRDT_Set_AddWins_PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	leftObj, ok := UnserializeSetAddWinsOpAddRmv( leftOperand)
	if ok == false {
		return nil,false
	}
	leftOp, ok := leftObj.(*SetAddWinsOpAddRmv)
	if ok == false {
		return nil, false
	}
	rightObj, ok := UnserializeSetAddWinsOpAddRmv( rightOperand)
	if ok == false {
		return nil,false
	}
	rightOp, ok := rightObj.(*SetAddWinsOpAddRmv)
	if ok == false {
		return nil, false
	}
	ok = leftOp.merge(rightOp)
	if ok == false {
		return nil, false
	} else {
		return leftOp.Serialize()
	}
}
*/






