package mvdb

import (
	"rockscrdtdb/common"
	"rockscrdtdb/utils"
)

type MvDbMerger struct{

}

func (m *MvDbMerger) Name() string {
	return "nova.dbmerger"
}
func (m *MvDbMerger)FullMerge(key []byte, existingValue []byte, operands [][]byte) ([]byte, bool) {
	return MvDbFullMerge(key,existingValue,operands)
}

func (m *MvDbMerger)PartialMerge(key []byte, leftOperand []byte, rightOperand []byte) ([]byte, bool) {
	return MvDbPartialMerge(key, leftOperand, rightOperand)
}

func NewMvDbMerger() *MvDbMerger {
	return &MvDbMerger{}
}

func MvDbFullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	t := key[len(key)-1]
	obj, ok := UnserializeMvDBCRDT( existingValue)
	if ! ok {
		o := common.FunCRDTNew[t]()
		obj = NewMvDBCRDT( o, utils.NewVersionVector())
	}
	for _, opB := range operands {
		op, ok := UnserializeMvDBCRDTOperation( opB)
		if ! ok {
			return nil,false
		}
		obj.Obj.Apply(op.Op)
		obj.Vv.PointwiseMax( op.Vv)
	}
	objFinal, okFinal := obj.Serialize()
	if ! okFinal {
		return nil, false
	} else {
		return objFinal, true
	}
}

func MvDbPartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	leftOp, ok := UnserializeMvDBCRDTOperation( leftOperand)
	if ok == false {
		return nil,false
	}
	rightOp, ok := UnserializeMvDBCRDTOperation( rightOperand)
	if ok == false {
		return nil,false
	}
	ok = leftOp.Op.Merge(rightOp.Op)
	if ok == false {
		return nil, false
	} else {
		leftOp.Vv.PointwiseMax( rightOp.Vv)
		return leftOp.Serialize()
	}
}


