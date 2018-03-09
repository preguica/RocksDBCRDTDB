package db

import (
	"rockscrdtdb/common"
)

type DbMerger struct{

}

func (m *DbMerger) Name() string {
	return "nova.dbmerger"
}
func (m *DbMerger)FullMerge(key []byte, existingValue []byte, operands [][]byte) ([]byte, bool) {
	return CRDTFullMerge(key,existingValue,operands)
}

func (m *DbMerger)PartialMerge(key []byte, leftOperand []byte, rightOperand []byte) ([]byte, bool) {
	return CRDTPartialMerge(key, leftOperand, rightOperand)
}

func NewDbMerger() *DbMerger {
	return &DbMerger{}
}

func CRDTFullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	t := key[len(key)-1]
	obj, ok := common.FunCRDTUnserializer[t]( existingValue)
	if ! ok {
		obj = common.FunCRDTNew[t]()
	}
	for _, opB := range operands {
		op, ok := common.FunCRDTOpUnserializer[t]( opB)
		if ! ok {
			return nil,false
		}
		obj.Apply(op)
	}
	objFinal, okFinal := obj.Serialize()
	if ! okFinal {
		return nil, false
	} else {
		return objFinal, true
	}
}

func CRDTPartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	t := key[len(key)-1]
	leftOp, ok := common.FunCRDTOpUnserializer[t]( leftOperand)
	if ok == false {
		return nil,false
	}
	rightOp, ok := common.FunCRDTOpUnserializer[t]( rightOperand)
	if ok == false {
		return nil,false
	}
	ok = leftOp.Merge(rightOp)
	if ok == false {
		return nil, false
	} else {
		return leftOp.Serialize()
	}
}

