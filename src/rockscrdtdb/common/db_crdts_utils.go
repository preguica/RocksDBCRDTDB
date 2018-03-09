package common

import "rockscrdtdb/opcrdts"


var FunCRDTNew = map[byte]func () opcrdts.CRDT {
	opcrdts.CRDT_OPCOUNTER: func() opcrdts.CRDT {
		return opcrdts.NewCounter()
	},
	opcrdts.CRDT_OPSET_ADDWINS: func() opcrdts.CRDT {
		return opcrdts.NewSetAddWins()
	},
}

var FunCRDTUnserializer = map[byte]func ([]byte) (opcrdts.CRDT, bool){
	opcrdts.CRDT_OPCOUNTER: opcrdts.OpCRDT_Counter_unserialize,
	opcrdts.CRDT_OPSET_ADDWINS: opcrdts.UnserializeSetAddWins,
}

var FunCRDTOpUnserializer = map[byte]func ([]byte) (opcrdts.CRDTOperation, bool){
	opcrdts.CRDT_OPCOUNTER: opcrdts.UnserializeCounterOpAdd,
	opcrdts.CRDT_OPSET_ADDWINS: opcrdts.UnserializeSetAddWinsOpAddRmv,
}

/*var CRDTOpFullMerge = map[byte]func (key, existingValue []byte, operands [][]byte) ([]byte, bool){
	CRDT_OPCOUNTER: OpCRDT_Counter_FullMerge,
	CRDT_OPSET_ADDWINS: OpCRDT_Set_AddWins_FullMerge,
}

var CRDTOpPartialMerge = map[byte]func (key, leftOperand, rightOperand []byte) ([]byte, bool){
	CRDT_OPCOUNTER: OpCRDT_Counter_PartialMerge,
	CRDT_OPSET_ADDWINS: OpCRDT_Set_AddWins_PartialMerge,
}
*/

type NullMergeOperator struct {
}

func (m *NullMergeOperator) Name() string {
	return "nova.nullmerger"
}
func (m *NullMergeOperator) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	return nil, false
}
func (m *NullMergeOperator) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	return nil, false
}

func NewNullMergeOperator() *NullMergeOperator {
	return &NullMergeOperator{}
}