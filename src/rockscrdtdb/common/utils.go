package common


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