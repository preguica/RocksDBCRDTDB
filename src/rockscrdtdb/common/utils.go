// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
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