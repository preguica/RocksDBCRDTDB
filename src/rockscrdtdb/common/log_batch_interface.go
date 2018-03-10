// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
package common

type LogBatchInterface interface {
	Put( key []byte, value []byte) error
	Merge( key []byte, value []byte) error
	Delete( key []byte) error
}

