// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
package opcrdts

const (
	CRDT_COUNTER         byte = 0
	CRDT_SET_ADDWINS     byte = 1
	CRDT_INTEGER         byte = 2
	CRDT_RESERVED_LAST   byte = 253
	CRDT_RESERVED_STABLE byte = 254
	CRDT_RESERVED_OPS    byte = 255
)

const (
	//Ops for CRDT_COUNTER
	CRDT_COUNTER__INC byte = 0
	//Ops for CRDT_SET_ADDWINS
	CRDT_OPSET_ADDWINS__ADDRMV byte = 0
	//Ops for CRDT_INTEGER
	CRDT_INTEGER__INC byte = 0
	CRDT_INTEGER__SET byte = 1
)

var FunCRDTNew = map[byte]func () CRDT {
	CRDT_COUNTER: func() CRDT {
		return NewCounter()
	},
	CRDT_SET_ADDWINS: func() CRDT {
		return NewSetAddWins()
	},
	CRDT_INTEGER: func() CRDT {
		return NewInteger()
	},
}

var FunCRDTUnserializer = map[byte]func ([]byte) (CRDT, bool){
	CRDT_COUNTER:     UnserializeCounter,
	CRDT_SET_ADDWINS: UnserializeSetAddWins,
	CRDT_INTEGER:     UnserializeInteger,
}
var FunCRDTOpUnserializer = map[byte]map[byte]func ([]byte) (CRDTOperation, bool){
	CRDT_COUNTER: {
		CRDT_COUNTER__INC: UnserializeCounterOpAdd,
	},
	CRDT_SET_ADDWINS: {
		CRDT_SET_ADDWINS: UnserializeSetAddWinsOpAddRmv,
	},
	CRDT_INTEGER: {
		CRDT_INTEGER__INC: UnserializeIntegerOpAdd,
		CRDT_INTEGER__SET: UnserializeIntegerOpSet,
	},
}
