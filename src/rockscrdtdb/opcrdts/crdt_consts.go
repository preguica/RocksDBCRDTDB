package opcrdts

const (
	CRDT_OPCOUNTER byte = 0
	CRDT_OPSET_ADDWINS byte = 1
	CRDT_RESERVED_LAST byte = 253
	CRDT_RESERVED_STABLE byte = 254
	CRDT_RESERVED_OPS byte = 255
)

const (
	//Ops for CRDT_OPCOUNTER
	CRDT_OPCOUNTER_INC byte = 0
	//Ops for CRDT_OPSET_ADDWINS byte = 1
	CRDT_OPSET_ADDWINS_ADDRMV byte = 0
)

var FunCRDTNew = map[byte]func () CRDT {
	CRDT_OPCOUNTER: func() CRDT {
		return NewCounter()
	},
	CRDT_OPSET_ADDWINS: func() CRDT {
		return NewSetAddWins()
	},
}

var FunCRDTUnserializer = map[byte]func ([]byte) (CRDT, bool){
	CRDT_OPCOUNTER: UnserializeCounter,
	CRDT_OPSET_ADDWINS: UnserializeSetAddWins,
}

var FunCRDTOpUnserializer = map[byte]func ([]byte) (CRDTOperation, bool){
	CRDT_OPCOUNTER: UnserializeCounterOpAdd,
	CRDT_OPSET_ADDWINS: UnserializeSetAddWinsOpAddRmv,
}

