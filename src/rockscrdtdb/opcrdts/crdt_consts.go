package opcrdts

const (
	CRDT_OPCOUNTER byte = 0
	CRDT_OPSET_ADDWINS byte = 1
)

const (
	//Ops for CRDT_OPCOUNTER
	CRDT_OPCOUNTER_INC byte = 0
	//Ops for CRDT_OPSET_ADDWINS byte = 1
	CRDT_OPSET_ADDWINS_ADDRMV byte = 0
)
