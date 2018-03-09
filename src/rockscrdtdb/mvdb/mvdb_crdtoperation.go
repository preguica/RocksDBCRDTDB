package mvdb

import (
	"encoding/json"
	"rockscrdtdb/utils"
	"rockscrdtdb/opcrdts"
	"rockscrdtdb/common"
)

type RawMvDBCRDTOperation struct {
	TypeCRDT byte
	TypeCRDTOp byte
	Data []byte
	Vv *utils.VersionVector
}

func (op *RawMvDBCRDTOperation)serialize() ([]byte, bool) {
	b, err := json.Marshal(*op)
	if err != nil {
		return nil, false
	} else {
		return b, true
	}
}

type MvDBCRDTOperation struct {
	Op opcrdts.CRDTOperation
	Vv *utils.VersionVector
}

func NewMvDBCRDTOperation( Op opcrdts.CRDTOperation, Ts *utils.Timestamp) *MvDBCRDTOperation{
	vv := utils.NewVersionVector()
	vv.AddTS(Ts)
	return &MvDBCRDTOperation{ Op, vv}
}

func (op *MvDBCRDTOperation)GetCRDTType() byte {
	return op.Op.GetCRDTType()
}

func (op *MvDBCRDTOperation)Serialize() ([]byte, bool) {
	b,ok := op.Op.Serialize()
	if ok == false {
		return nil, false
	}
	rawOp := RawMvDBCRDTOperation{ op.Op.GetCRDTType(), op.Op.GetType(), b, op.Vv}
	b, err := json.Marshal(rawOp)
	if err != nil {
		return nil, false
	} else {
		return b, true
	}
}

func UnserializeMvDBCRDTOperation(b []byte) (*MvDBCRDTOperation, bool) {
	rawOp := RawMvDBCRDTOperation{}
	err := json.Unmarshal( b, &rawOp)
	if err != nil {
		return nil, false
	}
	op,ok := common.FunCRDTOpUnserializer[rawOp.TypeCRDT]( rawOp.Data)
	if ok == false {
		return nil, false
	}
	return &MvDBCRDTOperation{ op, rawOp.Vv}, true
}

