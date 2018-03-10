package mvdb

import (
	"encoding/json"
	"rockscrdtdb/utils"
	"rockscrdtdb/opcrdts"
)

type RawMvDBCRDT struct {
	t byte
	Data []byte
	Vv *utils.VersionVector
}

func (obj *RawMvDBCRDT)serialize() ([]byte, bool) {
	b, err := json.Marshal(*obj)
	if err != nil {
		return nil, false
	} else {
		return b, true
	}
}

type MvDBCRDT struct {
	Obj opcrdts.CRDT
	Vv *utils.VersionVector
}

func NewMvDBCRDT( obj opcrdts.CRDT, vv *utils.VersionVector) *MvDBCRDT {
	return &MvDBCRDT{ obj, vv}
}

func (obj *MvDBCRDT)Serialize() ([]byte, bool) {
	b,ok := obj.Obj.Serialize()
	if ok == false {
		return nil, false
	}
	rawObj := RawMvDBCRDT{ obj.Obj.GetType(), b, obj.Vv}
	b, err := json.Marshal(rawObj)
	if err != nil {
		return nil, false
	} else {
		return b, true
	}
}


func UnserializeMvDBCRDT(b []byte) (*MvDBCRDT, bool) {
	rawObj := RawMvDBCRDT{}
	err := json.Unmarshal( b, &rawObj)
	if err != nil {
		return nil, false
	}
	obj,ok := opcrdts.FunCRDTUnserializer[rawObj.t]( rawObj.Data)
	if ok == false {
		return nil, false
	}
	return &MvDBCRDT{ obj, rawObj.Vv}, true
}
