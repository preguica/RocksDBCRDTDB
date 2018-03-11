// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
package opcrdts

import (
	"encoding/json"
	"fmt"
	"rockscrdtdb/utils"
)

//proteus:generate
type SetAddWins struct {
	Vals map[string]*utils.VersionVector
}

func NewSetAddWins() *SetAddWins {
	obj := SetAddWins{}
	obj.Vals = make(map[string]*utils.VersionVector)
	return &obj
}

func OpCRDTSetAddWinsAdd( ts *utils.Timestamp, vv *utils.VersionVector, val []byte) CRDTOperation {
	op := NewOpCRDTSetAddWinsOp()
	op.AddElem( ts, vv, string(val))
	return op
}


func OpCRDTSetAddWinsRmv( ts *utils.Timestamp, vv *utils.VersionVector, val []byte) CRDTOperation {
	op := NewOpCRDTSetAddWinsOp()
	op.RmvElem( ts, vv, string(val))
	return op
}

func (obj *SetAddWins) Add( ts *utils.Timestamp, vv *utils.VersionVector, val []byte) CRDTOperation {
	return OpCRDTSetAddWinsAdd( ts, vv, val)
}

func (obj *SetAddWins) Rmv( ts *utils.Timestamp, vv *utils.VersionVector, val []byte) CRDTOperation {
	return OpCRDTSetAddWinsRmv( ts, vv, val)
}

func (obj *SetAddWins) Contains( val []byte) bool {
	_, ok := obj.Vals[string(val)]
	return ok
}

func (obj *SetAddWins) GetType() byte {
	return CRDT_SET_ADDWINS
}

func (obj *SetAddWins) ToString() string {
	return fmt.Sprint( obj)
}

func (obj *SetAddWins) Apply( op CRDTOperation) bool {
	return op.Apply( obj)
}

func (obj *SetAddWins) Serialize()  ([]byte, bool) {
	b, err := json.Marshal(*obj)
	if err != nil {
		return nil, false
	} else {
		return b, true
	}
}

func UnserializeSetAddWins(b []byte) (CRDT, bool) {
	obj := SetAddWins{}
	err := json.Unmarshal( b, &obj)
	if err != nil {
		return nil, false
	} else {
		return &obj, true
	}
}


