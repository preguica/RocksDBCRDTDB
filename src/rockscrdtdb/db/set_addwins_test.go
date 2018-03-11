// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
package db

import (
	"testing"
	"github.com/facebookgo/ensure"
	"fmt"
	"time"
	"rockscrdtdb/utils"
	"rockscrdtdb/opcrdts"
)

func TestSetAddWins(t *testing.T) {
	var (
		value1 = []byte("nuno")
		value2 = []byte("elsa")
	)

	env1 := utils.NewSimpleEnvironment(utils.DCId("dc1"))
	rep1 := opcrdts.NewSetAddWins()

	ensure.True( t, rep1.Contains( value1) == false)
	ensure.True( t, rep1.Contains( value2) == false)

	ts1 := env1.GetNewTimestamp()
	op1 := rep1.Add( ts1, env1.GetCurrentState(), value1)
	ensure.True( t, rep1.Contains( value1) == false)
	ensure.True( t, rep1.Contains( value2) == false)

	ok := rep1.Apply(op1)
	env1.UpdateStateTS( ts1)
	ensure.True( t, ok)
	ensure.True( t, rep1.Contains( value1) == true)
	ensure.True( t, rep1.Contains( value2) == false)

	env2 := utils.NewSimpleEnvironment(utils.DCId("dc2"))
	rep2 := opcrdts.NewSetAddWins()

	ensure.True( t, rep2.Contains( value1) == false)
	ensure.True( t, rep2.Contains( value2) == false)

	ts2 := env2.GetNewTimestamp()
	op2 := rep2.Add( ts2, env2.GetCurrentState(), value2)
	ensure.True( t, rep2.Contains( value1) == false)
	ensure.True( t, rep2.Contains( value2) == false)

	ok2 := rep2.Apply(op2)
	env2.UpdateStateTS( ts2)
	ensure.True( t, ok2)
	ensure.True( t, rep2.Contains( value1) == false)
	ensure.True( t, rep2.Contains( value2) == true)
	ensure.True( t, rep1.Contains( value1) == true)
	ensure.True( t, rep1.Contains( value2) == false)

	rep2.Apply(op1)
	env2.UpdateStateTS( ts1)
	ensure.True( t, rep2.Contains( value1) == true)
	ensure.True( t, rep2.Contains( value2) == true)
	ensure.True( t, rep1.Contains( value1) == true)
	ensure.True( t, rep1.Contains( value2) == false)

	rep1.Apply(op2)
	env1.UpdateStateTS( ts2)
	ensure.True( t, rep2.Contains( value1) == true)
	ensure.True( t, rep2.Contains( value2) == true)
	ensure.True( t, rep1.Contains( value1) == true)
	ensure.True( t, rep1.Contains( value2) == true)

	b,ok := rep1.Serialize()
	ensure.True( t, ok == true)
	repcopy,ok := opcrdts.UnserializeSetAddWins(b)
	ensure.True( t, ok == true)

	rep1copy,ok := repcopy.(*opcrdts.SetAddWins)
	ensure.True( t, ok == true)

	ensure.True( t, len(rep1copy.Vals) == len(rep1.Vals))
}



func HelperTestSetAddWinsRocks(t *testing.T, doPut bool) {
	var (
		value1 = []byte("nuno")
		value2 = []byte("elsa")
		keyStr = fmt.Sprintf("hello%d", time.Now().UnixNano())
		givenKey    = []byte(keyStr)
	)
	db, err := OpenCRDTDB("tmp/test.db")
	ensure.Nil(t, err)
	defer db.Close()

	env1 := utils.NewSimpleEnvironment(utils.DCId("dc1"))
	rep1 := opcrdts.NewSetAddWins()

	ts1 := env1.GetNewTimestamp()
	op1 := rep1.Add( ts1, env1.GetCurrentState(), value1)
	if doPut {
		ok := rep1.Apply(op1)
		ensure.True( t, ok)

		err = db.Put(givenKey, rep1)
		ensure.Nil(t, err)

	} else {
		err = db.PutOp(givenKey, op1)
	}
	env1.UpdateStateTS(ts1)
	rep1Read,err := db.Get(opcrdts.CRDT_SET_ADDWINS, givenKey)
	ensure.Nil(t, err)

	rep1r, ok := (*rep1Read).(*opcrdts.SetAddWins)
	ensure.True(t, ok)
	ensure.True(t, rep1r.Contains(value1) == true)
	ensure.True(t, rep1r.Contains(value2) == false)

	env2 := utils.NewSimpleEnvironment(utils.DCId("dc2"))
	rep2 := opcrdts.NewSetAddWins()

	ts2 := env2.GetNewTimestamp()
	op2 := rep2.Add( ts2, env2.GetCurrentState(), value2)

	err = db.PutOp(givenKey, op2)
	ensure.Nil(t, err)
	env1.UpdateStateTS(ts2)

	rep1Read,err = db.Get(opcrdts.CRDT_SET_ADDWINS, givenKey)
	ensure.Nil(t, err)

	rep1r, ok = (*rep1Read).(*opcrdts.SetAddWins)
	ensure.True(t, ok)
	ensure.True(t, rep1r.Contains(value1) == true)
	ensure.True(t, rep1r.Contains(value2) == true)


}


func TestSetAddWinsRocks(t *testing.T) {
	HelperTestSetAddWinsRocks(t, true)
	HelperTestSetAddWinsRocks(t, false)
}

func HelperTestRemove(t *testing.T, doPut bool) {
	var (
		value = []byte("nuno")
		keyStr = fmt.Sprintf("hello%d", time.Now().UnixNano())
		givenKey    = []byte(keyStr)
	)
	db, err := OpenCRDTDB("tmp/test.db")
	ensure.Nil(t, err)
	defer db.Close()

	env := utils.NewSimpleEnvironment(utils.DCId("dc1"))
	rep := opcrdts.NewSetAddWins()

	ts1 := env.GetNewTimestamp()
	op1 := rep.Add( ts1, env.GetCurrentState(), value)
	if doPut {
		ok := rep.Apply(op1)
		ensure.True( t, ok)

		err = db.Put(givenKey, rep)
		ensure.Nil(t, err)

	} else {
		err = db.PutOp(givenKey, op1)
	}
	env.UpdateStateTS(ts1)
	rep1Read,err := db.Get(opcrdts.CRDT_SET_ADDWINS, givenKey)
	ensure.Nil(t, err)

	rep1r, ok := (*rep1Read).(*opcrdts.SetAddWins)
	ensure.True(t, ok)
	ensure.True(t, rep1r.Contains(value) == true)

	stateVV := env.GetCurrentState()
	tsRmv1 := env.GetNewTimestamp()
	opRmv1 := rep.Rmv( tsRmv1, stateVV, value)

	err = db.PutOp(givenKey, opRmv1)
	ensure.Nil(t, err)
	env.UpdateStateTS(tsRmv1)

	rep1Read,err = db.Get(opcrdts.CRDT_SET_ADDWINS, givenKey)
	ensure.Nil(t, err)
	rep1r, ok = (*rep1Read).(*opcrdts.SetAddWins)
	ensure.True(t, ok)
	ensure.True(t, rep1r.Contains(value) == false)
}

func TestRemove(t *testing.T) {
	HelperTestRemove(t, true)
	HelperTestRemove(t, false)
}



func HelperConcurrentAddRmvRocks(t *testing.T, doPut bool) {
	var (
		value = []byte("nuno")
		keyStr = fmt.Sprintf("hello%d", time.Now().UnixNano())
		givenKey    = []byte(keyStr)
	)
	db, err := OpenCRDTDB("tmp/test.db")
	ensure.Nil(t, err)
	defer db.Close()

	env := utils.NewSimpleEnvironment(utils.DCId("dc1"))
	rep := opcrdts.NewSetAddWins()

	ts1 := env.GetNewTimestamp()
	op1 := rep.Add( ts1, env.GetCurrentState(), value)
	if doPut {
		ok := rep.Apply(op1)
		ensure.True( t, ok)

		err = db.Put(givenKey, rep)
		ensure.Nil(t, err)

	} else {
		err = db.PutOp(givenKey, op1)
	}
	env.UpdateStateTS(ts1)
	rep1Read,err := db.Get(opcrdts.CRDT_SET_ADDWINS, givenKey)
	ensure.Nil(t, err)

	rep1r, ok := (*rep1Read).(*opcrdts.SetAddWins)
	ensure.True(t, ok)
	ensure.True(t, rep1r.Contains(value) == true)

	stateVV := env.GetCurrentState()
	tsRmv1 := env.GetNewTimestamp()
	opRmv1 := rep.Add( tsRmv1, stateVV, value)

	tsAdd1 := env.GetNewTimestamp()
	opAdd1 := rep.Add( tsAdd1, stateVV, value)

	tsAdd2 := env.GetNewTimestamp()
	opAdd2 := rep.Add( tsAdd2, stateVV, value)


	err = db.PutOp(givenKey, opRmv1)
	ensure.Nil(t, err)
	env.UpdateStateTS(tsRmv1)

	err = db.PutOp(givenKey, opAdd1)
	ensure.Nil(t, err)
	env.UpdateStateTS(tsAdd1)

	rep1Read,err = db.Get(opcrdts.CRDT_SET_ADDWINS, givenKey)
	ensure.Nil(t, err)
	rep1r, ok = (*rep1Read).(*opcrdts.SetAddWins)
	ensure.True(t, ok)
	ensure.True(t, rep1r.Contains(value) == true)

	err = db.PutOp(givenKey, opAdd2)
	ensure.Nil(t, err)
	env.UpdateStateTS(tsAdd2)

	rep1Read,err = db.Get(opcrdts.CRDT_SET_ADDWINS, givenKey)
	ensure.Nil(t, err)
	rep1r, ok = (*rep1Read).(*opcrdts.SetAddWins)
	ensure.True(t, ok)
	ensure.True(t, rep1r.Contains(value) == true)

	stateVV = env.GetCurrentState()
	tsRmv2 := env.GetNewTimestamp()
	opRmv2 := rep.Rmv( tsRmv2, stateVV, value)
	err = db.PutOp(givenKey, opRmv2)

	rep1Read,err = db.Get(opcrdts.CRDT_SET_ADDWINS, givenKey)
	ensure.Nil(t, err)
	rep1r, ok = (*rep1Read).(*opcrdts.SetAddWins)
	ensure.True(t, ok)
	ensure.True(t, rep1r.Contains(value) == false)

}

func TestConcurrentAddRmvRocks(t *testing.T) {
	HelperConcurrentAddRmvRocks(t, true)
	HelperConcurrentAddRmvRocks(t, false)
}

