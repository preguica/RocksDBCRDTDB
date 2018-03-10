package mvdb

import (
	"fmt"
	"time"
	"github.com/facebookgo/ensure"
	"rockscrdtdb/utils"
	"rockscrdtdb/opcrdts"
	"testing"
)

func TestMvCounter(t *testing.T) {
	var (
		keyStr = fmt.Sprintf("cnt%d", time.Now().UnixNano())
		givenKey    = []byte(keyStr)
	)
	db, err := OpenCreateIfNotCRDTMvDB("tmp/test.mvdb", true, true)
	ensure.Nil(t, err)
	defer db.Close()

	env := utils.NewSimpleEnvironment(utils.DCId("dc1"))

	cnt := opcrdts.NewCounter()
	ts := env.GetNewTimestamp()
	vv := env.GetCurrentState()
	op := cnt.Add(ts, vv, 1)
	ok := cnt.Apply( op)
	ensure.True(t, ok)

	mvOp := NewMvDBCRDT( cnt, vv)
	err = db.Put(givenKey, mvOp)
	ensure.Nil(t, err)

	cntRead, err := db.Get(opcrdts.CRDT_OPCOUNTER, givenKey)
	ensure.Nil(t, err)
	cnt3, ok := cntRead.Obj.(*opcrdts.Counter)
	ensure.True(t, ok)
	ensure.True( t, cnt3.Val() == 1)

	//	value, err := db.Get(givenKey)
	//	defer value.Free()
	//	ensure.Nil(t, err)

}

func TestMvCounterOp(t *testing.T) {
	var (
		keyStr = fmt.Sprintf("cnt%d", time.Now().UnixNano())
		givenKey    = []byte(keyStr)
	)
	db, err := OpenCreateIfNotCRDTMvDB("tmp/test.mvdb", true, true)
	ensure.Nil(t, err)
	defer db.Close()

	env := utils.NewSimpleEnvironment(utils.DCId("dc1"))

	cnt := opcrdts.NewCounter()
	ts := env.GetNewTimestamp()
	vv := env.GetCurrentState()
	op := cnt.Add(ts, vv, 1)

	mvOp := NewMvDBCRDTOperation( op, ts)
	err = db.PutOp(givenKey, mvOp)
	ensure.Nil(t, err)

	cntRead, err := db.Get(opcrdts.CRDT_OPCOUNTER, givenKey)
	ensure.Nil(t, err)
	cnt3, ok := cntRead.Obj.(*opcrdts.Counter)
	ensure.True(t, ok)
	ensure.True( t, cnt3.Val() == 1)
}

func TestMvCounterVersions(t *testing.T) {
	var (
		keyStr = fmt.Sprintf("cnt%d", time.Now().UnixNano())
		givenKey    = []byte(keyStr)
	)
	db, err := OpenCreateIfNotCRDTMvDB("tmp/test.mvdb", true, true)
	ensure.Nil(t, err)
	defer db.Close()

	env1 := utils.NewSimpleEnvironment(utils.DCId("dc1"))
	cnt1 := opcrdts.NewCounter()
	vv := env1.GetCurrentState()

	//=======================================================================
	// Add 1 with timestamp ts1
	ts1 := env1.GetNewTimestamp()
	op1 := cnt1.Add(ts1, vv, 1)
	mvOp := NewMvDBCRDTOperation( op1, ts1)
	err = db.PutOp(givenKey, mvOp)
	ensure.Nil(t, err)
	env1.UpdateStateTS( ts1)

	//copy of version state after executing op1
	vv1 := env1.GetCurrentState()
	ensure.NotNil(t,vv1)

	//=======================================================================
	// Add 2 with timestamp ts2
	ts2 := env1.GetNewTimestamp()
	op2 := cnt1.Add(ts2, vv1, 2)
	mvOp = NewMvDBCRDTOperation( op2, ts2)
	err = db.PutOp(givenKey, mvOp)
	ensure.Nil(t, err)
	env1.UpdateStateTS( ts2)

	//copy of version state after executing op1
	vv2 := env1.GetCurrentState()
	ensure.NotNil(t,vv2)

	//=======================================================================
	//=======================================================================
	//=======================================================================
	env2 := utils.NewSimpleEnvironment(utils.DCId("dc2"))
	cnt2 := opcrdts.NewCounter()
	vv = env2.GetCurrentState()

	//=======================================================================
	// Add 4 with timestamp ts3
	ts3 := env2.GetNewTimestamp()
	op3 := cnt2.Add(ts3, vv, 4)
	mvOp = NewMvDBCRDTOperation( op3, ts3)
	err = db.PutOp(givenKey, mvOp)
	ensure.Nil(t, err)
	env2.UpdateStateTS( ts3)

	//copy of version state after executing op1
	vv3 := env2.GetCurrentState()
	ensure.NotNil(t,vv3)

	//=======================================================================
	cntRead, err := db.Get(opcrdts.CRDT_OPCOUNTER, givenKey)
	ensure.Nil(t, err)
	cnt3, ok := cntRead.Obj.(*opcrdts.Counter)
	ensure.True(t, ok)
	ensure.True( t, cnt3.Val() == 7)

	cntRead, err = db.GetVersion(opcrdts.CRDT_OPCOUNTER, givenKey, vv1)
	ensure.Nil(t, err)
	cnt3, ok = cntRead.Obj.(*opcrdts.Counter)
	ensure.True(t, ok)
	ensure.True( t, cnt3.Val() == 1)

	cntRead, err = db.GetVersion(opcrdts.CRDT_OPCOUNTER, givenKey,vv2)
	ensure.Nil(t, err)
	cnt3, ok = cntRead.Obj.(*opcrdts.Counter)
	ensure.True(t, ok)
	ensure.True( t, cnt3.Val() == 3)

	cntRead, err = db.GetVersion(opcrdts.CRDT_OPCOUNTER, givenKey,vv3)
	ensure.Nil(t, err)
	cnt3, ok = cntRead.Obj.(*opcrdts.Counter)
	ensure.True(t, ok)
	ensure.True( t, cnt3.Val() == 4)

	vv4 := utils.NewVersionVector()
	vv4.AddTS(ts1)
	vv4.AddTS(ts3)
	cntRead, err = db.GetVersion(opcrdts.CRDT_OPCOUNTER, givenKey,vv4)
	ensure.Nil(t, err)
	cnt3, ok = cntRead.Obj.(*opcrdts.Counter)
	ensure.True(t, ok)
	ensure.True( t, cnt3.Val() == 5)

}

