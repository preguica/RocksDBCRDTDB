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
	db, err := OpenCRDTMvDB("tmp/test.db")
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
	db, err := OpenCRDTMvDB("tmp/test.db")
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