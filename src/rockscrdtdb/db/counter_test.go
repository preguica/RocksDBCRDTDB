package db

import (
"testing"
"github.com/facebookgo/ensure"
	"time"
	"fmt"
	"rockscrdtdb/utils"
	"rockscrdtdb/opcrdts"
	"regexp"
)

func TestDBCounter(t *testing.T) {
	var (
		givenKey    = []byte("hello")
	)
	db, err := OpenCRDTDB("tmp/test.db")
	ensure.Nil(t, err)
	defer db.Close()

	cnt := opcrdts.NewCounter()

	err = db.Put(givenKey, cnt)
	ensure.Nil(t, err)

	cntRead,err := db.Get(opcrdts.CRDT_OPCOUNTER, givenKey)
	ensure.Nil(t, err)


	cnt2, ok := (*cntRead).(*opcrdts.Counter)
	ensure.True(t, ok)
	ensure.DeepEqual(t, cnt, cnt2)

	err = db.PutOp(givenKey, cnt2.Add( nil, nil,1))
	ensure.Nil(t, err)
	err = db.PutOp(givenKey, cnt2.Add(&utils.Timestamp{}, &utils.VersionVector{},2))
	ensure.Nil(t, err)

	cntRead, err = db.Get(opcrdts.CRDT_OPCOUNTER, givenKey)
	ensure.Nil(t, err)

	cnt3, ok := (*cntRead).(*opcrdts.Counter)
	ensure.True(t, ok)
	ensure.True( t, cnt3.Val() == cnt.Val() + 1 + 2)

	//	value, err := db.Get(givenKey)
	//	defer value.Free()
	//	ensure.Nil(t, err)

}

func TestCounterNoPut(t *testing.T) {
	var (
		keyStr = fmt.Sprintf("hello%d", time.Now().UnixNano())
		givenKey    = []byte(keyStr)
	)
	db, err := OpenCRDTDB("tmp/test.db")
	ensure.Nil(t, err)
	defer db.Close()

	cntRead,err := db.Get(opcrdts.CRDT_OPCOUNTER, givenKey)
	ensure.Err(t, err, regexp.MustCompile("(does not exist)"))

	cnt := opcrdts.NewCounter()

	err = db.PutOp(givenKey, cnt.Add(nil, nil, 1))
	ensure.Nil(t, err)
	err = db.PutOp(givenKey, cnt.Add(nil, nil, 2))
	ensure.Nil(t, err)

	cntRead, err = db.Get(opcrdts.CRDT_OPCOUNTER, givenKey)
	ensure.Nil(t, err)

	cnt3, ok := (*cntRead).(*opcrdts.Counter)
	ensure.True(t, ok)
	ensure.True( t, cnt3.Val() == 1 + 2)

	//	value, err := db.Get(givenKey)
	//	defer value.Free()
	//	ensure.Nil(t, err)

}

