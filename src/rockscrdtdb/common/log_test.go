package common

import (
	"testing"
	"github.com/facebookgo/ensure"
)

func TestSimpleWriteAndRead(t *testing.T) {
	var (
		givenKey    = []byte("foo")
		givenVal   = []byte("bar")
	)

	db, err := OpenLog("tmp/test.db", NewNullMergeOperator())
	ensure.Nil(t, err)
	defer db.Close()

	err = db.Put(givenKey, givenVal)
	ensure.Nil(t, err)

	value, err := db.Get(givenKey)
	defer value.Free()
	ensure.Nil(t, err)

	ensure.DeepEqual(t, value.Data(), givenVal)
}
