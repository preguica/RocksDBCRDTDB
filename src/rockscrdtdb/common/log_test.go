// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
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

	db, err := OpenLog("tmp/test.db", true, NewNullMergeOperator())
	ensure.Nil(t, err)
	defer db.Close()

	err = db.Put(givenKey, givenVal)
	ensure.Nil(t, err)

	value, err := db.Get(givenKey)
	defer value.Free()
	ensure.Nil(t, err)

	ensure.DeepEqual(t, value.Data(), givenVal)
}
