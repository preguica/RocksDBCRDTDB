// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
package mvdb

import (
	"testing"
	"github.com/facebookgo/ensure"
	"rockscrdtdb/utils"
	"rockscrdtdb/opcrdts"
)

func TestSerializationOfCounterOp(t *testing.T) {
	env := utils.NewSimpleEnvironment(utils.DCId("dc1"))
	env.UpdateStateTS( env.GetNewTimestamp())

	vv := env.GetCurrentState()
	ts := env.GetNewTimestamp()

	op := opcrdts.NewCounterOpAdd( ts, vv, 3)

	mvOp := NewMvDBCRDTOperation( op, ts)
	b,ok := mvOp.Serialize();
	ensure.True( t, ok)
	ensure.NotNil( t, b)
	b,ok = op.Serialize();
	ensure.True( t, ok)
	ensure.NotNil( t, b)
	//TODO: need to unserialize
}

