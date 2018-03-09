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

//	vv := env.GetCurrentState()
	ts := env.GetNewTimestamp()

	op := opcrdts.NewCounterOpAdd(3)

	mvOp := NewMvDBCRDTOperation( op, ts)
	b,ok := mvOp.Serialize();
	ensure.True( t, ok)
	ensure.NotNil( t, b)
	b,ok = op.Serialize();
	ensure.True( t, ok)
	ensure.NotNil( t, b)
	//TODO: need to unserialize
}

