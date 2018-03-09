package opcrdts

import (
	"testing"
	"github.com/facebookgo/ensure"
	"rockscrdtdb/utils"
)

func TestSetAddWins(t *testing.T) {
	var (
		value1 = []byte("nuno")
		value2 = []byte("elsa")
	)

	env1 := utils.NewSimpleEnvironment(utils.DCId("dc1"))
	rep1 := NewSetAddWins()

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
	rep2 := NewSetAddWins()

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
	repcopy,ok := UnserializeSetAddWins(b)
	ensure.True( t, ok == true)

	rep1copy,ok := repcopy.(*SetAddWins)
	ensure.True( t, ok == true)

	ensure.True( t, len(rep1copy.Vals) == len(rep1.Vals))
}



