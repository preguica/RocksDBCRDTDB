package utils

import (
	"testing"
	"github.com/facebookgo/ensure"
)

func TestTimestampGeneration(t *testing.T) {
	var(
		dc1 = "dc1"
	)

	id := DCId(dc1)

	env := SimpleEnvironment{Dc: id}
	ensure.True(t, env.LastTs == 0)


	t1 := env.GetNewTimestamp()
	t2 := env.GetNewTimestamp()
	ensure.True(t, t1.CompareTo(t2) < 0)
}
