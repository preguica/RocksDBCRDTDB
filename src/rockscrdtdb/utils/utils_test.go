// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
package utils

import (
	"testing"
	"github.com/facebookgo/ensure"
	"fmt"
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
	fmt.Println( len(t1.Dc))
}
