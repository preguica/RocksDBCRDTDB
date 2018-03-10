// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
package opcrdts

import (
"testing"
"github.com/facebookgo/ensure"
)

func TestCounterSimpleOp(t *testing.T) {
	cnt := &Counter{10}
	ok := cnt.Apply(cnt.Add(nil,nil, 3))
	ensure.True(t, ok)
	ok = cnt.Apply(cnt.Add(nil,nil, 1))
	ensure.True(t, ok)
	ensure.True( t, cnt.val == 14)
}

