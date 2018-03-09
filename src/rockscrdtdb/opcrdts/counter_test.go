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

