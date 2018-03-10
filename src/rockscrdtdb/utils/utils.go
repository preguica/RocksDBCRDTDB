// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
package utils

import (
	"strings"
)

type AnyValue interface{}

type DCId string

// Returns negative is this DCIs is lexicographically smaller than otherId,
// 0 if it is equal and 1 if it is larger
func (id DCId)CompareTo( otherId *DCId) int {
	return strings.Compare(string(id),string(*otherId))
}





