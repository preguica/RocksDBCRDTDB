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





