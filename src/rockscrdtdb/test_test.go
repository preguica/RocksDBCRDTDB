package rockscrdtdb

import (
	"testing"
	"encoding/json"
	"github.com/facebookgo/ensure"
	"fmt"
	"crypto/md5"
	"crypto/sha1"
)

type response1 struct {
	Page   int
	Fruits []string
}


type X struct {
	A int		`json:"a"`
	B float32	`json:"b"`
}

func TestTest(t *testing.T) {
	x := &X{ A:10, B:4.3}

	fmt.Println(x.A)

	res1D := &response1{
		Page:   1,
		Fruits: []string{"apple", "peach", "pear"}}
	res1B, _ := json.Marshal(res1D)
	fmt.Println(string(res1B))

	b,err := json.Marshal( x)
	ensure.True( t, err == nil)
	fmt.Println(string(b))
}

func BenchmarkMDSize10(b *testing.B) {
	b1 := []byte("0123456789")
	for i := 0; i < b.N; i++ {
		md5.Sum(b1)
	}
}

func BenchmarkMDSize20(b *testing.B) {
	b1 := []byte("01234567890123456789")
	for i := 0; i < b.N; i++ {
		md5.Sum(b1)
	}
}

func BenchmarkSHASize10(b *testing.B) {
	b1 := []byte("0123456789")
	for i := 0; i < b.N; i++ {
		sha1.Sum(b1)
	}
}

func BenchmarkSHASize20(b *testing.B) {
	b1 := []byte("01234567890123456789")
	for i := 0; i < b.N; i++ {
		sha1.Sum(b1)
	}
}
