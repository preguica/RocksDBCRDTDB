// Copyright 2018 Nuno Preguica, NOVA LINCS, FCT, Universidade NOVA de Lisboa.
// All rights reserved.
// Use of this source code is governed by Apache 2.0
// license that can be found in the LICENSE file.
package db

// Auxiliary function to create a key that encodes the opcrdts.CRDT type
func createKey( t byte, key []byte) []byte {
	key = key[0:len(key)+1]
	key[len(key)-1] = t
	return key
}

