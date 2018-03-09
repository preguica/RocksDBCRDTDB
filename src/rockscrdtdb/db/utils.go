package db

// Auxiliary function to create a key that encodes the opcrdts.CRDT type
func createKey( t byte, key []byte) []byte {
	key = key[0:len(key)+1]
	key[len(key)-1] = t
	return key
}

