package mvdb

import (
	"rockscrdtdb/utils"
	"encoding/binary"
	"crypto/md5"
	"rockscrdtdb/opcrdts"
)

const KEY_PREFIX_SIZE = md5.Size + 1

func createKeyBase( key []byte) []byte {
	b := md5.Sum( key)
	return b[:]
}

// Auxiliary function to create a key for storing the stable version, given the key encoded as an hash
// key = [MD5(key)][CRDT_RESERVED_STABLE][crdt_type]
func createStableKeyWithBase( t byte, keyBase []byte) []byte {
	size := len(keyBase)
	arr := make([]byte, size+2)
	for i, b := range keyBase {
		arr[i] = b
	}
	arr[size] = opcrdts.CRDT_RESERVED_STABLE
	arr[size+1] = t
	return arr
}

// Auxiliary function to create a key that encodes the opcrdts.CRDT type, given the key encoded as an hash
// key = [MD5(key)][CRDT_RESERVED_LAST][crdt_type]
func createKeyWithBase( t byte, keyBase []byte) []byte {
	size := len(keyBase)
	arr := make([]byte, size+2)
	for i, b := range keyBase {
		arr[i] = b
	}
	arr[size] = opcrdts.CRDT_RESERVED_LAST
	arr[size+1] = t
	return arr
}


// Auxiliary function to create a key that encodes the opcrdts.CRDT type
// key = [MD5(key)][CRDT_RESERVED_LAST][crdt_type]
func createKey( t byte, key []byte) []byte {
	return createKeyWithBase( t, createKeyBase( key))
}

// Auxiliary function to create a key that encodes the CRDT type and the timestamp of operation, given the key
// encoded as an hash
// key = [MD5(key)][CRDT_RESERVED_OPS][8_byte_time][site_id]
func createBaseOpKeyWithBase( key []byte) []byte {
	size := len(key)
	arr := make([]byte, size+1)
	for i, b := range key {
		arr[i] = b
	}
	arr[size] = opcrdts.CRDT_RESERVED_OPS
	return arr
}

// Auxiliary function to create a key that encodes the CRDT type and the timestamp of operation, given the key
// encoded as an hash
// key = [MD5(key)][CRDT_RESERVED_OPS][8_byte_time][site_id]
func createOpKeyWithBase( t byte, ts *utils.Timestamp, key []byte) []byte {
	size := len(key)
	arr := make([]byte, size+9+len(ts.Dc))
	for i, b := range key {
		arr[i] = b
	}
	arr[size] = opcrdts.CRDT_RESERVED_OPS
	binary.BigEndian.PutUint64(arr[size+1:], uint64(ts.Ts))
	size = size + 9
	barr := []byte(ts.Dc)
	for _, b := range barr {
		arr[size] = b
		size++
	}
	return arr
}

// Auxiliary function to create a key that encodes the CRDT type and the timestamp of operation
// key = [MD5(key)][CRDT_RESERVED_OPS][8_byte_time][site_id]
func createOpKey( t byte, ts *utils.Timestamp, key []byte) []byte {
	return createOpKeyWithBase( t, ts, createKeyBase( key))
}


