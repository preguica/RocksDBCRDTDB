package common

type LogBatchInterface interface {
	Put( key []byte, value []byte) error
	Merge( key []byte, value []byte) error
	Delete( key []byte) error
}

