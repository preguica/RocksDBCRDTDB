package common

import (
	"github.com/tecbot/gorocksdb"
)

type Log struct {
	db *gorocksdb.DB
	ro *gorocksdb.ReadOptions
	wo *gorocksdb.WriteOptions
}


func OpenLog( logName string, merger gorocksdb.MergeOperator) (*Log, error)  {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	opts.SetMergeOperator( merger)
	db, err := gorocksdb.OpenDb(opts, logName)
	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	return &Log{ db, ro, wo}, err
}

func (l *Log) Close() {
	l.db.Close()
}

func (l *Log) Get( key []byte) (*gorocksdb.Slice, error) {
	return l.db.Get(l.ro, key)
}

func (l *Log) Put( key []byte, value []byte) error {
	return l.db.Put(l.wo, key, value)
}

func (l *Log) Merge( key []byte, value []byte) error {
	return l.db.Merge(l.wo, key, value)
}
