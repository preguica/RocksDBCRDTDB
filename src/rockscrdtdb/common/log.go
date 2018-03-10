package common

import (
	"github.com/tecbot/gorocksdb"
)

type Log struct {
	db *gorocksdb.DB
	ro *gorocksdb.ReadOptions
	wo *gorocksdb.WriteOptions
}


func OpenLogExtended( logName string, funOpts func(options *gorocksdb.Options, blockOption *gorocksdb.BlockBasedTableOptions)) (*Log, error)  {
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	bbto.SetCacheIndexAndFilterBlocks( true)
	opts := gorocksdb.NewDefaultOptions()
	funOpts( opts, bbto)
	opts.SetBlockBasedTableFactory(bbto)
	db, err := gorocksdb.OpenDb(opts, logName)
	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	return &Log{ db, ro, wo}, err
}

func OpenLog( logName string, createIfMissing bool, merger gorocksdb.MergeOperator) (*Log, error)  {
	return OpenLogExtended( logName, func(options *gorocksdb.Options, blockOption *gorocksdb.BlockBasedTableOptions) {
		options.SetCreateIfMissing(createIfMissing)
		options.SetMergeOperator( merger)
	});
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

func (l *Log) NewIterator() *gorocksdb.Iterator {
	return l.db.NewIterator( l.ro)
}
