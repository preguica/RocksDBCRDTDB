package common

import (
	"github.com/tecbot/gorocksdb"
	"errors"
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

func (l *Log) Delete( key []byte) error {
	return l.db.Delete(l.wo, key)
}

func (l *Log) WriteBatch() *LogBatchImpl {
	return NewLogBatchImpl( gorocksdb.NewWriteBatch())
}

func (l *Log) RunBatch( batch LogBatchInterface) error {
	batchImpl, ok := batch.(*LogBatchImpl)
	if ok == false {
		errors.New("Unexpect batch type")
	}
	return l.db.Write( l.wo, batchImpl.batch)
}



type LogBatchImpl struct {
	batch *gorocksdb.WriteBatch
}

func NewLogBatchImpl( batch *gorocksdb.WriteBatch) *LogBatchImpl{
	return &LogBatchImpl{batch}
}

func (l *LogBatchImpl) Put( key []byte, value []byte) error {
	l.batch.Put(key, value)
	return nil
}

func (l *LogBatchImpl) Merge( key []byte, value []byte) error {
	l.batch.Merge(key, value)
	return nil
}

func (l *LogBatchImpl) Delete( key []byte) error {
	l.batch.Delete(key)
	return nil
}

func (l *LogBatchImpl) Destroy() {
	l.batch.Destroy()
}
