package versions

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/ristretto"
)

// BadgerV2 wraps the Badger v2 database to implement the BadgerDB interface.
type BadgerV2 struct {
	*badger.DB
}

func (b *BadgerV2) Close() error {
	return b.DB.Close()
}

func (b *BadgerV2) IsClosed() bool {
	return b.DB.IsClosed()
}

func (b *BadgerV2) NewStream() BadgerStream {
	return &BadgerV2Stream{b.DB.NewStream()}
}

func (b *BadgerV2) Update(fn func(txn Txn) error) error {
	return b.DB.Update(func(txn *badger.Txn) error {
		return fn(&BadgerV2Txn{txn})
	})
}

func (b *BadgerV2) View(fn func(txn Txn) error) error {
	return b.DB.View(func(txn *badger.Txn) error {
		return fn(&BadgerV2Txn{txn})
	})
}

func (b *BadgerV2) NewTransaction(update bool) Txn {
	return &BadgerV2Txn{b.DB.NewTransaction(update)}
}

func (b *BadgerV2) RunValueLogGC(discardRatio float64) error {
	return b.DB.RunValueLogGC(discardRatio)
}

func (b *BadgerV2) Sync() error {
	return b.DB.Sync()
}

func (b *BadgerV2) MaxBatchCount() int64 {
	return b.DB.MaxBatchCount()
}

func (b *BadgerV2) MaxBatchSize() int64 {
	return b.DB.MaxBatchSize()
}

func (b *BadgerV2) Subscribe(ctx context.Context, cb func(kv *KVList) error, prefixes ...[]byte) error {
	return b.DB.Subscribe(ctx, cb, prefixes...)
}

func (b *BadgerV2) BlockCacheMetrics() *ristretto.Metrics {
	return b.DB.BlockCacheMetrics()
}

func (b *BadgerV2) IndexCacheMetrics() *ristretto.Metrics {
	return b.DB.IndexCacheMetrics()
}

func (b *BadgerV2) GetErrKeyNotFound() error {
	return badger.ErrKeyNotFound
}

func (b *BadgerV2) GetErrNoRewrite() error {
	return badger.ErrNoRewrite
}

func (b *BadgerV2) NewWriteBatch() WriteBatch {
	return &BadgerV2WriteBatch{b.DB.NewWriteBatch()}
}

func (b *BadgerV2) Flatten(workers int) error{
	return b.DB.Flatten(workers)
}

func (b *BadgerV2) Size() (lsm int64, vlog int64){
return b.DB.Size()
}

type BadgerV2WriteBatch struct {
	*badger.WriteBatch
}

func (wb *BadgerV2WriteBatch) Set(key, val []byte) error {
	return wb.WriteBatch.Set(key, val)
}

func (wb *BadgerV2WriteBatch) Delete(key []byte) error {
	return wb.WriteBatch.Delete(key)
}

func (wb *BadgerV2WriteBatch) Flush() error {
	return wb.WriteBatch.Flush()
}

func (wb *BadgerV2WriteBatch) Cancel() {
	wb.WriteBatch.Cancel()
}

type BadgerV2Stream struct {
	*badger.Stream
}

func (s *BadgerV2Stream) SetNumGo(numGo int) {
	s.NumGo = numGo
}

func (s *BadgerV2Stream) SetLogPrefix(prefix string) {
	s.LogPrefix = prefix
}

func (s *BadgerV2Stream) Send(buf *Buffer) error {
	/* ??? */
	fmt.Println("MIKE")
	return nil
}

func (s *BadgerV2Stream) Orchestrate(ctx context.Context) error {
	return s.Stream.Orchestrate(ctx)
}

type BadgerV2Txn struct {
	*badger.Txn
}

func (txn *BadgerV2Txn) Get(key []byte) (Item, error) {
	item, err := txn.Txn.Get(key)
	return &BadgerV2Item{item}, err
}

func (txn *BadgerV2Txn) Set(key, val []byte) error {
	return txn.Txn.Set(key, val)
}

func (txn *BadgerV2Txn) Delete(key []byte) error {
	return txn.Txn.Delete(key)
}

func (txn *BadgerV2Txn) Commit() error {
	return txn.Txn.Commit()
}

func (txn *BadgerV2Txn) Discard() {
	txn.Txn.Discard()
}

type BadgerV2Item struct {
	*badger.Item
}

func (item *BadgerV2Item) Value(fn func([]byte) error) error {
	return item.Item.Value(fn)
}

func (item *BadgerV2Item) Key() []byte {
	return item.Item.Key()
}

func (item *BadgerV2Item) Version() uint64 {
	return item.Item.Version()
}

func (item *BadgerV2Item) ValueCopy(dst []byte) ([]byte, error) {
	return item.Item.ValueCopy(dst)
}

func (item *BadgerV2Item) ValueSize() int64 {
	return item.Item.ValueSize()
}
