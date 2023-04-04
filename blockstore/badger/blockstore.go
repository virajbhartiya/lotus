package badgerbs

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/dgraph-io/badger/v2"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	blocks "github.com/ipfs/go-libipfs/blocks"
	logger "github.com/ipfs/go-log/v2"
	pool "github.com/libp2p/go-buffer-pool"
	"github.com/multiformats/go-base32"
	"go.uber.org/zap"

	"github.com/filecoin-project/lotus/blockstore"
)

var (
	// KeyPool is the buffer pool we use to compute storage keys.
	KeyPool *pool.BufferPool = pool.GlobalPool
)

var (
	// ErrBlockstoreClosed is returned from blockstore operations after
	// the blockstore has been closed.
	ErrBlockstoreClosed = fmt.Errorf("badger blockstore closed")

	log = logger.Logger("badgerbs")
)

// Options embeds the badger options themselves, and augments them with
// blockstore-specific options.
type Options struct {
	ConnectionString string
	SyncWrites       bool

	// Prefix is an optional prefix to prepend to keys. Default: "".
	Prefix string
}

// Data format sent to badger server
type Request struct {
	Action string `json:"action"`
	Key    []byte `json:"key,omitempty"`
	Value  []byte `json:"value,omitempty"`

	Keys   [][]byte `json:"keys,omitempty"`   //used for PutMany
	Values [][]byte `json:"values,omitempty"` //used for PutMany
}

func DefaultOptions(path string) Options {
	return Options{
		ConnectionString: "localhost:8080",
		Prefix:           "",
	}
}

// badgerLogger is a local wrapper for go-log to make the interface
// compatible with badger.Logger (namely, aliasing Warnf to Warningf)
type badgerLogger struct {
	*zap.SugaredLogger // skips 1 caller to get useful line info, skipping over badger.Options.

	skip2 *zap.SugaredLogger // skips 2 callers, just like above + this logger.
}

// Warningf is required by the badger logger APIs.
func (b *badgerLogger) Warningf(format string, args ...interface{}) {
	b.skip2.Warnf(format, args...)
}

// bsState is the current blockstore state
type bsState int

const (
	// stateOpen signifies an open blockstore
	stateOpen bsState = iota
	// stateClosing signifies a blockstore that is currently closing
	stateClosing
	// stateClosed signifies a blockstore that has been colosed
	stateClosed
)

type bsMoveState int

const (
	// moveStateNone signifies that there is no move in progress
	moveStateNone bsMoveState = iota
	// moveStateMoving signifies that there is a move  in a progress
	moveStateMoving
	// moveStateCleanup signifies that a move has completed or aborted and we are cleaning up
	moveStateCleanup
	// moveStateLock signifies that an exclusive lock has been acquired
	moveStateLock
)

// Blockstore is a badger-backed IPLD blockstore.
type Blockstore struct {
	stateLk   sync.RWMutex
	state     bsState
	viewers   sync.WaitGroup
	moveMx    sync.Mutex
	rlock     int
	moveState bsMoveState
	moveCond  sync.Cond

	db     net.Conn
	writer *bufio.Writer
	reader *bufio.Reader
	opts   Options

	prefixing bool
	prefix    []byte
	prefixLen int
}

var _ blockstore.Blockstore = (*Blockstore)(nil)
var _ blockstore.Viewer = (*Blockstore)(nil)
var _ blockstore.BlockstoreIterator = (*Blockstore)(nil)
var _ blockstore.BlockstoreGC = (*Blockstore)(nil)
var _ blockstore.BlockstoreSize = (*Blockstore)(nil)
var _ io.Closer = (*Blockstore)(nil)

// Open creates a new badger-backed blockstore, with the supplied options.
func Open(opts Options) (*Blockstore, error) {

	connection, err := net.Dial("tcp", opts.ConnectionString)
	reader := bufio.NewReader(connection)
	writer := bufio.NewWriter(connection)

	if err != nil {
		return nil, fmt.Errorf("failed to connect to badger server: %w", err)
	}

	bs := &Blockstore{db: connection, writer: writer, reader: reader, opts: opts}
	if p := opts.Prefix; p != "" {
		bs.prefixing = true
		bs.prefix = []byte(p)
		bs.prefixLen = len(bs.prefix)
	}

	return bs, nil
}

// Close closes the store. If the store has already been closed, this noops and
// returns an error, even if the first closure resulted in error.
func (b *Blockstore) Close() error {
	b.stateLk.Lock()
	if b.state != stateOpen {
		b.stateLk.Unlock()
		return nil
	}
	b.state = stateClosing
	b.stateLk.Unlock()

	defer func() {
		b.stateLk.Lock()
		b.state = stateClosed
		b.stateLk.Unlock()
	}()

	// wait for all accesses to complete
	b.viewers.Wait()

	return b.db.Close()
}

func (b *Blockstore) access() error {
	b.stateLk.RLock()
	defer b.stateLk.RUnlock()

	if b.state != stateOpen {
		return ErrBlockstoreClosed
	}

	b.viewers.Add(1)
	return nil
}

func (b *Blockstore) isOpen() bool {
	b.stateLk.RLock()
	defer b.stateLk.RUnlock()

	return b.state == stateOpen
}

// lockDB/unlockDB implement a recursive lock contingent on move state
func (b *Blockstore) lockDB() {
	b.moveMx.Lock()
	defer b.moveMx.Unlock()

	if b.rlock == 0 {
		for b.moveState == moveStateLock {
			b.moveCond.Wait()
		}
	}

	b.rlock++
}

func (b *Blockstore) unlockDB() {
	b.moveMx.Lock()
	defer b.moveMx.Unlock()

	b.rlock--
	if b.rlock == 0 && b.moveState == moveStateLock {
		b.moveCond.Broadcast()
	}
}

// CollectGarbage compacts and runs garbage collection on the value log;
// implements the BlockstoreGC trait
func (b *Blockstore) CollectGarbage(ctx context.Context, opts ...blockstore.BlockstoreGCOption) error {
	return nil
}

// GCOnce runs garbage collection on the value log;
// implements BlockstoreGCOnce trait
func (b *Blockstore) GCOnce(ctx context.Context, opts ...blockstore.BlockstoreGCOption) error {
	return nil
}

// Size returns the aggregate size of the blockstore
func (b *Blockstore) Size() (int64, error) {

	req := Request{Action: "Size"}
	response, err := b.query(req)
	if err != nil {
		return 0, err
	}

	_ = response
	return 0, nil //todo change to actual result bool value
}

// View implements blockstore.Viewer, which leverages zero-copy read-only
// access to values.
func (b *Blockstore) View(ctx context.Context, cid cid.Cid, fn func([]byte) error) error {
	defer b.viewers.Done()

	b.lockDB()
	defer b.unlockDB()

	item, err := b.Get(ctx, cid)

	switch err {
	case nil:
		return fn(item.RawData()) //todo is this fn call correct?
	case badger.ErrKeyNotFound:
		return ipld.ErrNotFound{Cid: cid}
	default:
		return fmt.Errorf("failed to view block from badger blockstore: %w", err)
	}
}

func (b *Blockstore) Flush(context.Context) error {
	defer b.viewers.Done()

	b.lockDB()
	defer b.unlockDB()
	req := Request{Action: "Flush"}
	_, err := b.query(req)
	return err
}

func (b *Blockstore) query(req Request) (string, error) {

	jsonReq, err := json.Marshal(req)
	if err != nil {
		return "", err
	}

	_, err = b.writer.WriteString(string(jsonReq) + "\n")
	if err != nil {
		return "", err
	}

	err = b.writer.Flush()
	if err != nil {
		return "", err
	}

	response, err := b.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return response, nil

}

// Has implements Blockstore.Has.
func (b *Blockstore) Has(ctx context.Context, cid cid.Cid) (bool, error) {
	if err := b.access(); err != nil {
		return false, err
	}
	defer b.viewers.Done()

	b.lockDB()
	defer b.unlockDB()

	k, pooled := b.PooledStorageKey(cid)
	if pooled {
		defer KeyPool.Put(k)
	}

	req := Request{Action: "Has", Key: k}
	response, err := b.query(req)
	if err != nil {
		return false, err
	}

	_ = response
	return err != nil, nil //todo change to actual Has bool value

}

// Get implements Blockstore.Get.
func (b *Blockstore) Get(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
	fmt.Println("Get:", cid)
	if !cid.Defined() {
		return nil, ipld.ErrNotFound{Cid: cid}
	}

	if err := b.access(); err != nil {
		return nil, err
	}
	defer b.viewers.Done()

	b.lockDB()
	defer b.unlockDB()

	k, pooled := b.PooledStorageKey(cid)
	if pooled {
		defer KeyPool.Put(k)
	}

	req := Request{Action: "Get", Key: k}
	response, err := b.query(req)
	if err != nil {
		return nil, err
	}

	return blocks.NewBlockWithCid([]byte(response), cid)
}

// GetSize implements Blockstore.GetSize.
func (b *Blockstore) GetSize(ctx context.Context, cid cid.Cid) (int, error) {
	if err := b.access(); err != nil {
		return 0, err
	}
	defer b.viewers.Done()

	b.lockDB()
	defer b.unlockDB()

	k, pooled := b.PooledStorageKey(cid)
	if pooled {
		defer KeyPool.Put(k)
	}

	req := Request{Action: "GetSize", Key: k}
	response, err := b.query(req)
	if err != nil {
		return -1, err
	}

	var size int
	if err != nil {
		size = -1
	}
	//todo get size from response
	_ = response
	size = 1
	return size, err
}

// Put implements Blockstore.Put.
func (b *Blockstore) Put(ctx context.Context, block blocks.Block) error {
	fmt.Println("Put:", block)
	if err := b.access(); err != nil {
		return err
	}
	defer b.viewers.Done()

	b.lockDB()
	defer b.unlockDB()

	k, pooled := b.PooledStorageKey(block.Cid())
	if pooled {
		defer KeyPool.Put(k)
	}
	req := Request{Action: "Put", Key: k, Value: block.RawData()}
	response, err := b.query(req)
	if err != nil {
		return err
	}

	_ = response
	return nil
}

// PutMany implements Blockstore.PutMany.
func (b *Blockstore) PutMany(ctx context.Context, blocks []blocks.Block) error {
	if err := b.access(); err != nil {
		return err
	}
	defer b.viewers.Done()

	b.lockDB()
	defer b.unlockDB()

	// toReturn tracks the byte slices to return to the pool, if we're using key
	// prefixing. we can't return each slice to the pool after each Set, because
	// badger holds on to the slice.
	var toReturn [][]byte
	if b.prefixing {
		toReturn = make([][]byte, 0, len(blocks))
		defer func() {
			for _, b := range toReturn {
				KeyPool.Put(b)
			}
		}()
	}

	keys := make([][]byte, 0, len(blocks))
	values := make([][]byte, 0, len(blocks))
	for _, block := range blocks {
		k, pooled := b.PooledStorageKey(block.Cid())
		if pooled {
			toReturn = append(toReturn, k)
		}
		keys = append(keys, k)
		values = append(values, block.RawData())
	}

	req := Request{Action: "PutMany", Keys: keys, Values: values}
	response, err := b.query(req)
	if err != nil {
		return err
	}

	_ = response
	return nil
}

// DeleteBlock implements Blockstore.DeleteBlock.
func (b *Blockstore) DeleteBlock(ctx context.Context, cid cid.Cid) error {
	if err := b.access(); err != nil {
		return err
	}
	defer b.viewers.Done()

	b.lockDB()
	defer b.unlockDB()

	k, pooled := b.PooledStorageKey(cid)
	if pooled {
		defer KeyPool.Put(k)
	}

	req := Request{Action: "DeleteBlock", Key: k}
	response, err := b.query(req)
	if err != nil {
		return err
	}
	_ = response
	return nil
}

func (b *Blockstore) DeleteMany(ctx context.Context, cids []cid.Cid) error {
	if err := b.access(); err != nil {
		return err
	}
	defer b.viewers.Done()

	b.lockDB()
	defer b.unlockDB()

	// toReturn tracks the byte slices to return to the pool, if we're using key
	// prefixing. we can't return each slice to the pool after each Set, because
	// badger holds on to the slice.
	var toReturn [][]byte
	if b.prefixing {
		toReturn = make([][]byte, 0, len(cids))
		defer func() {
			for _, b := range toReturn {
				KeyPool.Put(b)
			}
		}()
	}

	keys := make([][]byte, 0, len(cids))

	for _, cid := range cids {
		k, pooled := b.PooledStorageKey(cid)
		if pooled {
			toReturn = append(toReturn, k)
		}
		keys = append(keys, k)
	}

	req := Request{Action: "DeleteBlock", Keys: keys}
	response, err := b.query(req)
	if err != nil {
		err = fmt.Errorf("failed to delete blocks from badger blockstore server: %w", err)
	}
	_ = response
	return nil
}

// AllKeysChan implements Blockstore.AllKeysChan.
func (b *Blockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	log.Warnf("called ForEachKey on badger blockstore client; function not currently supported; ignoring")
	return nil, nil
	/*

		if err := b.access(); err != nil {
			return nil, err
		}

		b.lockDB()
		defer b.unlockDB()

		txn := b.db.NewTransaction(false)
		opts := badger.IteratorOptions{PrefetchSize: 100}
		if b.prefixing {
			opts.Prefix = b.prefix
		}
		iter := txn.NewIterator(opts)

		ch := make(chan cid.Cid)
		go func() {
			defer b.viewers.Done()
			defer close(ch)
			defer iter.Close()

			// NewCidV1 makes a copy of the multihash buffer, so we can reuse it to
			// contain allocs.
			var buf []byte
			for iter.Rewind(); iter.Valid(); iter.Next() {
				if ctx.Err() != nil {
					return // context has fired.
				}
				if !b.isOpen() {
					// open iterators will run even after the database is closed...
					return // closing, yield.
				}
				k := iter.Item().Key()
				if b.prefixing {
					k = k[b.prefixLen:]
				}

				if reqlen := base32.RawStdEncoding.DecodedLen(len(k)); len(buf) < reqlen {
					buf = make([]byte, reqlen)
				}
				if n, err := base32.RawStdEncoding.Decode(buf, k); err == nil {
					select {
					case ch <- cid.NewCidV1(cid.Raw, buf[:n]):
					case <-ctx.Done():
						return
					}
				} else {
					log.Warnf("failed to decode key %s in badger AllKeysChan; err: %s", k, err)
				}
			}
		}()

		return ch, nil
	*/
}

// Implementation of BlockstoreIterator interface
func (b *Blockstore) ForEachKey(f func(cid.Cid) error) error {
	log.Warnf("called ForEachKey on badger blockstore; function not currently supported; ignoring")
	/*
		if err := b.access(); err != nil {
			return err
		}
		defer b.viewers.Done()

		b.lockDB()
		defer b.unlockDB()

		txn := b.db.NewTransaction(false)
		defer txn.Discard()

		opts := badger.IteratorOptions{PrefetchSize: 100}
		if b.prefixing {
			opts.Prefix = b.prefix
		}

		iter := txn.NewIterator(opts)
		defer iter.Close()

		var buf []byte
		for iter.Rewind(); iter.Valid(); iter.Next() {
			if !b.isOpen() {
				return ErrBlockstoreClosed
			}

			k := iter.Item().Key()
			if b.prefixing {
				k = k[b.prefixLen:]
			}

			klen := base32.RawStdEncoding.DecodedLen(len(k))
			if klen > len(buf) {
				buf = make([]byte, klen)
			}

			n, err := base32.RawStdEncoding.Decode(buf, k)
			if err != nil {
				return err
			}

			c := cid.NewCidV1(cid.Raw, buf[:n])

			err = f(c)
			if err != nil {
				return err
			}
		}

	*/
	return nil
}

// HashOnRead implements Blockstore.HashOnRead. It is not supported by this
// blockstore.
func (b *Blockstore) HashOnRead(_ bool) {
	log.Warnf("called HashOnRead on badger blockstore; function not supported; ignoring")
}

// PooledStorageKey returns the storage key under which this CID is stored.
//
// The key is: prefix + base32_no_padding(cid.Hash)
//
// This method may return pooled byte slice, which MUST be returned to the
// KeyPool if pooled=true, or a leak will occur.
func (b *Blockstore) PooledStorageKey(cid cid.Cid) (key []byte, pooled bool) {
	h := cid.Hash()
	size := base32.RawStdEncoding.EncodedLen(len(h))
	if !b.prefixing { // optimize for branch prediction.
		k := pool.Get(size)
		base32.RawStdEncoding.Encode(k, h)
		return k, true // slicing upto length unnecessary; the pool has already done this.
	}

	size += b.prefixLen
	k := pool.Get(size)
	copy(k, b.prefix)
	base32.RawStdEncoding.Encode(k[b.prefixLen:], h)
	return k, true // slicing upto length unnecessary; the pool has already done this.
}

// Storage acts like PooledStorageKey, but attempts to write the storage key
// into the provided slice. If the slice capacity is insufficient, it allocates
// a new byte slice with enough capacity to accommodate the result. This method
// returns the resulting slice.
func (b *Blockstore) StorageKey(dst []byte, cid cid.Cid) []byte {
	h := cid.Hash()
	reqsize := base32.RawStdEncoding.EncodedLen(len(h)) + b.prefixLen
	if reqsize > cap(dst) {
		// passed slice is smaller than required size; create new.
		dst = make([]byte, reqsize)
	} else if reqsize > len(dst) {
		// passed slice has enough capacity, but its length is
		// restricted, expand.
		dst = dst[:cap(dst)]
	}

	if b.prefixing { // optimize for branch prediction.
		copy(dst, b.prefix)
		base32.RawStdEncoding.Encode(dst[b.prefixLen:], h)
	} else {
		base32.RawStdEncoding.Encode(dst, h)
	}
	return dst[:reqsize]
}

// this method is added for lotus-shed needs
// WARNING: THIS IS COMPLETELY UNSAFE; DONT USE THIS IN PRODUCTION CODE
func (b *Blockstore) DB() *badger.DB {
	return nil
}
