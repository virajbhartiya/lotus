package gomapbs

import (
	"context"
	"fmt"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"

	"github.com/snissn/gomap"
)

var log = logging.Logger("gomapbs")

type GomapDatastore struct {
	gmap *gomap.HashmapDistributed
	lock sync.RWMutex
}

func NewGomapDS(folder string) (*GomapDatastore, error) {
	fmt.Println("NewGomapDS", folder)
	var h gomap.HashmapDistributed
	h.New(folder)
	gmds := GomapDatastore{gmap: &h}
	return &gmds, nil
}

func toKey(key cid.Cid) []byte {

	return key.Bytes() //todo zero copy?
}

func (gmds *GomapDatastore) Put(ctx context.Context, b blocks.Block) error {
	gmds.lock.Lock()
	defer gmds.lock.Unlock()
	gmds.gmap.Add(toKey(b.Cid()), b.RawData())
	//todo less copy and return err
	return nil
}

func (gmds *GomapDatastore) PutMany(ctx context.Context, blks []blocks.Block) error {
	var wg sync.WaitGroup
	//hold lock for datastore
	//internally gomap uses a lock for each shard and is safe
	gmds.lock.Lock()
	defer gmds.lock.Unlock()

	for _, block := range blks {
		// Increment the WaitGroup counter.
		wg.Add(1)

		// Start a new goroutine.
		go func(b blocks.Block) {
			defer wg.Done() // Decrement the counter when the goroutine completes.
			gmds.gmap.Add(toKey(b.Cid()), b.RawData())
		}(block)
	}

	// Wait for all goroutines to finish.
	wg.Wait()

	return nil
}

func (gmds *GomapDatastore) Delete(ctx context.Context, key cid.Cid) error {
	//not yet implemented
	return nil
}

func (gmds *GomapDatastore) Get(ctx context.Context, key cid.Cid) (blocks.Block, error) {
	gmds.lock.RLock()
	defer gmds.lock.RUnlock()
	val, err := gmds.gmap.Get(toKey(key))
	if val == nil || err != nil {
		//todo check error
		return nil, datastore.ErrNotFound
	}

	return blocks.NewBlockWithCid([]byte(val), key)

}

func (gmds *GomapDatastore) Has(ctx context.Context, key cid.Cid) (bool, error) {
	_, err := gmds.Get(ctx, key)
	return err != datastore.ErrNotFound, nil
	//todo check error type
}

// AllKeysChan implements Blockstore.AllKeysChan.
func (gmds *GomapDatastore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, nil
	//todo
}

func (gmds *GomapDatastore) DeleteBlock(ctx context.Context, cid cid.Cid) error {
	return nil
	//todo
}

func (gmds *GomapDatastore) GetSize(ctx context.Context, key cid.Cid) (int, error) {
	val, err := gmds.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	return int(len(val.RawData())), nil
}

func (gmds *GomapDatastore) HashOnRead(_ bool) {
	log.Warnf("called HashOnRead on badger blockstore; function not supported; ignoring")
}
