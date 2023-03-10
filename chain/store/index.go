package store

import (
	"context"
	"math"
	"math/bits"
	"math/rand"
	"os"
	"strconv"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/chain/types"
)

// 5000 => 80
var DefaultChainIndexCacheSize = 64

func init() {
	if s := os.Getenv("LOTUS_CHAIN_INDEX_CACHE"); s != "" {
		lcic, err := strconv.Atoi(s)
		if err != nil {
			log.Errorf("failed to parse 'LOTUS_CHAIN_INDEX_CACHE' env var: %s", err)
		}
		DefaultChainIndexCacheSize = lcic
	}

}

type ChainIndex struct {
	// The skipCache contains tipsets and epochs on their parents, as well as
	// skip entries on best effort basis
	skipCache rr2Cache[types.TipSetKey, *lbEntry]

	loadTipSet loadTipSetFunc
}

type loadTipSetFunc func(context.Context, types.TipSetKey) (*types.TipSet, error)

func NewChainIndex(lts loadTipSetFunc) *ChainIndex {
	//sc, _ := lru.NewARC[types.TipSetKey, *lbEntry](DefaultChainIndexCacheSize)
	sc := newRR2Cache(DefaultChainIndexCacheSize,
		func(_ types.TipSetKey, e1 *lbEntry, _ types.TipSetKey, e2 *lbEntry) float64 {
			diff := float64(len(e1.targets)) - float64(len(e2.targets))
			res := 1 / (1 + math.Exp(2.242*diff/1.4426))
			log.Errorf("evict %d, %d, %f", len(e1.targets), len(e2.targets), res)
			return res
		})
	return &ChainIndex{
		skipCache:  sc,
		loadTipSet: lts,
	}
}

type lbEntry struct {
	ts           *types.TipSet
	parentHeight abi.ChainEpoch
	targetHeight abi.ChainEpoch
	target       types.TipSetKey
	// target skip entries always point to at epochs CurrentEpoch() - 2**i
	targets []types.TipSetKey
}

func (ci ChainIndex) CacheHistogram() []int {
	res := make([]int, 64)
	for _, v := range ci.skipCache.cacheMap {
		res[len(v.targets)] += 1
	}
	last := 0
	for i, v := range res {
		if v != 0 {
			last = i
		}
	}

	return res[:last+1]
}

var LoadsFromBlockstore uint64 = 0

// load loads tsk from the cache, it uses passed tipset if tsk is not in cache
// If tsk is not in case it also loads parent tipset, fills out the parentHeight
// and returns it as second return
func (ci *ChainIndex) load(ctx context.Context, tsk types.TipSetKey, tipset *types.TipSet, peek bool) (*lbEntry, *types.TipSet, error) {
	entry, ok := ci.skipCache.Get(tsk)
	if ok {
		return entry, nil, nil
	}
	entry = &lbEntry{}

	// not found, lets build the basic entry
	if tipset != nil {
		entry.ts = tipset
	} else {
		var err error

		LoadsFromBlockstore++
		entry.ts, err = ci.loadTipSet(ctx, tsk)
		if err != nil {
			return nil, nil, xerrors.Errorf("loading tipset: %w", err)
		}
	}
	// fill out parent height, don't use `load` as it would recurse
	parentTsk := entry.ts.Parents()
	var parentTs *types.TipSet

	// Use Peek to not bump the parent if it is in recent cache
	if parent, ok := ci.skipCache.Get(parentTsk); ok {
		entry.parentHeight = parent.ts.Height()
	} else if entry.ts.Height() == 0 {
		// don't try to lookup epoch -1
		entry.parentHeight = -1
	} else {
		// load parent from chainstore
		var err error
		LoadsFromBlockstore++
		parentTs, err = ci.loadTipSet(ctx, parentTsk)
		if err != nil {
			return nil, nil, xerrors.Errorf("loading parent tipset for epoch %d: %w", entry.ts.Height(), err)
		}
		entry.parentHeight = parentTs.Height()
	}
	return entry, parentTs, nil
}

// GetTipsetByHeight returns a tipet in link chain starting at from with a hight no lower than
// the to epoch.
func (ci *ChainIndex) GetTipsetByHeight(ctx context.Context, from *types.TipSet, to abi.ChainEpoch) (*types.TipSet, error) {
	if from.Height() == to {
		return from, nil
	}
	if to > from.Height() {
		return nil, xerrors.Errorf("looking for tipset with height greater than start point")
	}

	peek := true
	cur := from.Key()
	curTs := from // optinally available

	updateInfos := make([]*lbEntry, bits.Len64(uint64(curTs.Height()-to))+1)

	for {
		curEntry, parentTs, err := ci.load(ctx, cur, curTs, peek)
		peek = true
		if err != nil {
			return nil, xerrors.Errorf("loading current tipset: %w", err)
		}
		curTs = curEntry.ts
		// not curTs available
		log.Errorf("looking for: %d from: %d at: %d", to, from.Height(), curTs.Height())

		// TODO expectedOrder should take into account null blocks
		expectedOrder := bits.TrailingZeros64(uint64(curTs.Height()))

		for i := 1; i < expectedOrder+1 && i-1 < len(updateInfos); i++ {
			entryToUpdate := updateInfos[i-1]
			updateInfos[i-1] = nil

			if entryToUpdate != nil {
				if i-1 >= len(entryToUpdate.targets) {
					newTargets := make([]types.TipSetKey, i)
					copy(newTargets, entryToUpdate.targets)
					entryToUpdate.targets = newTargets
				}
				entryToUpdate.targets[i-1] = cur
				ci.skipCache.Add(entryToUpdate.ts.Key(), entryToUpdate)
			}
		}
		for i := 1; i < expectedOrder+1; i++ {
			if curTs.Height() == 96 {
			}
			if i-1 < len(updateInfos) && (i-1 >= len(curEntry.targets) ||
				curEntry.targets[i-1] == types.EmptyTSK) {
				if updateInfos[i-1] != nil {
					log.Errorf("this should be empty")
				}
				updateInfos[i-1] = curEntry
			}
		}

		// parent is is beyond where we want to get to
		if curEntry.parentHeight < to {
			return curEntry.ts, nil
		}

		dist := curTs.Height() - to
		maxJumpOrder := bits.Len64(uint64(dist)) - 1 // Log2Floor
		// in case of null epochs, parent tipset can provide futher jump
		// than the maximum jump we can take
		if 1<<maxJumpOrder <= curTs.Height()-curEntry.parentHeight {
			// use parent
			cur = curTs.Parents()
			curTs = parentTs
			continue
		}
		availableOrder := len(curEntry.targets)

		jumpOrder := availableOrder
		if maxJumpOrder < availableOrder {
			jumpOrder = maxJumpOrder
		}

		if jumpOrder == 0 {
			// use parent
			cur = curTs.Parents()
			curTs = parentTs
		} else {
			cur = curTs.Parents()
			curTs = nil
		targetSelect:
			for i := jumpOrder - 1; i >= 0; i-- {
				next := curEntry.targets[jumpOrder-1]
				if next != types.EmptyTSK {
					peek = false
					cur = next
					break targetSelect
				}
			}
		}

	}
}

func (ci *ChainIndex) GetTipsetByHeightWithoutCache(ctx context.Context, from *types.TipSet, to abi.ChainEpoch) (*types.TipSet, error) {
	return ci.walkBack(ctx, from, to)
}

func (ci *ChainIndex) walkBack(ctx context.Context, from *types.TipSet, to abi.ChainEpoch) (*types.TipSet, error) {
	if to > from.Height() {
		return nil, xerrors.Errorf("looking for tipset with height greater than start point")
	}

	if to == from.Height() {
		return from, nil
	}

	ts := from

	for {
		pts, err := ci.loadTipSet(ctx, ts.Parents())
		if err != nil {
			return nil, err
		}

		if to > pts.Height() {
			// in case pts is lower than the epoch we're looking for (null blocks)
			// return a tipset above that height
			return ts, nil
		}
		if to == pts.Height() {
			return pts, nil
		}

		ts = pts
	}
}

type rr2Cache[K comparable, V any] struct {
	cacheMap   map[K]V
	targetSize int
	rng        *rand.Rand

	policyFunction func(K, V, K, V) float64
}

// newRR2Cache creates a new Random Replacment 2 cache with a polciy function,
// the policy function return defines the probability for first KV pair to be evicted from the cache
// if the first entry from policy function is not evicted, the second entry will be
func newRR2Cache[K comparable, V any](targetSize int, policyFunction func(K, V, K, V) float64) rr2Cache[K, V] {
	return rr2Cache[K, V]{
		targetSize:     targetSize,
		cacheMap:       make(map[K]V),
		rng:            rand.New(rand.NewSource(time.Now().UnixNano())),
		policyFunction: policyFunction,
	}
}

func (rr *rr2Cache[K, V]) Add(key K, value V) {
	rr.cacheMap[key] = value
	if len(rr.cacheMap) > rr.targetSize {
		rr.EvictOne(key, value)
	}
}

func (rr *rr2Cache[K, V]) Get(key K) (V, bool) {
	v, ok := rr.cacheMap[key]
	return v, ok
}

func (rr *rr2Cache[K, V]) EvictOne(key K, value V) {
	// use Golang's random map acess
	var k1, k2 K
	var v1, v2 V
	k1, v1 = key, value
	for k, v := range rr.cacheMap {
		k2 = k
		v2 = v
		break
	}

	var ek1 K
	var ev1 V

	if rr.rng.Float64() < rr.policyFunction(k1, v1, k2, v2) {
		ek1 = k1
		ev1 = v1
	} else {
		ek1 = k2
		ev1 = v2
	}

	for k, v := range rr.cacheMap {
		k1 = k
		v1 = v
		break
	}
	for k, v := range rr.cacheMap {
		k2 = k
		v2 = v
		break
	}

	var ek2 K
	var ev2 V
	if rr.rng.Float64() < rr.policyFunction(k1, v1, k2, v2) {
		ek2 = k1
		ev2 = v1
	} else {
		ek2 = k2
		ev2 = v2
	}
	if rr.rng.Float64() < rr.policyFunction(ek1, ev1, ek2, ev2) {
		delete(rr.cacheMap, ek1)
	} else {
		delete(rr.cacheMap, ek2)
	}

}
