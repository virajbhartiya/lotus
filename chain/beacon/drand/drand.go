package drand

import (
	"bytes"
	"context"
	"time"

	dchain "github.com/drand/drand/chain"
	dclient "github.com/drand/drand/client"
	hclient "github.com/drand/drand/client/http"
	dcrypto "github.com/drand/drand/crypto"
	dlog "github.com/drand/drand/log"
	gclient "github.com/drand/drand/lp2p/client"
	"github.com/drand/kyber"
	lru "github.com/hashicorp/golang-lru/v2"
	logging "github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/network"

	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/beacon"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
)

var log = logging.Logger("drand")

// DrandBeacon connects Lotus with a drand network in order to provide
// randomness to the system in a way that's aligned with Filecoin rounds/epochs.
//
// We connect to drand peers via their public HTTP endpoints. The peers are
// enumerated in the drandServers variable.
//
// The root trust for the Drand chain is configured from build.DrandChain.
type DrandBeacon struct {
	client dclient.Client

	pubkey kyber.Point

	// seconds
	interval time.Duration

	drandGenTime uint64
	filGenTime   uint64
	filRoundTime uint64
	scheme       *dcrypto.Scheme

	localCache *lru.Cache[uint64, *types.BeaconEntry]
}

// DrandHTTPClient interface overrides the user agent used by drand
type DrandHTTPClient interface {
	SetUserAgent(string)
}

type logger struct {
	*zap.SugaredLogger
}

func (l *logger) With(args ...interface{}) dlog.Logger {
	return &logger{l.SugaredLogger.With(args...)}
}

func (l *logger) Named(s string) dlog.Logger {
	return &logger{l.SugaredLogger.Named(s)}
}

func (l *logger) AddCallerSkip(skip int) dlog.Logger {
	return l
}
func NewDrandBeacon(genesisTs, interval uint64, ps *pubsub.PubSub, config dtypes.DrandConfig) (*DrandBeacon, error) {
	if genesisTs == 0 {
		panic("what are you doing this cant be zero")
	}

	drandChain, err := dchain.InfoFromJSON(bytes.NewReader([]byte(config.ChainInfoJSON)))
	if err != nil {
		return nil, xerrors.Errorf("unable to unmarshal drand chain info: %w", err)
	}

	var clients []dclient.Client
	for _, url := range config.Servers {
		hc, err := hclient.NewWithInfo(url, drandChain, nil)
		if err != nil {
			return nil, xerrors.Errorf("could not create http drand client: %w", err)
		}
		hc.(DrandHTTPClient).SetUserAgent("drand-client-lotus/" + build.BuildVersion)
		clients = append(clients, hc)

	}

	opts := []dclient.Option{
		dclient.WithChainInfo(drandChain),
		dclient.WithCacheSize(1024),
		dclient.WithLogger(&logger{&log.SugaredLogger}),
	}

	if ps != nil {
		opts = append(opts, gclient.WithPubsub(ps))
	} else {
		log.Info("drand beacon without pubsub")
	}

	client, err := dclient.Wrap(clients, opts...)
	if err != nil {
		return nil, xerrors.Errorf("creating drand client: %w", err)
	}

	lc, err := lru.New[uint64, *types.BeaconEntry](1024)
	if err != nil {
		return nil, err
	}

	db := &DrandBeacon{
		client:     client,
		localCache: lc,
	}

	sch, err := dcrypto.GetSchemeByIDWithDefault(drandChain.Scheme)
	if err != nil {
		return nil, err
	}
	db.scheme = sch
	db.pubkey = drandChain.PublicKey
	db.interval = drandChain.Period
	db.drandGenTime = uint64(drandChain.GenesisTime)
	db.filRoundTime = interval
	db.filGenTime = genesisTs

	return db, err
}

func (d *DrandBeacon) Entry(ctx context.Context, round uint64) <-chan beacon.Response {
	out := make(chan beacon.Response, 1)
	if round != 0 {
		be := d.getCachedValue(round)
		if be != nil {
			out <- beacon.Response{Entry: *be}
			close(out)
			return out
		}
	}

	go func() {
		start := build.Clock.Now()
		log.Debugw("start fetching randomness", "round", round)
		resp, err := d.client.Get(ctx, round)

		var br beacon.Response
		if err != nil {
			br.Err = xerrors.Errorf("drand failed Get request: %w", err)
		} else {
			br.Entry.Round = resp.Round()
			br.Entry.Data = resp.Signature()
		}
		log.Debugw("done fetching randomness", "round", round, "took", build.Clock.Since(start))
		out <- br
		close(out)
	}()

	return out
}
func (d *DrandBeacon) cacheValue(e types.BeaconEntry) {
	d.localCache.Add(e.Round, &e)
}

func (d *DrandBeacon) getCachedValue(round uint64) *types.BeaconEntry {
	v, _ := d.localCache.Get(round)
	return v
}

func (d *DrandBeacon) VerifyEntry(curr types.BeaconEntry, prev types.BeaconEntry) error {
	if prev.Round == 0 {
		// TODO handle genesis better
		return nil
	}

	if curr.Round != d.NextRound(prev) {
		return xerrors.Errorf("invalid beacon entry: cur (%d) != prev (%d) + 1", curr.Round, prev.Round)
	}

	if be := d.getCachedValue(curr.Round); be != nil {
		if !bytes.Equal(curr.Data, be.Data) {
			return xerrors.New("invalid beacon value, does not match cached good value")
		}
		// return no error if the value is in the cache already
		return nil
	}
	b := &dchain.Beacon{
		PreviousSig: prev.Data,
		Round:       curr.Round,
		Signature:   curr.Data,
	}

	err := d.scheme.VerifyBeacon(b, d.pubkey)
	if err == nil {
		d.cacheValue(curr)
	}
	return err
}

func (d *DrandBeacon) MaxBeaconRoundForEpoch(nv network.Version, filEpoch abi.ChainEpoch) uint64 {
	// TODO: sometimes the genesis time for filecoin is zero and this goes negative
	latestTs := ((uint64(filEpoch) * d.filRoundTime) + d.filGenTime) - d.filRoundTime

	if nv <= network.Version15 {
		return d.maxBeaconRoundV1(latestTs)
	}
	// FUCK this should really be version20, but 21 isn't in the dependent repo yet
	if nv <= network.Version19 {
		return d.maxBeaconRoundV2(latestTs)
	}

	return d.maxBeaconRoundV2(latestTs)
}

func (d *DrandBeacon) maxBeaconRoundV1(latestTs uint64) uint64 {
	dround := (latestTs - d.drandGenTime) / uint64(d.interval.Seconds())
	return dround
}

func (d *DrandBeacon) maxBeaconRoundV2(latestTs uint64) uint64 {
	if latestTs < d.drandGenTime {
		return 1
	}

	fromGenesis := latestTs - d.drandGenTime
	// we take the time from genesis divided by the periods in seconds, that
	// gives us the number of periods since genesis.  We also add +1 because
	// round 1 starts at genesis time.
	return fromGenesis/uint64(d.interval.Seconds()) + 1
}

var _ beacon.RandomBeacon = (*DrandBeacon)(nil)

func BeaconScheduleFromDrandSchedule(dcs dtypes.DrandSchedule, genesisTime uint64, ps *pubsub.PubSub) (beacon.Schedule, error) {
	shd := beacon.Schedule{}
	for _, dc := range dcs {
		bc, err := NewDrandBeacon(genesisTime, build.BlockDelaySecs, ps, dc.Config)
		if err != nil {
			return nil, xerrors.Errorf("creating drand beacon: %w", err)
		}
		shd = append(shd, beacon.BeaconPoint{Start: dc.Start, Beacon: bc})
	}

	return shd, nil
}

func (d *DrandBeacon) PrevRound(current types.BeaconEntry) uint64 {
	drandRoundsPerFilecoinRound := d.filRoundTime / uint64(d.interval.Seconds())
	if current.Round <= drandRoundsPerFilecoinRound {
		return 1
	}
	return current.Round - drandRoundsPerFilecoinRound
}
func (d *DrandBeacon) NextRound(current types.BeaconEntry) uint64 {
	drandRoundsPerFilecoinRound := d.filRoundTime / uint64(d.interval.Seconds())
	return current.Round + drandRoundsPerFilecoinRound
}
