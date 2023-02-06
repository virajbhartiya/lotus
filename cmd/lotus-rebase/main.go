package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/migration"
	"github.com/filecoin-project/go-state-types/network"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/beacon"
	"github.com/filecoin-project/lotus/chain/beacon/drand"
	"github.com/filecoin-project/lotus/chain/consensus/filcns"
	"github.com/filecoin-project/lotus/chain/rand"
	"github.com/filecoin-project/lotus/chain/stmgr"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/vm"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/lotus/storage/sealer/ffiwrapper"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slices"
)

// FullAPI is a JSON-RPC client targeting a full node. It's initialized in a
// cli.BeforeFunc.
var FullAPI v0api.FullNode

// Closer is the closer for the JSON-RPC client, which must be called on
// cli.AfterFunc.
var Closer jsonrpc.ClientCloser

var log = logging.Logger("lotus-rebase")

const DefaultLotusRepoPath = "~/.lotus"

var repoFlag = cli.StringFlag{
	Name:      "repo",
	EnvVars:   []string{"LOTUS_PATH"},
	Value:     DefaultLotusRepoPath,
	TakesFile: true,
}

func main() {
	adjustLogLevels()

	app := &cli.App{
		Name:    "lotus-rebase",
		Usage:   "lotus-rebase is a tool for replaying chain history on top of an alternative network version and report differences",
		Version: build.UserVersion(),
		Flags: []cli.Flag{
			&repoFlag,
			&cli.StringFlag{
				Name:  "log-level",
				Value: "info",
			},
			&cli.StringFlag{
				Name:     "epoch",
				Usage:    "the epoch (1234), or range of epochs (1212..1234; both inclusive), to replay",
				Required: true,
			},
			&cli.Uint64Flag{
				Name:     "onto-nv",
				Usage:    "network version to rebase chain activity onto",
				Required: true,
			},
			&cli.BoolFlag{
				Name:     "run-migrations",
				Usage:    "whether to run the migrations to upgrade from the current to the target nv",
				Value:    true,
				Required: true,
			},
			&cli.StringFlag{
				Name:  "adapt-gas",
				Usage: "whether to ratchet the gas limit up if execution fails due to out-of-gas; values: no (default), up",
				Value: "no",
			},
		},
		Before: func(cctx *cli.Context) error {
			return logging.SetLogLevel("lotus-rebase", cctx.String("log-level"))
		},
		Action: run,
	}

	if err := app.Run(os.Args); err != nil {
		log.Errorf("%+v", err)
		os.Exit(1)
		return
	}
}

func adjustLogLevels() {
	_ = logging.SetLogLevel("*", "INFO")
	_ = logging.SetLogLevelRegex("badger*", "ERROR")
	_ = logging.SetLogLevel("drand", "ERROR")
	_ = logging.SetLogLevel("chainstore", "ERROR")
}

func run(c *cli.Context) error {
	const (
		AdaptGasNo = "no"
		AdaptGasUp = "up"
	)

	var (
		epochStart int
		epochEnd   int
		err        error

		ctx = c.Context
	)

	bs, mds, closeFn, err := openStores(c)
	if err != nil {
		return fmt.Errorf("failed to openStores: %w", err)
	}
	defer closeFn()

	// Parameter: epoch
	switch splt := strings.Split(c.String("epoch"), ".."); {
	case len(splt) == 1:
		if epochStart, err = strconv.Atoi(splt[0]); err != nil {
			return fmt.Errorf("failed to parse single epoch: %w", err)
		}
		epochEnd = epochStart
	case len(splt) == 2:
		if epochStart, err = strconv.Atoi(splt[0]); err != nil {
			return fmt.Errorf("failed to parse start epoch: %w", err)
		}
		if epochEnd, err = strconv.Atoi(splt[1]); err != nil {
			return fmt.Errorf("failed to parse end epoch: %w", err)
		}
		if epochEnd < epochStart {
			return fmt.Errorf("end epoch smaller (%d) than start epoch (%d)", epochEnd, epochStart)
		}
	default:
		return fmt.Errorf("invalid epoch format; expected single epoch (1212), or range (1212..1234); got: %s", c.String("epoch"))
	}

	cs := store.NewChainStore(bs, bs, mds, filcns.Weight, nil)
	sm, err := stmgr.NewStateManager(cs,
		filcns.NewTipSetExecutor(),
		vm.Syscalls(ffiwrapper.ProofVerifier),
		filcns.DefaultUpgradeSchedule(),
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to create state manager: %w", err)
	}

	var (
		nvStart = sm.GetNetworkVersion(ctx, abi.ChainEpoch(epochStart))
		nvEnd   = sm.GetNetworkVersion(ctx, abi.ChainEpoch(epochEnd))
		// Parameter: onto-nv
		nvTgt = network.Version(c.Uint64("onto-nv"))
	)

	if nvStart != nvEnd {
		return fmt.Errorf("network version at start epoch (%d) not equal to network version at end epoch (%d); this tool is not sophisticated enough yet to deal with such a case", nvStart, nvEnd)
	}

	idxCurr := slices.IndexFunc(filcns.DefaultUpgradeSchedule(), func(upgrade stmgr.Upgrade) bool {
		return upgrade.Network == nvStart
	})
	idxTgt := slices.IndexFunc(filcns.DefaultUpgradeSchedule(), func(upgrade stmgr.Upgrade) bool {
		return upgrade.Network == nvTgt
	})
	if idxCurr == -1 || idxTgt == -1 {
		return fmt.Errorf("some network versions don't have migrations associated to them")
	}
	upgradePath := filcns.DefaultUpgradeSchedule()[idxCurr+1 : idxTgt+1]

	var (
		tse                = filcns.NewTipSetExecutor()
		syscalls           = vm.Syscalls(ffiwrapper.ProofVerifier)
		nilUpgradeSchedule stmgr.UpgradeSchedule
	)

	drand, err := setupDrand(ctx, cs)
	if err != nil {
		return fmt.Errorf("failed to setup drand: %w", err)
	}

	for i := epochStart; i <= epochEnd; i++ {
		ts, err := cs.GetTipsetByHeight(ctx, abi.ChainEpoch(i), nil, false)
		if err != nil {
			return fmt.Errorf("failed to get tipset at epoch %d: %w", i, err)
		}

		overlayBs := blockstore.NewBuffered(bs)
		// TODO overlayMds; currently writes can go to the _actual_ metadata store.
		tmpCs := store.NewChainStore(overlayBs, overlayBs, mds, filcns.Weight, nil)

		tmpSm, err := stmgr.NewStateManager(tmpCs, tse, syscalls, nilUpgradeSchedule, drand)
		if err != nil {
			return fmt.Errorf("failed to setup state manager: %w", err)
		}

		migratedRoot := ts.ParentState()
		for _, u := range upgradePath {
			cache := migration.NewMemMigrationCache()
			migratedRoot, err = u.Migration(ctx, tmpSm, cache, nil, ts.ParentState(), ts.Height(), ts)
			if err != nil {
				return fmt.Errorf("failed to execute migration for nv %d: %w", u.Network, err)
			}
		}

		r := rand.NewStateRand(sm.ChainStore(), ts.Cids(), sm.Beacon(), sm.GetNetworkVersion)
		msgs, err := getBlockMessages(ctx, tmpSm, ts)
		if err != nil {
			return fmt.Errorf("failed to get block messages: %w", err)
		}
		baseFee := ts.Blocks()[0].ParentBaseFee

		// TODO deal with null rounds when taking the parent height.
		postStateRoot, postRecRoot, err := tse.ApplyBlocks(ctx, sm, ts.Height()-1, migratedRoot, msgs, ts.Height(), r, nil, false, baseFee, ts)
		if err != nil {
			return fmt.Errorf("failed to apply blocks: %w", err)
		}
		fmt.Println(postStateRoot, postRecRoot)
	}

	return nil
}

func openStores(c *cli.Context) (blockstore.Blockstore, datastore.Batching, func() error, error) {
	var (
		fsrepo *repo.FsRepo
		lkrepo repo.LockedRepo
		bs     blockstore.Blockstore
		err    error
	)

	if fsrepo, err = repo.NewFS(c.String("repo")); err != nil {
		return nil, nil, nil, err
	}

	if lkrepo, err = fsrepo.Lock(repo.FullNode); err != nil {
		return nil, nil, nil, err
	}

	if bs, err = lkrepo.Blockstore(c.Context, repo.UniversalBlockstore); err != nil {
		_ = lkrepo.Close()
		return nil, nil, nil, fmt.Errorf("failed to open blockstore: %w", err)
	}

	mds, err := lkrepo.Datastore(c.Context, "/metadata")
	if err != nil {
		_ = lkrepo.Close()
		return nil, nil, nil, err
	}

	return bs, mds, lkrepo.Close, nil
}

func setupDrand(ctx context.Context, cs *store.ChainStore) (beacon.Schedule, error) {
	gen, err := cs.GetGenesis(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get genesis: %w", err)
	}

	var shd beacon.Schedule
	for _, dc := range build.DrandConfigSchedule() {
		bc, err := drand.NewDrandBeacon(gen.Timestamp, build.BlockDelaySecs, nil, dc.Config)
		if err != nil {
			return nil, fmt.Errorf("creating drand beacon: %w", err)
		}
		shd = append(shd, beacon.BeaconPoint{Start: dc.Start, Beacon: bc})
	}
	return shd, nil
}

func getBlockMessages(ctx context.Context, sm *stmgr.StateManager, ts *types.TipSet) ([]filcns.FilecoinBlockMessages, error) {
	blkmsgs, err := sm.ChainStore().BlockMsgsForTipset(ctx, ts)
	if err != nil {
		return nil, fmt.Errorf("getting block messages for tipset: %w", err)
	}

	bms := make([]filcns.FilecoinBlockMessages, len(blkmsgs))
	for i := range bms {
		bms[i].BlockMessages = blkmsgs[i]
		bms[i].WinCount = ts.Blocks()[i].ElectionProof.WinCount
	}
	return bms, nil
}
