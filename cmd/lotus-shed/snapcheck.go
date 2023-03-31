package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/chain/actors/builtin/market"

	"github.com/filecoin-project/lotus/chain/actors/adt"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/big"

	badgerbs "github.com/filecoin-project/lotus/blockstore/badger"
	"github.com/filecoin-project/lotus/chain/actors/builtin"
	"github.com/filecoin-project/lotus/chain/state"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/node/repo"
)

var SnapAnalyticsCmd = &cli.Command{
	Name:  "snap-analytics",
	Usage: "Get snap related metrics",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "repo",
			Value: "~/.lotus",
		},
	},
	Subcommands: []*cli.Command{
		snapCmd,
	},
}

var snapCmd = &cli.Command{
	Name:      "snap",
	Usage:     "get snap data",
	ArgsUsage: "[state root]",

	Action: func(cctx *cli.Context) error {
		if cctx.NArg() != 1 {
			return xerrors.New("only needs state root")
		}

		if !cctx.Args().Present() {
			return fmt.Errorf("must pass state root")
		}

		sroot, err := cid.Decode(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("failed to parse input: %w", err)
		}

		fsrepo, err := repo.NewFS(cctx.String("repo"))
		if err != nil {
			return err
		}

		lkrepo, err := fsrepo.Lock(repo.FullNode)
		if err != nil {
			return err
		}

		defer lkrepo.Close() //nolint:errcheck

		path, err := lkrepo.SplitstorePath()
		if err != nil {
			return err
		}

		path = filepath.Join(path, "hot.badger")
		if err := os.MkdirAll(path, 0755); err != nil {
			return err
		}

		opts, err := repo.BadgerBlockstoreOptions(repo.HotBlockstore, path, lkrepo.Readonly())
		if err != nil {
			return err
		}

		bs, err := badgerbs.Open(opts)
		if err != nil {
			return err
		}

		ctx := context.TODO()
		cst := cbor.NewCborStore(bs)
		store := adt.WrapStore(ctx, cst)

		st, err := state.LoadStateTree(cst, sroot)
		if err != nil {
			return err
		}

		count := 0
		spCount := 0
		dealSPCount := 0
		snapSPCount := 0
		snapDealCount := 0
		snapDeals := make(map[abi.DealID]bool)

		// market
		ma, err := st.GetActor(market.Address)
		if err != nil {
			return xerrors.Errorf("fail to get market actor: %w\n", err)
		}

		ms, err := market.Load(store, ma)
		if err != nil {
			return xerrors.Errorf("fail to load market state: %w\n", err)
		}

		dealProposals, err := ms.Proposals()
		if err != nil {
			return err
		}

		fmt.Println("iterating over all actors")
		err = st.ForEach(func(addr address.Address, act *types.Actor) error {
			if count%200000 == 0 {
				fmt.Println("processed /n", count)
			}
			count++
			fmt.Println(">>>> ", act.Address)

			if builtin.IsStorageMinerActor(act.Code) {
				spCount++
				fmt.Println(">>>> ", act.Address)

				spa, err := miner.Load(store, act)
				if err != nil {
					return xerrors.Errorf("fail to load miner actor: %w", err)
				}

				soci, err := spa.LoadSectors(nil)
				if err != nil {
					return xerrors.Errorf("fail to load miner actor: %w", err)
				}

				isDealSP, isSnapSP := false, false
				for _, sector := range soci {
					if len(sector.DealIDs) != 0 {
						isDealSP = true
					}

					if sector.SectorKeyCID != nil {
						isSnapSP = true
						for d := range sector.DealIDs {
							snapDeals[abi.DealID(d)] = true
						}
						snapDealCount += len(sector.DealIDs)
					}
				}

				if isDealSP {
					dealSPCount++
				}
				if isSnapSP {
					snapSPCount++
				}
			}

			return nil
		})
		if err != nil {
			return err
		}

		dealSize := abi.NewStoragePower(0)
		snapDealSize := abi.NewStoragePower(0)
		if err := dealProposals.ForEach(func(dealID abi.DealID, d market.DealProposal) error {
			dealSize = big.Add(dealSize, big.NewInt(int64(d.PieceSize)))
			if _, found := snapDeals[dealID]; found {
				snapDealSize = big.Add(snapDealSize, big.NewInt(int64(d.PieceSize)))
			}
			return nil
		}); err != nil {
			return xerrors.Errorf("fail to get deals")
		}

		dealCount, _ := ms.NextID()
		fmt.Println("Total # of SP: ", spCount)
		fmt.Println("Total # of deal SP: ", dealSPCount)
		fmt.Println("Total # of SP that has snapped: ", snapSPCount)
		fmt.Println("# of deal SP / # of SP: ", float32(dealSPCount)/float32(spCount))
		fmt.Println("# of snap SP / # of deal SP: ", float32(snapSPCount)/float32(dealSPCount))
		fmt.Println("# of Snap SP / # of SP : ", float32(snapSPCount)/float32(spCount))
		fmt.Println("Total deal #: ", dealCount)
		fmt.Println("Total snap deal #: ", snapDealCount)
		fmt.Println("snap % ", float32(snapDealCount)/float32(int(dealCount)))
		fmt.Println("Total deal RB: ", dealSize)
		fmt.Println("Total snap deal RB: ", snapDealSize)
		return nil
	},
}
