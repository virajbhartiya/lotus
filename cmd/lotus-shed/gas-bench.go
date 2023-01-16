package main

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"

	"github.com/filecoin-project/go-address"
	actorstypes "github.com/filecoin-project/go-state-types/actors"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/state"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
	cliutil "github.com/filecoin-project/lotus/cli/util"
	"github.com/filecoin-project/lotus/node/repo"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var gasBenchCmd = &cli.Command{
	Name:        "gas-bench",
	Subcommands: []*cli.Command{benchStateUpdateCmd},
}

var benchStateUpdateCmd = &cli.Command{
	Name:      "state-update",
	ArgsUsage: "[state root]",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "repo",
			Value: "~/.lotus",
		},
		&cli.Uint64Flag{
			Name:  "max-id",
			Value: 1992000,
		},
		&cli.Uint64Flag{
			Name:  "scan-start",
			Value: 100,
		},
		&cli.Uint64Flag{
			Name:  "scan-end",
			Value: 40000,
		},
		&cli.Uint64Flag{
			Name:  "scan-steps",
			Value: 20,
		},
		&cli.IntFlag{
			Name:  "samples",
			Value: 1,
		},
		&cli.BoolFlag{
			Name:  "header",
			Value: true,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := cliutil.ReqContext(cctx)

		if cctx.NArg() != 1 {
			return lcli.IncorrectNumArgs(cctx)
		}

		stateCid, err := cid.Decode(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("failed to parse input: %w", err)
		}

		maxId := cctx.Uint64("max-id")
		fsrepo, err := repo.NewFS(cctx.String("repo"))
		if err != nil {
			return err
		}

		lkrepo, err := fsrepo.Lock(repo.FullNode)
		if err != nil {
			return err
		}

		defer lkrepo.Close() //nolint:errcheck

		bsOrg, err := lkrepo.Blockstore(ctx, repo.UniversalBlockstore)
		if err != nil {
			return fmt.Errorf("failed to open blockstore: %w", err)
		}

		defer func() {
			if c, ok := bsOrg.(io.Closer); ok {
				if err := c.Close(); err != nil {
					log.Warnf("failed to close blockstore: %s", err)
				}
			}
		}()

		bs := &countingBlockstore{Blockstore: bsOrg}
		cst := cbor.NewCborStore(bs)
		codeCid, ok := actors.GetActorCodeID(actorstypes.Version8, "account")
		if !ok {
			return xerrors.Errorf("getting code cid")
		}
		startExp := math.Log10(float64(cctx.Uint64("scan-start")))
		endExp := math.Log10(float64(cctx.Uint64("scan-end")))
		steps := float64(cctx.Uint64("scan-steps"))

		if cctx.Bool("header") {
			fmt.Println("BatchSize, GetsNo, PutsNo, PutsBytes")
		}

		for e := startExp; e < endExp; e += (endExp - startExp) / steps {
			n := int(math.Pow(10, e))
			for i := 0; i < cctx.Int("samples"); i++ {
				stateTree, err := state.LoadStateTree(cst, stateCid)
				if err != nil {
					return xerrors.Errorf("loading state tree")
				}
				for j := 0; j < n; j++ {
					addr, err := address.NewIDAddress((rand.Uint64() % maxId) + 1) // not perfect distribution but good enough
					if err != nil {
						return xerrors.Errorf("creating ID address %d: %w", maxId, err)
					}
					err = stateTree.SetActor(addr, &types.Actor{Code: codeCid, Head: codeCid, Nonce: 1 << 62}) // nonce that high is guarnateed to not exist
					if err != nil {
						log.Warnf("error while setting actor: %+v", err)
					}
				}
				_, err = stateTree.Flush(ctx)
				if err != nil {
					log.Warnf("error while flushing actor: %+v", err)
				}
				fmt.Printf("%d, %d, %d, %d\n", n, bs.getsNo, bs.putsNo, bs.putsBytes)
			}
		}

		return nil
	},
}

type countingBlockstore struct {
	blockstore.Blockstore

	getsNo    uint64
	putsNo    uint64
	putsBytes uint64

	putsBatchNo     uint64
	putsBatchBlocks uint64
	putsBatchBytes  uint64
}

func (b *countingBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	b.getsNo++
	return b.Blockstore.Get(ctx, c)
}
func (b *countingBlockstore) View(ctx context.Context, c cid.Cid, callback func([]byte) error) error {
	b.getsNo++
	return b.Blockstore.View(ctx, c, callback)
}

func (b *countingBlockstore) Put(ctx context.Context, blk blocks.Block) error {
	b.putsNo++
	b.putsBytes += uint64(len(blk.RawData()))
	return b.Blockstore.Put(ctx, blk)
}

func (b *countingBlockstore) PutMany(ctx context.Context, blks []blocks.Block) error {
	b.putsBatchNo++
	b.putsBatchBlocks += uint64(len(blks))
	for _, blk := range blks {
		b.putsBytes += uint64(len(blk.RawData()))
	}
	return b.Blockstore.PutMany(ctx, blks)
}
