package main

import (
	"context"
	"fmt"
	"io"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/blockstore"
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
			Value: 1700000,
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

		bs, err := lkrepo.Blockstore(ctx, repo.UniversalBlockstore)
		bs = &countingBlockstore{Blockstore: bs}
		if err != nil {
			return fmt.Errorf("failed to open blockstore: %w", err)
		}

		defer func() {
			if c, ok := bs.(io.Closer); ok {
				if err := c.Close(); err != nil {
					log.Warnf("failed to close blockstore: %s", err)
				}
			}
		}()
		//mds, err := lkrepo.Datastore(context.Background(), "/metadata")
		//if err != nil {
		//return err
		//}

		cst := cbor.NewCborStore(bs)
		stateTree, err := state.LoadStateTree(cst, stateCid)
		addr, err := address.NewIDAddress(maxId)
		if err != nil {
			return xerrors.Errorf("creating ID address %d: %w", maxId, err)
		}

		err = stateTree.SetActor(addr, &types.Actor{Nonce: 1 << 62}) // nonce that high is guarnateed to not exist
		if err != nil {
			log.Warnf("error while setting actor: %+v", err)
		}
		_, err = stateTree.Flush(ctx)
		if err != nil {
			log.Warnf("error while flushing actor: %+v", err)
		}
		fmt.Println("%+v\n", bs)

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
func (b *countingBlockstore) View(ctx context.Context, cid cid.Cid, callback func([]byte) error) error {
	b.getsNo++
	return b.Blockstore.View(ctx, cid, callback)
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
