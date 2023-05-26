package main

import (
	"fmt"
	"os"
	"path"

	"github.com/fatih/color"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	lcli "github.com/filecoin-project/lotus/cli"
)

var backfillSqlCmd = &cli.Command{
	Name:        "backfill-sql",
	Description: "Walk up the chain, recomputing state, and populate sqlite",
	Flags: []cli.Flag{
		&cli.UintFlag{
			Name:  "from",
			Value: 0,
			Usage: "the tipset height to start backfilling from (0 is head of chain)",
		},
		&cli.IntFlag{
			Name:  "epochs",
			Value: 2000,
			Usage: "the number of epochs to backfill",
		},
		&cli.BoolFlag{
			Name:  "skip-events",
			Value: false,
			Usage: "whether to skip backfilling actor events (sqlite/events.db)",
		},
		&cli.BoolFlag{
			Name:  "skip-msgindex",
			Value: false,
			Usage: "whether to skip backfilling msgindex (sqlite/msgindex.db)",
		},
		&cli.BoolFlag{
			Name:  "skip-txhash",
			Value: false,
			Usage: "whether to skip backfilling txhashes (sqlite/txhash.db)",
		},
		&cli.BoolFlag{
			Name:  "dry-run",
			Value: false,
			Usage: "runs this script in read only mode (no side effects)",
		},
		&cli.BoolFlag{
			Name:  "compute-state",
			Value: false,
			Usage: "whether to compute state for messages in tipset when state was missing (only applies to actor events)",
		},
		&cli.BoolFlag{
			Name:  "analyze",
			Value: false,
			Usage: "analyze current state of sqlite databases and exits",
		},
		&cli.StringFlag{
			Name:  "repo",
			Value: "~/.lotus-local-net",
			Usage: "path to the repo",
		},
	},
	Action: func(cctx *cli.Context) error {
		return backfill(cctx)
	},
}

func performSqliteAnalysis(cctx *cli.Context, api api.FullNode) error {
	color.Green("Walking up chain to know where state ends (can take a few min)...")

	checkTs, err := api.ChainHead(cctx.Context)
	if err != nil {
		return err
	}

	// walk up the chain to find the chain length
	//
	nrTipsetts := 0
	for {
		select {
		case <-cctx.Done():
			fmt.Println("request cancelled")
			return nil
		default:
		}

		nrTipsetts++

		execTsk := checkTs.Parents()
		execTs, err := api.ChainGetTipSet(cctx.Context, execTsk)
		if err != nil {
			break
		}

		_, err = api.ChainGetMessagesInTipset(cctx.Context, execTsk)
		if err != nil {
			break
		}

		checkTs = execTs
	}
	color.Green("\tHave state for %d tipsets", nrTipsetts)

	basePath, err := homedir.Expand(cctx.String("repo"))
	if err != nil {
		return err
	}

	color.Green("Check missing database if sqlite databases..")
	for _, db := range []string{"events.db", "msgindex.db", "txhash.db"} {
		path := path.Join(basePath, "sqlite", db)
		_, err := os.Stat(path)
		if os.IsNotExist(err) {
			color.Red("\t%s missing", path)
			continue
		}
		color.Green("\t%s found", path)
	}

	color.Green("Check missing database if sqlite databases..")

	return nil
}

func backfill(cctx *cli.Context) error {
	srv, err := lcli.GetFullNodeServices(cctx)
	if err != nil {
		return err
	}
	defer srv.Close() //nolint:errcheck

	api := srv.FullNodeAPI()
	ctx := lcli.ReqContext(cctx)

	if cctx.IsSet("analyze") {
		return performSqliteAnalysis(cctx, api)
	}

	checkTs, err := api.ChainHead(ctx)
	if err != nil {
		return err
	}
	if cctx.IsSet("from") {
		from := cctx.Uint("from")
		checkTs, err = api.ChainGetTipSetByHeight(ctx, abi.ChainEpoch(from), types.EmptyTSK)
		if err != nil {
			return err
		}
	}
	epochs := cctx.Int("epochs")

	//startHeight := checkTs.Height()
	for i := 0; i < epochs; i++ {
		//epoch := abi.ChainEpoch(startHeight - int64(i))

		select {
		case <-cctx.Done():
			fmt.Println("request cancelled")
			return nil
		default:
		}

		execTsk := checkTs.Parents()
		execTs, err := api.ChainGetTipSet(ctx, execTsk)
		if err != nil {
			return err
		}

		msgs, err := api.ChainGetMessagesInTipset(cctx.Context, execTsk)
		if err != nil {
			log.Infof("could not get messages in tipset: %s\n", err)
			return err
		}

		if !cctx.Bool("skip-msgindex") {
			fmt.Printf("%d %d: %d messages\n", i, execTs.Height(), len(msgs))
		}

		if !cctx.Bool("skip-txhash") {
			for _, blk := range execTs.Blocks() {
				blkMsgs, err := api.ChainGetBlockMessages(ctx, blk.Cid())
				if err != nil {
					return err
				}

				for _, smsg := range blkMsgs.SecpkMessages {
					if smsg.Signature.Type != crypto.SigTypeDelegated {
						continue
					}

					tx, err := ethtypes.EthTxFromSignedEthMessage(smsg)
					if err != nil {
						return fmt.Errorf("failed to convert from signed message: %w", err)
					}

					tx.Hash, err = tx.TxHash()
					if err != nil {
						return fmt.Errorf("failed to calculate hash for ethTx: %w", err)
					}

					log.Infof("INSERT INTO eth_tx_hashes(hash, cid) VALUES (%s, %s)\n", tx.Hash.String(), smsg.Cid())
				}
			}
		}

		if !cctx.Bool("skip-events") {
			////
			/////
			var ptrMsgs []*types.Message
			for _, sm := range msgs {
				ptrMsgs = append(ptrMsgs, sm.Message)
			}
			st, err := api.StateCompute(ctx, execTs.Height(), ptrMsgs, execTsk)
			if err != nil {
				return err
			}

			if st.Root != checkTs.ParentState() {
				fmt.Println("consensus mismatch found at height ", execTs.Height())
			}

			// loop over each events root
			for _, trace := range st.Trace {
				if trace.MsgRct.EventsRoot != nil {
					events, err := api.ChainGetEvents(ctx, *trace.MsgRct.EventsRoot)
					if err != nil {
						fmt.Printf("err4: %v\n", err)
						continue
					}

					for _, event := range events {
						for _, entry := range event.Entries {
							fmt.Printf("<<<<< %d %d: emitter:%d entry:%s\n", i, execTs.Height(), event.Emitter, entry.Key)
						}
					}
					fmt.Println()
				}
			}
			fmt.Println()

			// get the events for all messages
			//
			for _, msg := range msgs {
				ml, err := api.StateSearchMsg(ctx, checkTs.Key(), msg.Cid, 0, false)
				if err != nil {
					fmt.Printf("err1: %v\n", err)

				}
				if ml == nil {
					fmt.Println("nil message")
					continue
				}
				if ml.Receipt.ExitCode != exitcode.Ok {
					fmt.Println("err2: ExitCode != exitcode.Ok")
					continue
				}

				if ml.Receipt.EventsRoot != nil {
					events, err := api.ChainGetEvents(ctx, *ml.Receipt.EventsRoot)
					if err != nil {
						fmt.Printf("err3: %v\n", err)
						continue
					}

					for _, event := range events {
						for _, entry := range event.Entries {
							fmt.Printf("%d %d: emitter:%d entry:%s\n", i, execTs.Height(), event.Emitter, entry.Key)
						}
					}
					fmt.Println()
				}
			}
		}

		checkTs = execTs

	}

	return nil
}
