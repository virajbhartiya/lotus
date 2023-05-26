package main

import (
	"fmt"
	"path/filepath"

	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/chain/events/filter"
	"github.com/filecoin-project/lotus/chain/types"
	lcli "github.com/filecoin-project/lotus/cli"
)

var backfillEventsCmd = &cli.Command{
	Name:        "backfill-events",
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
		&cli.StringFlag{
			Name:  "repo",
			Value: "~/.lotus-local-net",
			Usage: "path to the repo",
		},
	},
	Action: func(cctx *cli.Context) error {
		return backfillEvents(cctx)
	},
}

func backfillEvents(cctx *cli.Context) error {
	srv, err := lcli.GetFullNodeServices(cctx)
	if err != nil {
		return err
	}
	defer srv.Close() //nolint:errcheck

	api := srv.FullNodeAPI()
	ctx := lcli.ReqContext(cctx)

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

	basePath, err := homedir.Expand(cctx.String("repo"))
	if err != nil {
		return err
	}

	dbPath := filepath.Join(basePath, "sqlite", "events2.db")
	eventDb, err := filter.NewEventIndex(dbPath)
	if err != nil {
		return err
	}
	db := eventDb.DB()

	defer func() {
		err := db.Close()
		if err != nil {
			fmt.Printf("ERROR: closing db: %s", err)
		}
	}()

	_, err = db.Prepare("INSERT OR IGNORE INTO event (height, tipset_key, tipset_key_cid, emitter_addr, event_index, message_cid, message_index, reverted) VALUES(?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	_, err = db.Prepare("INSERT OR IGNORE INTO event_entry(event_id, indexed, flags, key, codec, value) VALUES(?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}

	var totalRowsAffected int64 = 0
	for i := 0; i < epochs; i++ {
		select {
		case <-cctx.Done():
			fmt.Println("request cancelled")
			return nil
		default:
		}

		execTsk := checkTs.Parents()
		execTs, err := api.ChainGetTipSet(ctx, execTsk)
		if err != nil {
			return fmt.Errorf("failed to call ChainGetTipSet: %w", err)
		}

		if i%100 == 0 {
			log.Infof("%d/%d processing height:%d", i, epochs, execTs.Height())
		}

		msgs, err := api.ChainGetMessagesInTipset(cctx.Context, execTsk)
		if err != nil {
			log.Infof("could not get messages in tipset: %s\n", err)
			return err
		}

		var pMsgs []*types.Message
		for _, sm := range msgs {
			pMsgs = append(pMsgs, sm.Message)
		}
		st, err := api.StateCompute(ctx, execTs.Height(), pMsgs, execTsk)
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
						fmt.Printf("<<<<< %d %d: event:%+v entry:%+v\n", i, execTs.Height(), event, entry)
					}
				}
				fmt.Println()
			}
		}
		fmt.Println()

		/*for _, msg := range msgs {
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
					stmtEvent.Exec(
						checkTs.Height(),
						checkTs.Key().Bytes(),
						checkTs.Key().String(),
						event.Emitter,
						event.Index,
						msg.Cid,
						msg.Message.Nonce,
						event.Reverted)

					for _, entry := range event.Entries {
						eventID := 1
						res, err := stmtEntry.Exec(eventID, isIndexedValue(entry.Flags), []byte{entry.Flags}, entry.Key, entry.Codec, entry.Value)
						fmt.Printf("%d %d: emitter:%d entry:%s\n", i, execTs.Height(), event.Emitter, entry.Key)
						if err != nil {
							return fmt.Errorf("exec insert entry: %w", err)
						}

						rowsAffected, err := res.RowsAffected()
						if err != nil {
							return fmt.Errorf("error getting rows affected: %s", err)
						}
						totalRowsAffected += rowsAffected
					}
				}
				fmt.Println()
			}
		}*/

		checkTs = execTs
	}

	log.Infof("Inserted %d missing txhashes", totalRowsAffected)
	log.Infoln("Done")

	return nil
}

func isIndexedValue(b uint8) bool {
	// currently we mark the full entry as indexed if either the key
	// or the value are indexed; in the future we will need finer-grained
	// management of indices
	return b&(types.EventFlagIndexedKey|types.EventFlagIndexedValue) > 0
}
