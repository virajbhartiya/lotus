package main

import (
	"fmt"
	"path/filepath"

	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"

	"github.com/filecoin-project/lotus/chain/ethhashlookup"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	lcli "github.com/filecoin-project/lotus/cli"
)

var backfillTxHashCmd = &cli.Command{
	Name:        "backfill-txhash",
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
		return backfillTxHash(cctx)
	},
}

func backfillTxHash(cctx *cli.Context) error {
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

	dbPath := filepath.Join(basePath, "sqlite", "txhash.db")
	ethDb, err := ethhashlookup.NewTransactionHashLookup(dbPath)
	if err != nil {
		return err
	}
	db := ethDb.DB()

	defer func() {
		err := db.Close()
		if err != nil {
			fmt.Printf("ERROR: closing db: %s", err)
		}
	}()

	insertStmt, err := db.Prepare("INSERT OR IGNORE INTO eth_tx_hashes(hash, cid) VALUES(?, ?)")
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

		for _, blockheader := range execTs.Blocks() {
			blkMsgs, err := api.ChainGetBlockMessages(ctx, blockheader.Cid())
			if err != nil {
				log.Infof("Could not get block messages at height: %d, stopping walking up the chain", execTs.Height())
				epochs = i
				break
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

				res, err := insertStmt.Exec(tx.Hash.String(), smsg.Cid().String())
				if err != nil {
					return fmt.Errorf("error inserting tx mapping to db: %s", err)
				}

				rowsAffected, err := res.RowsAffected()
				if err != nil {
					return fmt.Errorf("error getting rows affected: %s", err)
				}

				if rowsAffected > 0 {
					log.Infof("Inserted txhash %s, cid: %s", tx.Hash.String(), smsg.Cid().String())
				}

				totalRowsAffected += rowsAffected
			}
		}

		checkTs = execTs
	}

	log.Infof("Inserted %d missing txhashes", totalRowsAffected)
	log.Infoln("Done")

	return nil
}
