package chainindex

import (
	"context"
	"strconv"
	"time"

	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/lotus/chain/actors/builtin"
	"github.com/filecoin-project/lotus/chain/actors/policy"
)

var (
	log             = logging.Logger("chainindex")
	cleanupInterval = time.Duration(8) * time.Hour
)

func (si *SqliteIndexer) gcLoop() {
	defer si.wg.Done()

	// Initial cleanup before entering the loop
	si.cleanupRevertedTipsets(si.ctx)
	si.gc(si.ctx)

	cleanupTicker := time.NewTicker(cleanupInterval)
	defer cleanupTicker.Stop()

	for si.ctx.Err() == nil {
		select {
		case <-cleanupTicker.C:
			si.cleanupRevertedTipsets(si.ctx)
			si.gc(si.ctx)
		case <-si.ctx.Done():
			return
		}
	}
}

func (si *SqliteIndexer) gc(ctx context.Context) {
	if si.gcRetentionEpochs <= 0 {
		log.Info("gc retention epochs is not set, skipping gc")
		return
	}
	log.Info("starting index gc")

	head := si.cs.GetHeaviestTipSet()

	removalEpoch := int64(head.Height()) - si.gcRetentionEpochs - 10 // 10 is for some grace period
	if removalEpoch <= 0 {
		log.Info("no tipsets to gc")
		return
	}

	log.Infof("gc'ing all(reverted and non-reverted) tipsets before epoch %d", removalEpoch)

	res, err := si.removeTipsetsBeforeHeightStmt.ExecContext(ctx, removalEpoch)
	if err != nil {
		log.Errorw("failed to remove reverted tipsets before height", "height", removalEpoch, "error", err)
		return
	}

	rows, err := res.RowsAffected()
	if err != nil {
		log.Errorw("failed to get rows affected", "error", err)
		return
	}

	log.Infof("gc'd %d tipsets before epoch %d", rows, removalEpoch)

	// -------------------------------------------------------------------------------------------------
	// Also GC eth hashes

	// Convert gcRetentionEpochs to number of days
	gcRetentionDays := si.gcRetentionEpochs / (builtin.EpochsInDay)
	if gcRetentionDays < 1 {
		log.Infof("skipping gc of eth hashes as retention days is less than 1")
		return
	}

	log.Infof("gc'ing eth hashes older than %d days", gcRetentionDays)
	res, err = si.removeEthHashesOlderThanStmt.Exec("-" + strconv.Itoa(int(gcRetentionDays)) + " day")
	if err != nil {
		log.Errorf("failed to gc eth hashes older than %d days: %w", gcRetentionDays, err)
		return
	}

	rows, err = res.RowsAffected()
	if err != nil {
		log.Errorf("failed to get rows affected: %w", err)
		return
	}

	log.Infof("gc'd %d eth hashes older than %d days", rows, gcRetentionDays)
}

func (si *SqliteIndexer) cleanupRevertedTipsets(ctx context.Context) {
	head := si.cs.GetHeaviestTipSet()
	finalEpoch := (head.Height() - policy.ChainFinality) - 10 // 10 is for some grace period
	if finalEpoch <= 0 {
		return
	}

	log.Infof("cleaning up all reverted tipsets before epoch %d as it is now final", finalEpoch)

	// remove all entries from the `tipsets` table where `reverted=true` and height is < finalEpoch
	// cascade delete based on foreign key constraints takes care of cleaning up the other tables
	res, err := si.removeRevertedTipsetsBeforeHeightStmt.ExecContext(ctx, finalEpoch)
	if err != nil {
		log.Errorw("failed to remove reverted tipsets before height", "height", finalEpoch, "error", err)
		return
	}

	rows, err := res.RowsAffected()
	if err != nil {
		log.Errorw("failed to get rows affected", "error", err)
		return
	}

	log.Infow("removed reverted tipsets", "height", finalEpoch, "nRows", rows)
}
