package ffi

import (
	"context"
	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/lib/harmony/harmonytask"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
)

func (sb *SealCalls) EncodeUpdate(
	ctx context.Context,
	taskID harmonytask.TaskID,
	proofType abi.RegisteredUpdateProof,
	sector storiface.SectorRef,
	pieces []abi.PieceInfo) (sealedCID cid.Cid, unsealedCID cid.Cid, err error) {

	paths, pathIDs, releaseSector, err := sb.sectors.AcquireSector(ctx, &taskID, sector, storiface.FTNone, storiface.FTUpdate|storiface.FTUpdateCache, storiface.PathSealing)
	if err != nil {
		return cid.Undef, cid.Undef, xerrors.Errorf("acquiring sector paths: %w", err)
	}
	defer releaseSector()

	replicaPath := ""      // TODO can this be a named pipe??
	replicaCachePath := "" // todo some temp copy??
	stagedDataPath := ""   // todo can this be a named pipe??

	sealed, unsealed, err := ffi.SectorUpdate.EncodeInto(proofType, paths.Update, paths.UpdateCache, replicaPath, replicaCachePath, stagedDataPath, pieces)
	if err != nil {
		return cid.Undef, cid.Undef, xerrors.Errorf("ffi update encode: %w", err)
	}

	if err := sb.ensureOneCopy(ctx, sector.ID, pathIDs, storiface.FTUpdate|storiface.FTUpdateCache); err != nil {
		return cid.Undef, cid.Undef, xerrors.Errorf("ensure one copy: %w", err)
	}

	return sealed, unsealed, nil
}
