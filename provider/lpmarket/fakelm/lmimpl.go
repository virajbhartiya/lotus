package fakelm

import (
	"context"
	"io"
	"net/http"
	"net/url"

	"github.com/google/uuid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/provider/lpmarket"
	"github.com/filecoin-project/lotus/storage/paths"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

type LMRPCProvider struct {
	si paths.SectorIndex

	maddr   address.Address // lotus-miner RPC is single-actor
	minerID abi.ActorID

	ssize abi.SectorSize

	pi lpmarket.Ingester
}

func (l *LMRPCProvider) ActorAddress(ctx context.Context) (address.Address, error) {
	return l.maddr, nil
}

func (l *LMRPCProvider) WorkerJobs(ctx context.Context) (map[uuid.UUID][]storiface.WorkerJob, error) {
	// correct enough
	return map[uuid.UUID][]storiface.WorkerJob{}, nil
}

func (l *LMRPCProvider) SectorsStatus(ctx context.Context, sid abi.SectorNumber, showOnChainInfo bool) (api.SectorInfo, error) {
	si, err := l.si.StorageFindSector(ctx, abi.SectorID{Miner: l.minerID, Number: sid}, storiface.FTSealed|storiface.FTCache, 0, false)
	if err != nil {
		return api.SectorInfo{}, err
	}

	if len(si) == 0 {
		return api.SectorInfo{
			SectorID:             sid,
			State:                api.SectorState(sealing.UndefinedSectorState),
			CommD:                nil,
			CommR:                nil,
			Proof:                nil,
			Deals:                nil,
			Pieces:               nil,
			Ticket:               api.SealTicket{},
			Seed:                 api.SealSeed{},
			PreCommitMsg:         nil,
			CommitMsg:            nil,
			Retries:              0,
			ToUpgrade:            false,
			ReplicaUpdateMessage: nil,
			LastErr:              "",
			Log:                  nil,
			SealProof:            0,
			Activation:           0,
			Expiration:           0,
			DealWeight:           abi.DealWeight{},
			VerifiedDealWeight:   abi.DealWeight{},
			InitialPledge:        abi.TokenAmount{},
			OnTime:               0,
			Early:                0,
		}, nil
	}

	var state api.SectorState = api.SectorState(sealing.Proving)
	if !si[0].CanStore {
		state = api.SectorState(sealing.PreCommit1)
	}

	// todo improve this with on-chain info
	return api.SectorInfo{
		SectorID:             sid,
		State:                state,
		CommD:                nil,
		CommR:                nil,
		Proof:                nil,
		Deals:                nil,
		Pieces:               nil,
		Ticket:               api.SealTicket{},
		Seed:                 api.SealSeed{},
		PreCommitMsg:         nil,
		CommitMsg:            nil,
		Retries:              0,
		ToUpgrade:            false,
		ReplicaUpdateMessage: nil,
		LastErr:              "",
		Log:                  nil,

		SealProof:          0,
		Activation:         0,
		Expiration:         0,
		DealWeight:         abi.DealWeight{},
		VerifiedDealWeight: abi.DealWeight{},
		InitialPledge:      abi.TokenAmount{},
		OnTime:             0,
		Early:              0,
	}, nil
}

func (l *LMRPCProvider) SectorsList(ctx context.Context) ([]abi.SectorNumber, error) {
	decls, err := l.si.StorageList(ctx)
	if err != nil {
		return nil, err
	}

	var out []abi.SectorNumber
	for _, decl := range decls {
		for _, s := range decl {
			if s.Miner != l.minerID {
				continue
			}

			out = append(out, s.SectorID.Number)
		}
	}

	return out, nil
}

type sectorParts struct {
	sealed, unsealed, cache bool
	inStorage               bool
}

func (l *LMRPCProvider) SectorsSummary(ctx context.Context) (map[api.SectorState]int, error) {
	decls, err := l.si.StorageList(ctx)
	if err != nil {
		return nil, err
	}

	states := map[abi.SectorID]sectorParts{}
	for si, decll := range decls {
		sinfo, err := l.si.StorageInfo(ctx, si)
		if err != nil {
			return nil, err
		}

		for _, decl := range decll {
			if decl.Miner != l.minerID {
				continue
			}

			state := states[abi.SectorID{Miner: decl.Miner, Number: decl.SectorID.Number}]
			state.sealed = state.sealed || decl.Has(storiface.FTSealed)
			state.unsealed = state.unsealed || decl.Has(storiface.FTUnsealed)
			state.cache = state.cache || decl.Has(storiface.FTCache)
			state.inStorage = state.inStorage || sinfo.CanStore
			states[abi.SectorID{Miner: decl.Miner, Number: decl.SectorID.Number}] = state
		}
	}

	out := map[api.SectorState]int{}
	for _, state := range states {
		switch {
		case state.sealed && state.inStorage:
			out[api.SectorState(sealing.Proving)]++
		default:
			// not even close to correct, but good enough for now
			out[api.SectorState(sealing.PreCommit1)]++
		}
	}

	return out, nil
}

func (l *LMRPCProvider) SectorsListInStates(ctx context.Context, want []api.SectorState) ([]abi.SectorNumber, error) {
	decls, err := l.StorageList(ctx)
	if err != nil {
		return nil, err
	}

	wantProving, wantPrecommit1 := false, false
	for _, s := range want {
		switch s {
		case api.SectorState(sealing.Proving):
			wantProving = true
		case api.SectorState(sealing.PreCommit1):
			wantPrecommit1 = true
		}
	}

	states := map[abi.SectorID]sectorParts{}

	for si, decll := range decls {
		sinfo, err := l.si.StorageInfo(ctx, si)
		if err != nil {
			return nil, err
		}

		for _, decl := range decll {
			if decl.Miner != l.minerID {
				continue
			}

			state := states[abi.SectorID{Miner: decl.Miner, Number: decl.SectorID.Number}]
			state.sealed = state.sealed || decl.Has(storiface.FTSealed)
			state.unsealed = state.unsealed || decl.Has(storiface.FTUnsealed)
			state.cache = state.cache || decl.Has(storiface.FTCache)
			state.inStorage = state.inStorage || sinfo.CanStore
			states[abi.SectorID{Miner: decl.Miner, Number: decl.SectorID.Number}] = state
		}
	}
	var out []abi.SectorNumber

	for id, state := range states {
		switch {
		case state.sealed && state.inStorage:
			if wantProving {
				out = append(out, id.Number)
			}
		default:
			// not even close to correct, but good enough for now
			if wantPrecommit1 {
				out = append(out, id.Number)
			}
		}
	}

	return out, nil
}

func (l *LMRPCProvider) StorageRedeclareLocal(ctx context.Context, id *storiface.ID, b bool) error {
	// so this rescans and redeclares sectors on lotus-miner; whyyy is boost even calling this?

	return nil
}

func (l *LMRPCProvider) StorageList(ctx context.Context) (map[storiface.ID][]storiface.Decl, error) {
	return l.si.StorageList(ctx)
}

func (l *LMRPCProvider) UnsealSector(ctx context.Context, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (io.ReadCloser, error) {
	return nil, xerrors.Errorf("not supported")
}

func (l *LMRPCProvider) IsUnsealed(ctx context.Context, sectorNum abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (bool, error) {
	sectorID := abi.SectorID{Miner: l.minerID, Number: sectorNum}

	si, err := l.si.StorageFindSector(ctx, sectorID, storiface.FTUnsealed, 0, false)
	if err != nil {
		return false, err
	}

	// yes, yes, technically sectors can be partially unsealed, but that is never done in practice
	// and can't even be easily done with the current implementation
	return len(si) > 0, nil
}

func (l *LMRPCProvider) ComputeDataCid(ctx context.Context, pieceSize abi.UnpaddedPieceSize, pieceData storiface.Data) (abi.PieceInfo, error) {
	return abi.PieceInfo{}, xerrors.Errorf("not supported")
}

func (l *LMRPCProvider) SectorAddPieceToAny(ctx context.Context, size abi.UnpaddedPieceSize, r storiface.Data, d api.PieceDealInfo) (api.SectorOffset, error) {
	if d.DealProposal.PieceSize != abi.PaddedPieceSize(l.ssize) {
		return api.SectorOffset{}, xerrors.Errorf("only full-sector pieces are supported")
	}

	return api.SectorOffset{}, xerrors.Errorf("not supported, use AllocatePieceToSector")
}

func (l *LMRPCProvider) AllocatePieceToSector(ctx context.Context, maddr address.Address, piece api.PieceDealInfo, rawSize int64, source url.URL, header http.Header) (api.SectorOffset, error) {
	return l.pi.AllocatePieceToSector(ctx, maddr, piece, rawSize, source, header)
}

var _ MinimalLMApi = &LMRPCProvider{}