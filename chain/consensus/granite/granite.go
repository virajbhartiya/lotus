package granite

import (
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/consensus"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
)

type Granite struct {
	store *store.ChainStore
}

func (g Granite) ValidateBlockHeader(ctx context.Context, b *types.BlockHeader) (rejectReason string, err error) {
	//TODO implement me
	panic("implement me")
}

func (g Granite) ValidateBlock(ctx context.Context, b *types.FullBlock) (err error) {
	//TODO implement me
	panic("implement me")
}

func (g Granite) IsEpochInConsensusRange(epoch abi.ChainEpoch) bool {
	//TODO implement me
	panic("implement me")
}

func (g Granite) CreateBlock(ctx context.Context, w api.Wallet, bt *api.BlockTemplate) (*types.FullBlock, error) {
	//TODO implement me
	panic("implement me")
}

func NewGraniteConsensus() consensus.Consensus {
	return Granite{}
}
