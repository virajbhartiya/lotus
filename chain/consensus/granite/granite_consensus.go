package granite

import "github.com/filecoin-project/lotus/chain/types"

type GraniteConsensus struct {
}

// proposal is the proposed tipset (a bunch of blocks). Only some of them will be accepted
// (then they will become the new canonical tipset).
//
// participants are the SPs who participated in the consensus algorithm.
func (gc GraniteConsensus) Consensus(i int64, proposal *types.TipSet, participants []int64) (decision types.TipSet, proof int64) {
	return *proposal, 0
}

func (gc GraniteConsensus) Verify(proof int64, decision *types.TipSet, participants []int64) bool {
	return true
}
