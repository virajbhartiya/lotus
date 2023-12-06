package consensus

import (
	"fmt"
	"github.com/filecoin-project/lotus/chain/types"
	"golang.org/x/xerrors"
)

type FinalityState struct {
	lastFinalizedEpoch        int64
	lastGraniteInstanceNumber int64
	powerTable                types.PowerTable
}

func (fs *FinalityState) ValidateFinalityCertificate(fc *types.FinalityCertificate) error {
	if fc == nil {
		fmt.Println("Empty FinalityCertificate. Skip.")
		return nil
	}

	// TODO(jie): Validate voter's identity and total power
	// TODO(jie): Validate blssignature

	if fs.lastFinalizedEpoch >= fc.GraniteDecision.Epoch {
		return xerrors.Errorf("last finalized epoch %d >= proposed finalized epoch %d", fs.lastFinalizedEpoch, fc.GraniteDecision.Epoch)
	}
	if fs.lastGraniteInstanceNumber >= fc.GraniteDecision.InstanceNumber {
		return xerrors.Errorf("last granite instance %d >= proposed granite instance %d", fs.lastGraniteInstanceNumber, fc.GraniteDecision.InstanceNumber)
	}

	// TODO(jie): Update fields in fs

	return nil
}
