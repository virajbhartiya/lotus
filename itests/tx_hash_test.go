package itests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/types/ethtypes"
	"github.com/filecoin-project/lotus/itests/kit"
)

// TestTransactionHashLookupBlsFilecoinMessage tests to see if lotus can find a BLS Filecoin Message using the transaction hash
func TestTransactionHashLookupBlsFilecoinMessage(t *testing.T) {
	kit.QuietMiningLogs()

	blocktime := 1 * time.Second
	client, _, ens := kit.EnsembleMinimal(
		t,
		kit.MockProofs(),
		kit.ThroughRPC(),
	)
	ens.InterconnectAll().BeginMining(blocktime)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// get the existing balance from the default wallet to then split it.
	bal, err := client.WalletBalance(ctx, client.DefaultKey.Address)
	require.NoError(t, err)

	// create a new address where to send funds.
	addr, err := client.WalletNew(ctx, types.KTBLS)
	require.NoError(t, err)

	toSend := big.Div(bal, big.NewInt(2))
	msg := &types.Message{
		From:  client.DefaultKey.Address,
		To:    addr,
		Value: toSend,
	}

	sm, err := client.MpoolPushMessage(ctx, msg, nil)
	require.NoError(t, err)

	hash, err := ethtypes.EthHashFromCid(sm.Message.Cid())
	require.NoError(t, err)

	mpoolTx, err := client.EthGetTransactionByHash(ctx, &hash)
	require.NoError(t, err)
	require.Equal(t, hash, mpoolTx.Hash)

	// Wait for message to land on chain
	var receipt *api.EthTxReceipt
	for i := 0; i < 20; i++ {
		receipt, err = client.EthGetTransactionReceipt(ctx, hash)
		if err != nil || receipt == nil {
			time.Sleep(blocktime)
			continue
		}
		break
	}
	require.NoError(t, err)
	require.NotNil(t, receipt)
	require.Equal(t, hash, receipt.TransactionHash)

	// Verify that the chain transaction now has new fields set.
	chainTx, err := client.EthGetTransactionByHash(ctx, &hash)
	require.NoError(t, err)
	require.Equal(t, hash, chainTx.Hash)

	// require that the hashes are identical
	require.Equal(t, hash, chainTx.Hash)
	require.NotNil(t, chainTx.BlockNumber)
	require.Greater(t, uint64(*chainTx.BlockNumber), uint64(0))
	require.NotNil(t, chainTx.BlockHash)
	require.NotEmpty(t, *chainTx.BlockHash)
	require.NotNil(t, chainTx.TransactionIndex)
	require.Equal(t, uint64(*chainTx.TransactionIndex), uint64(0)) // only transaction
}

// TestTransactionHashLookupSecpFilecoinMessage tests to see if lotus can find a Secp Filecoin Message using the transaction hash
func TestTransactionHashLookupSecpFilecoinMessage(t *testing.T) {
	kit.QuietMiningLogs()

	blocktime := 1 * time.Second
	client, _, ens := kit.EnsembleMinimal(
		t,
		kit.MockProofs(),
		kit.ThroughRPC(),
	)
	ens.InterconnectAll().BeginMining(blocktime)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// get the existing balance from the default wallet to then split it.
	bal, err := client.WalletBalance(ctx, client.DefaultKey.Address)
	require.NoError(t, err)

	// create a new address where to send funds.
	addr, err := client.WalletNew(ctx, types.KTSecp256k1)
	require.NoError(t, err)

	toSend := big.Div(bal, big.NewInt(2))
	setupMsg := &types.Message{
		From:  client.DefaultKey.Address,
		To:    addr,
		Value: toSend,
	}

	setupSmsg, err := client.MpoolPushMessage(ctx, setupMsg, nil)
	require.NoError(t, err)

	_, err = client.StateWaitMsg(ctx, setupSmsg.Cid(), 3, api.LookbackNoLimit, true)
	require.NoError(t, err)

	// Send message for secp account
	secpMsg := &types.Message{
		From:  addr,
		To:    client.DefaultKey.Address,
		Value: big.Div(toSend, big.NewInt(2)),
	}

	secpSmsg, err := client.MpoolPushMessage(ctx, secpMsg, nil)
	require.NoError(t, err)

	hash, err := ethtypes.EthHashFromCid(secpSmsg.Cid())
	require.NoError(t, err)

	mpoolTx, err := client.EthGetTransactionByHash(ctx, &hash)
	require.NoError(t, err)
	require.Equal(t, hash, mpoolTx.Hash)

	_, err = client.StateWaitMsg(ctx, secpSmsg.Cid(), 3, api.LookbackNoLimit, true)
	require.NoError(t, err)

	receipt, err := client.EthGetTransactionReceipt(ctx, hash)
	require.NoError(t, err)
	require.NotNil(t, receipt)
	require.Equal(t, hash, receipt.TransactionHash)

	// Verify that the chain transaction now has new fields set.
	chainTx, err := client.EthGetTransactionByHash(ctx, &hash)
	require.NoError(t, err)
	require.Equal(t, hash, chainTx.Hash)

	// require that the hashes are identical
	require.Equal(t, hash, chainTx.Hash)
	require.NotNil(t, chainTx.BlockNumber)
	require.Greater(t, uint64(*chainTx.BlockNumber), uint64(0))
	require.NotNil(t, chainTx.BlockHash)
	require.NotEmpty(t, *chainTx.BlockHash)
	require.NotNil(t, chainTx.TransactionIndex)
	require.Equal(t, uint64(*chainTx.TransactionIndex), uint64(0)) // only transaction
}
