// stm: ignore
// Only tests external library behavior, therefore it should not be annotated
package drand

import (
	"context"
	"fmt"
	dchain "github.com/drand/drand/chain"
	hclient "github.com/drand/drand/client/http"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"

	"github.com/filecoin-project/go-state-types/network"

	"github.com/filecoin-project/lotus/build"
)

func TestPrintGroupInfo(t *testing.T) {
	server := build.DrandConfigs[build.DrandTestnet].Servers[0]
	c, err := hclient.New(server, nil, nil)
	assert.NoError(t, err)
	cg := c.(interface {
		FetchChainInfo(ctx context.Context, groupHash []byte) (*dchain.Info, error)
	})
	chain, err := cg.FetchChainInfo(context.Background(), nil)
	assert.NoError(t, err)
	err = chain.ToJSON(os.Stdout, nil)
	assert.NoError(t, err)
}

func TestMaxBeaconRoundForEpoch(t *testing.T) {
	todayTs := uint64(1652222222)
	db, err := NewDrandBeacon(todayTs, build.BlockDelaySecs, nil, build.DrandConfigs[build.DrandTestnet])
	assert.NoError(t, err)
	mbr15 := db.MaxBeaconRoundForEpoch(network.Version15, 100)
	mbr16 := db.MaxBeaconRoundForEpoch(network.Version16, 100)
	assert.Equal(t, mbr15+1, mbr16)
}

func TestPrevRoundReturnsCorrectValue(t *testing.T) {
	tests := []struct {
		name           string
		filRoundTime   uint64
		drandRoundTime uint64
		round          uint64
		expected       uint64
	}{
		{
			name:           "1 returns 1",
			filRoundTime:   1,
			drandRoundTime: 1,
			round:          1,
			expected:       1,
		},
		{
			name:           "0 returns 1",
			filRoundTime:   1,
			drandRoundTime: 1,
			round:          0,
			expected:       1,
		},
		{
			name:           "filecoin block time is divided by drand time correctly",
			filRoundTime:   30,
			drandRoundTime: 3,
			round:          30,
			expected:       20,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			drandInfo := fmt.Sprintf(`{"public_key":"a0b862a7527fee3a731bcb59280ab6abd62d5c0b6ea03dc4ddf6612fdfc9d01f01c31542541771903475eb1ec6615f8d0df0b8b6dce385811d6dcf8cbefb8759e5e616a3dfd054c928940766d9a5b9db91e3b697e5d70a975181e007f87fca5e","period":%d,"genesis_time":1677685200,"hash":"dbd506d6ef76e5f386f41c651dcb808c5bcbd75471cc4eafa3f4df7ad4e4c493","groupHash":"a81e9d63f614ccdb144b8ff79fbd4d5a2d22055c0bfe4ee9a8092003dab1c6c0","schemeID":"bls-unchained-on-g1","metadata":{"beaconID":"fastnet"}}`, test.drandRoundTime)
			drandConfig := dtypes.DrandConfig{
				Servers:       []string{"https://api.drand.sh"},
				ChainInfoJSON: drandInfo,
			}
			b, err := NewDrandBeacon(1, test.filRoundTime, nil, drandConfig)
			require.NoError(t, err)
			actual := b.PrevRound(types.BeaconEntry{
				Round: test.round,
				Data:  []byte("singleoriginbeans"),
			})
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestNextRoundReturnsCorrectValue(t *testing.T) {
	tests := []struct {
		name           string
		filRoundTime   uint64
		drandRoundTime uint64
		round          uint64
		expected       uint64
	}{
		{
			name:           "matching block time adds one",
			filRoundTime:   30,
			drandRoundTime: 30,
			round:          5,
			expected:       6,
		},
		{
			name:           "smaller drand block time adds divisor",
			filRoundTime:   30,
			drandRoundTime: 3,
			round:          5,
			expected:       15,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			drandInfo := fmt.Sprintf(`{"public_key":"a0b862a7527fee3a731bcb59280ab6abd62d5c0b6ea03dc4ddf6612fdfc9d01f01c31542541771903475eb1ec6615f8d0df0b8b6dce385811d6dcf8cbefb8759e5e616a3dfd054c928940766d9a5b9db91e3b697e5d70a975181e007f87fca5e","period":%d,"genesis_time":1677685200,"hash":"dbd506d6ef76e5f386f41c651dcb808c5bcbd75471cc4eafa3f4df7ad4e4c493","groupHash":"a81e9d63f614ccdb144b8ff79fbd4d5a2d22055c0bfe4ee9a8092003dab1c6c0","schemeID":"bls-unchained-on-g1","metadata":{"beaconID":"fastnet"}}`, test.drandRoundTime)
			drandConfig := dtypes.DrandConfig{
				Servers:       []string{"https://api.drand.sh"},
				ChainInfoJSON: drandInfo,
			}
			b, err := NewDrandBeacon(1, test.filRoundTime, nil, drandConfig)
			require.NoError(t, err)
			actual := b.NextRound(types.BeaconEntry{
				Round: test.round,
				Data:  []byte("singleoriginbeans"),
			})
			require.Equal(t, test.expected, actual)
		})
	}
}
