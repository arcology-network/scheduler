/*
 *   Copyright (c) 2024 Arcology Network

 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.

 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.

 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package workload

import (
	"encoding/hex"
	"math/big"
	"testing"

	commontype "github.com/arcology-network/common-lib/types"
	stgcommon "github.com/arcology-network/storage-committer/common"
	commutative "github.com/arcology-network/storage-committer/type/commutative"
	noncommutative "github.com/arcology-network/storage-committer/type/noncommutative"
	statecell "github.com/arcology-network/storage-committer/type/statecell"
	ethcore "github.com/ethereum/go-ethereum/core"
	ethcoretypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

// From in the result + in the native message
// one of them must go.

func TestResultPostprocessor(t *testing.T) {
	sender := [20]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	other := [20]byte{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
	coinbase := [20]byte{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}

	StdMsg :=
		&commontype.StandardMessage{
			Native: &ethcore.Message{
				GasPrice: big.NewInt(1),
				From:     sender,
			},
		}

	results := Result{
		// From: sender,
		// Coinbase: coinbase,
		Immuned: []*statecell.StateCell{},
		RawStateAccesses: []*statecell.StateCell{
			// sender transfer -> coinbase 50
			// sender gas fee -> Coinbase 100
			// Other transfer -> Coinbase 50
			statecell.NewStateCell(0, "blcc:/"+hex.EncodeToString(sender[:])+"/nonce", 0, 0, 0, commutative.NewUnboundedUint64(), nil),
			statecell.NewStateCell(0, "blcc:/"+hex.EncodeToString(sender[:])+"/balance", 0, 0, 0, commutative.NewU256Delta(uint256.NewInt(150), false), nil),
			statecell.NewStateCell(0, "blcc:/"+hex.EncodeToString(coinbase[:])+"/balance", 0, 0, 0, commutative.NewU256Delta(uint256.NewInt(200), true), nil),
			statecell.NewStateCell(0, "blcc:/"+hex.EncodeToString(other[:])+"/random", 0, 0, 0, noncommutative.NewString("Random"), nil),
			statecell.NewStateCell(0, "blcc:/"+hex.EncodeToString(other[:])+"/balance", 0, 0, 0, commutative.NewU256Delta(uint256.NewInt(50), false), nil),
		},

		StdMsg:  StdMsg,
		Receipt: &ethcoretypes.Receipt{GasUsed: uint64(100)},
		// Err:     errors.New("Error msg"),
	}

	normalizer := statecell.NewTransactionNormalizer(results.Receipt.GasUsed, coinbase, results.StdMsg)
	results.Immuned = normalizer.Normalize(results.RawStateAccesses)
	// execPipline := (&eu.ExecutionPipeline{Config: testEu.config})

	// eu.ExecutionPipeline(&results)

	if len(results.RawStateAccesses) != 5 {
		t.Errorf("Postprocess failed, expecting 5, got %d", len(results.RawStateAccesses))
	}
	// // results.Postprocess()

	if len(results.RawStateAccesses)+len(results.Immuned) != 8 {
		t.Errorf("Postprocess failed, expecting 7, got %d", len(results.RawStateAccesses)+len(results.Immuned))
	}

	delta, DeltaSign := results.RawStateAccesses[2].Value().(stgcommon.Type).Delta()
	if v := delta.(uint256.Int); (&v).Uint64() != 200 && DeltaSign {
		t.Errorf("Postprocess failed, expecting 100, got %d", v)
	}

	// Sender pay gas fee -100.
	delta, DeltaSign = results.Immuned[0].Value().(stgcommon.Type).Delta()
	if v := delta.(uint256.Int); (&v).Uint64() != 100 && !DeltaSign {
		t.Errorf("Postprocess failed, expecting 50, got %d", v)
	}

	// Coinbase gas fee + 100.
	delta, DeltaSign = results.Immuned[1].Value().(stgcommon.Type).Delta()
	if v := delta.(uint256.Int); (&v).Uint64() != 100 && DeltaSign {
		t.Errorf("Postprocess failed, expecting 50, got %d", v)
	}

	// Sender transfers -50.
	delta, _ = results.RawStateAccesses[1].Value().(stgcommon.Type).Delta()
	if v := delta.(uint256.Int); (&v).Uint64() != 50 && !DeltaSign {
		t.Errorf("Postprocess failed, expecting 50, got %d", v)
	}
}
