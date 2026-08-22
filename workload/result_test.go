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
	"encoding/json"
	"errors"
	"math/big"
	"testing"

	crdtcommon "github.com/arcology-network/common-lib/crdt/common"
	commutative "github.com/arcology-network/common-lib/crdt/commutative"
	noncommutative "github.com/arcology-network/common-lib/crdt/noncommutative"
	statecell "github.com/arcology-network/common-lib/crdt/statecell"
	commontype "github.com/arcology-network/common-lib/types"
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
		RawStateRecords: []*statecell.StateCell{
			// sender transfer -> coinbase 50
			// sender gas fee -> Coinbase 100
			// Other transfer -> Coinbase 50
			statecell.NewStateCell(0, "blcc:/"+hex.EncodeToString(sender[:])+"/nonce", 0, 0, 0, commutative.NewUnboundedUint64(), nil),
			statecell.NewStateCell(0, "blcc:/"+hex.EncodeToString(sender[:])+"/balance", 0, 0, 0, commutative.NewU256Delta(uint256.NewInt(150), false), nil),
			statecell.NewStateCell(0, "blcc:/"+hex.EncodeToString(coinbase[:])+"/balance", 0, 0, 0, commutative.NewU256Delta(uint256.NewInt(200), true), nil),
			statecell.NewStateCell(0, "blcc:/"+hex.EncodeToString(other[:])+"/random", 0, 0, 0, noncommutative.NewString("Random"), nil),
			statecell.NewStateCell(0, "blcc:/"+hex.EncodeToString(other[:])+"/balance", 0, 0, 0, commutative.NewU256Delta(uint256.NewInt(50), false), nil),
		},

		TxInfo:  StdMsg.ToView(),
		Receipt: &ethcoretypes.Receipt{GasUsed: uint64(100)},
		// Err:     errors.New("Error msg"),
	}

	normalizer := NewTransactionNormalizer(results.Receipt.GasUsed, coinbase, results.TxInfo)
	results.RawStateRecords, results.Immuned = normalizer.Normalize(
		nil,
		results.RawStateRecords,
		IndependentExecution,
	)
	// execPipline := (&eu.ExecutionPipeline{Config: testEu.config})

	// eu.ExecutionPipeline(&results)

	if len(results.RawStateRecords) != 5 {
		t.Errorf("Postprocess failed, expecting 5, got %d", len(results.RawStateRecords))
	}
	// // results.Postprocess()

	if len(results.RawStateRecords)+len(results.Immuned) != 8 {
		t.Errorf("Postprocess failed, expecting 7, got %d", len(results.RawStateRecords)+len(results.Immuned))
	}

	delta, DeltaSign := results.RawStateRecords[2].Value().(crdtcommon.CRDT).Delta()
	if v := delta.(uint256.Int); (&v).Uint64() != 200 && DeltaSign {
		t.Errorf("Postprocess failed, expecting 100, got %d", v)
	}

	// Sender pay gas fee -100.
	delta, DeltaSign = results.Immuned[0].Value().(crdtcommon.CRDT).Delta()
	if v := delta.(uint256.Int); (&v).Uint64() != 100 && !DeltaSign {
		t.Errorf("Postprocess failed, expecting 50, got %d", v)
	}

	// Coinbase gas fee + 100.
	delta, DeltaSign = results.Immuned[1].Value().(crdtcommon.CRDT).Delta()
	if v := delta.(uint256.Int); (&v).Uint64() != 100 && DeltaSign {
		t.Errorf("Postprocess failed, expecting 50, got %d", v)
	}

	// Sender transfers -50.
	delta, _ = results.RawStateRecords[1].Value().(crdtcommon.CRDT).Delta()
	if v := delta.(uint256.Int); (&v).Uint64() != 50 && !DeltaSign {
		t.Errorf("Postprocess failed, expecting 50, got %d", v)
	}
}

func marshalResultToJSONMap(t *testing.T, value any) map[string]json.RawMessage {
	t.Helper()

	blob, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	out := make(map[string]json.RawMessage)
	if err := json.Unmarshal(blob, &out); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	return out
}

func makeStateCell(tx uint64, seq uint64, path string, reads, writes, delta uint32) *statecell.StateCell {
	cell := statecell.NewStateCell(tx, path, reads, writes, delta, nil, nil)
	cell.JobSequenceID = seq
	return cell
}

func TestResultMarshalJSONSchema(t *testing.T) {
	cells := []*statecell.StateCell{
		makeStateCell(11, 21, "blcc://state/0", 1, 2, 3),
		makeStateCell(12, 22, "blcc://state/1", 0, 1, 0),
	}

	var hash [32]byte
	hash[0] = 1
	hash[31] = 2

	result := &Result{
		GenerationID:  7,
		JobSequenceID: 8,
		JobID:         9,
		// TxIndex:         10,
		RawStateRecords: []*statecell.StateCell{cells[0]},
		Immuned:         []*statecell.StateCell{cells[1]},
		Receipt:         &ethcoretypes.Receipt{GasUsed: 21000},
		EvmResult:       &ethcore.ExecutionResult{UsedGas: 21000},
		TxInfo:          nil,
		Err:             errors.New("boom"),
	}

	payload := marshalResultToJSONMap(t, result)

	required := []string{"generationId", "jobSequenceId", "jobId", "rawStateRecords", "immuned", "receipt", "evmResult", "stdMsg", "err"}
	for _, key := range required {
		if _, found := payload[key]; !found {
			t.Fatalf("missing %q in marshalled result", key)
		}
	}

	var errStr string
	if err := json.Unmarshal(payload["err"], &errStr); err != nil {
		t.Fatalf("decode err failed: %v", err)
	}
	if errStr != "boom" {
		t.Fatalf("unexpected err payload: %s", errStr)
	}

	var rawRecords []map[string]any
	if err := json.Unmarshal(payload["rawStateRecords"], &rawRecords); err != nil {
		t.Fatalf("decode rawStateRecords failed: %v", err)
	}
	if len(rawRecords) != 1 {
		t.Fatalf("unexpected raw state record count: %d", len(rawRecords))
	}

	rawPath, ok := rawRecords[0]["path"].(string)
	if !ok {
		t.Fatalf("rawStateRecords path is not a string: %#v", rawRecords[0]["path"])
	}
	if rawPath != "blcc://state/0" {
		t.Fatalf("unexpected path: %s", rawPath)
	}
	rawTx, ok := rawRecords[0]["tx"].(float64)
	if !ok {
		t.Fatalf("rawStateRecords tx is not numeric: %#v", rawRecords[0]["tx"])
	}
	if rawTx != 11 {
		t.Fatalf("unexpected tx: %v", rawTx)
	}
	rawSeq, ok := rawRecords[0]["sequence"].(float64)
	if !ok {
		t.Fatalf("rawStateRecords sequence is not numeric: %#v", rawRecords[0]["sequence"])
	}
	if rawSeq != 21 {
		t.Fatalf("unexpected jobSequenceId: %v", rawSeq)
	}

	var immuned []map[string]any
	if err := json.Unmarshal(payload["immuned"], &immuned); err != nil {
		t.Fatalf("decode immuned failed: %v", err)
	}
	if len(immuned) != 1 {
		t.Fatalf("unexpected immuned count: %d", len(immuned))
	}
	immunedPath, ok := immuned[0]["path"].(string)
	if !ok {
		t.Fatalf("immuned path is not a string: %#v", immuned[0]["path"])
	}
	if immunedPath != "blcc://state/1" {
		t.Fatalf("unexpected immuned path: %s", immunedPath)
	}
}
