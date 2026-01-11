/*
 *   Copyright (c) 2026 Arcology Network

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
	"bytes"
	"math/big"
	"testing"

	commutative "github.com/arcology-network/common-lib/crdt/commutative"
	noncommutative "github.com/arcology-network/common-lib/crdt/noncommutative"
	statecell "github.com/arcology-network/common-lib/crdt/statecell"
	commontype "github.com/arcology-network/common-lib/types"
	ethcommon "github.com/ethereum/go-ethereum/common"
	ethcore "github.com/ethereum/go-ethereum/core"
	ethcoretypes "github.com/ethereum/go-ethereum/core/types"
)

func buildStandardMessage(from ethcommon.Address, to *ethcommon.Address, gasPrice *big.Int, selector []byte) *commontype.StandardMessage {
	data := make([]byte, len(selector))
	copy(data, selector)

	return &commontype.StandardMessage{
		Native: &ethcore.Message{
			From:     from,
			To:       to,
			GasPrice: gasPrice,
			Data:     data,
		},
	}
}

func buildResultFixture(t *testing.T) *Result {
	t.Helper()

	from := ethcommon.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	to := ethcommon.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	gasPrice := big.NewInt(0).SetUint64(123456789)
	selector := []byte{0xde, 0xad, 0xbe, 0xef}

	stdMsg := buildStandardMessage(from, &to, gasPrice, selector)

	var hash [32]byte
	copy(hash[:], []byte("result-internal-codec-hash........"))

	rawState := statecell.NewStateCell(11, "blcc://account/alpha", 1, 1, 0, noncommutative.NewString("alpha"), nil)
	immunedState := statecell.NewStateCell(12, "blcc://account/beta", 0, 1, 0, commutative.NewUnboundedUint64(), nil)

	return &Result{
		GenerationID:    1,
		JobSequenceID:   2,
		JobID:           3,
		TxIndex:         4,
		TxHash:          hash,
		MsgView:         stdMsg.ToView(),
		RawStateRecords: []*statecell.StateCell{rawState},
		Immuned:         []*statecell.StateCell{immunedState},
		Receipt:         &ethcoretypes.Receipt{GasUsed: 21000},
		EvmResult:       &ethcore.ExecutionResult{UsedGas: 21000, ReturnData: []byte{0xab, 0xcd}},
	}
}

func TestResultEncodeDecodeRoundTrip(t *testing.T) {
	original := buildResultFixture(t)

	encoded := original.Encode()

	decoded := new(Result)
	if _, err := decoded.Decode(encoded); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.GenerationID != original.GenerationID {
		t.Fatalf("generation id mismatch: got %d want %d", decoded.GenerationID, original.GenerationID)
	}
	if decoded.JobSequenceID != original.JobSequenceID {
		t.Fatalf("job sequence id mismatch: got %d want %d", decoded.JobSequenceID, original.JobSequenceID)
	}
	if decoded.JobID != original.JobID {
		t.Fatalf("job id mismatch: got %d want %d", decoded.JobID, original.JobID)
	}
	if decoded.TxIndex != original.TxIndex {
		t.Fatalf("tx index mismatch: got %d want %d", decoded.TxIndex, original.TxIndex)
	}
	if decoded.TxHash != original.TxHash {
		t.Fatalf("tx hash mismatch: got %x want %x", decoded.TxHash, original.TxHash)
	}

	if decoded.MsgView == nil {
		t.Fatal("decoded message view is nil")
	}
	originalMsgView, err := original.MsgView.Encode()
	if err != nil {
		t.Fatalf("original message view encode failed: %v", err)
	}
	decodedMsgView, err := decoded.MsgView.Encode()
	if err != nil {
		t.Fatalf("decoded message view encode failed: %v", err)
	}
	if !bytes.Equal(decodedMsgView, originalMsgView) {
		t.Fatalf("message view payload mismatch")
	}

	originalRawEncoded := statecell.StateCells(original.RawStateRecords).Encode()
	decodedRawEncoded := statecell.StateCells(decoded.RawStateRecords).Encode()
	if !bytes.Equal(decodedRawEncoded, originalRawEncoded) {
		t.Fatalf("raw state records mismatch")
	}

	originalImmunedEncoded := statecell.StateCells(original.Immuned).Encode()
	decodedImmunedEncoded := statecell.StateCells(decoded.Immuned).Encode()
	if !bytes.Equal(decodedImmunedEncoded, originalImmunedEncoded) {
		t.Fatalf("immuned state records mismatch")
	}

	originalReceiptEncoded, err := commontype.EncodeReceipt(original.Receipt)
	if err != nil {
		t.Fatalf("original receipt encode failed: %v", err)
	}
	decodedReceiptEncoded, err := commontype.EncodeReceipt(decoded.Receipt)
	if err != nil {
		t.Fatalf("decoded receipt encode failed: %v", err)
	}
	if !bytes.Equal(decodedReceiptEncoded, originalReceiptEncoded) {
		t.Fatalf("receipt payload mismatch")
	}

	originalResultEncoded, err := commontype.EncodeExecutionResult(original.EvmResult)
	if err != nil {
		t.Fatalf("original evm result encode failed: %v", err)
	}
	decodedResultEncoded, err := commontype.EncodeExecutionResult(decoded.EvmResult)
	if err != nil {
		t.Fatalf("decoded evm result encode failed: %v", err)
	}
	if !bytes.Equal(decodedResultEncoded, originalResultEncoded) {
		t.Fatalf("execution result payload mismatch")
	}

	reencoded := decoded.Encode()
	if !bytes.Equal(reencoded, encoded) {
		t.Fatalf("encode/decode round trip mismatch")
	}
}

func TestResultEncodeDecodeWithEmptySlices(t *testing.T) {
	from := ethcommon.HexToAddress("0x9999999999999999999999999999999999999999")
	selector := []byte{0x01, 0x02, 0x03, 0x04}
	stdMsg := buildStandardMessage(from, nil, big.NewInt(0), selector)

	var hash [32]byte
	copy(hash[:], []byte("result-internal-codec-empty........"))

	original := &Result{
		GenerationID:    100,
		JobSequenceID:   200,
		JobID:           300,
		TxIndex:         400,
		TxHash:          hash,
		MsgView:         stdMsg.ToView(),
		RawStateRecords: nil,
		Immuned:         nil,
		Receipt:         nil,
		EvmResult:       nil,
	}

	encoded := original.Encode()

	decoded := new(Result)
	if _, err := decoded.Decode(encoded); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(decoded.RawStateRecords) != 0 {
		t.Fatalf("expected no raw state records, got %d", len(decoded.RawStateRecords))
	}
	if len(decoded.Immuned) != 0 {
		t.Fatalf("expected no immuned records, got %d", len(decoded.Immuned))
	}
	if decoded.Receipt != nil {
		t.Fatalf("expected nil receipt, got %#v", decoded.Receipt)
	}
	if decoded.EvmResult != nil {
		t.Fatalf("expected nil evm result, got %#v", decoded.EvmResult)
	}

	reencoded := decoded.Encode()
	if !bytes.Equal(reencoded, encoded) {
		t.Fatalf("encode/decode mismatch for empty payload")
	}
}
