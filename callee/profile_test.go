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

package profile

import (
	"encoding/json"
	"slices"
	"testing"

	statecommon "github.com/arcology-network/state-engine/common"
	ethcommon "github.com/ethereum/go-ethereum/common"
)

func marshalProfileJSON(t *testing.T, prof *Profile) map[string]json.RawMessage {
	t.Helper()

	blob, err := prof.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	out := make(map[string]json.RawMessage)
	if err := json.Unmarshal(blob, &out); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	return out
}

func TestProfileMarshalJSONSchema(t *testing.T) {
	addr := ethcommon.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")
	selector := [4]byte{0xaa, 0xbb, 0xcc, 0xdd}

	prof := &Profile{
		ID:                NewID(42, addr, selector),
		parallelismDegree: 3,
		prepayment:        99,
		ConflictPeers:     []uint64{7, 8, 9},
	}

	payload := marshalProfileJSON(t, prof)

	required := []string{"id", "parallelismDegree", "prepayment", "conflictPeers"}
	for _, key := range required {
		if _, found := payload[key]; !found {
			t.Fatalf("missing %q in marshalled payload", key)
		}
	}

	var parallelism uint32
	if err := json.Unmarshal(payload["parallelismDegree"], &parallelism); err != nil {
		t.Fatalf("decode parallelism failed: %v", err)
	}
	if parallelism != 3 {
		t.Fatalf("unexpected parallelism value: %d", parallelism)
	}

	var prepayment uint64
	if err := json.Unmarshal(payload["prepayment"], &prepayment); err != nil {
		t.Fatalf("decode prepayment failed: %v", err)
	}
	if prepayment != 99 {
		t.Fatalf("unexpected prepayment value: %d", prepayment)
	}

	var conflicts []uint64
	if err := json.Unmarshal(payload["conflictPeers"], &conflicts); err != nil {
		t.Fatalf("decode conflictPeers failed: %v", err)
	}
	if len(conflicts) != 3 || conflicts[0] != 7 || conflicts[1] != 8 || conflicts[2] != 9 {
		t.Fatalf("unexpected conflictPeers payload: %v", conflicts)
	}

	var idPayload map[string]json.RawMessage
	if err := json.Unmarshal(payload["id"], &idPayload); err != nil {
		t.Fatalf("decode id failed: %v", err)
	}

	var tx uint64
	if err := json.Unmarshal(idPayload["Tx"], &tx); err != nil {
		t.Fatalf("decode Tx failed: %v", err)
	}
	if tx != 42 {
		t.Fatalf("unexpected Tx value: %d", tx)
	}

	var address string
	if err := json.Unmarshal(idPayload["Address"], &address); err != nil {
		t.Fatalf("decode Address failed: %v", err)
	}
	if ethcommon.HexToAddress(address) != addr {
		t.Fatalf("unexpected Address value: got %s want %s", address, addr.Hex())
	}

	var uid uint64
	if err := json.Unmarshal(idPayload["UID"], &uid); err != nil {
		t.Fatalf("decode UID failed: %v", err)
	}
	expectedUID := statecommon.DeriveEthCalleeUID(addr, selector)
	if uid != expectedUID {
		t.Fatalf("unexpected UID value: %d", uid)
	}

	var selectorPayload []uint8
	if err := json.Unmarshal(idPayload["Selector"], &selectorPayload); err != nil {
		t.Fatalf("decode Selector failed: %v", err)
	}
	if len(selectorPayload) != len(selector) {
		t.Fatalf("unexpected selector length: %d", len(selectorPayload))
	}
	if !slices.Equal(selectorPayload, selector[:]) {
		t.Fatalf("unexpected selector payload: %v", selectorPayload)
	}
}
