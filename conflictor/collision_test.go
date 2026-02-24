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

package conflictor

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	statecell "github.com/arcology-network/common-lib/crdt/statecell"
)

func marshalToJSONMap(t *testing.T, value any) map[string]json.RawMessage {
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

func buildTestConflict(t *testing.T) *Collision {
	t.Helper()

	cells := []*statecell.StateCell{
		statecell.NewStateCell(1, "blcc://state/0", 0, 1, 0, nil, nil),
		statecell.NewStateCell(2, "blcc://state/1", 0, 1, 0, nil, nil),
		statecell.NewStateCell(3, "blcc://state/2", 0, 1, 0, nil, nil),
	}

	for idx, cell := range cells {
		cell.JobSequenceID = uint64(idx)
	}

	return &Collision{
		Self:   cells[0],
		Peers:  cells[1:],
		Reason: errors.New("test reason"),
	}
}

func TestConflictJSONSchema(t *testing.T) {
	conflict := buildTestConflict(t)

	payload := marshalToJSONMap(t, conflict)

	required := []string{"self", "peers", "reason"}
	for _, key := range required {
		if _, found := payload[key]; !found {
			t.Fatalf("missing %q in marshalled payload", key)
		}
	}

	var reason string
	if err := json.Unmarshal(payload["reason"], &reason); err != nil {
		t.Fatalf("unexpected reason decoding error: %v", err)
	}
	if reason != "test reason" {
		t.Fatalf("unexpected reason value: %s", reason)
	}

	var peers []map[string]any
	if err := json.Unmarshal(payload["peers"], &peers); err != nil {
		t.Fatalf("decode peers failed: %v", err)
	}
	if len(peers) != 2 {
		t.Fatalf("unexpected peer count: %d", len(peers))
	}

	var self map[string]any
	if err := json.Unmarshal(payload["self"], &self); err != nil {
		t.Fatalf("decode self failed: %v", err)
	}
	if self == nil {
		t.Fatal("self entry should not be nil")
	}
}

func TestConflictsJSONSchema(t *testing.T) {
	conflict := buildTestConflict(t)

	bundle := &CollisionSummary{
		Collisions:       []*Collision{conflict},
		RevertTxLookup:   map[uint64]error{conflict.Peers[0].GetTx(): errors.New("peer reverted")},
		RevertSeqLookup:  map[uint64]uint64{conflict.Peers[0].JobSequenceID: 1},
		CollisionFreeTxs: []uint64{conflict.Self.GetTx()},
	}

	payload := marshalToJSONMap(t, bundle)

	required := []string{"conflicts", "revertTxLookup", "revertSeqLookup", "cleared"}
	for _, key := range required {
		if _, found := payload[key]; !found {
			t.Fatalf("missing %q in marshalled bundle", key)
		}
	}

	var conflictsArray []map[string]json.RawMessage
	if err := json.Unmarshal(payload["conflicts"], &conflictsArray); err != nil {
		t.Fatalf("decode conflicts failed: %v", err)
	}
	if len(conflictsArray) != 1 {
		t.Fatalf("unexpected conflicts count: %d", len(conflictsArray))
	}

	nested := conflictsArray[0]
	for _, key := range []string{"self", "peers", "reason"} {
		if _, found := nested[key]; !found {
			t.Fatalf("missing %q in nested conflict", key)
		}
	}

	var revertTx map[string]string
	if err := json.Unmarshal(payload["revertTxLookup"], &revertTx); err != nil {
		t.Fatalf("decode revertTxLookup failed: %v", err)
	}
	expectedKey := fmt.Sprintf("%d", conflict.Peers[0].GetTx())
	if revertTx[expectedKey] != "peer reverted" {
		t.Fatalf("unexpected revertTx value: %v", revertTx[expectedKey])
	}

	var revertSeq map[string]uint64
	if err := json.Unmarshal(payload["revertSeqLookup"], &revertSeq); err != nil {
		t.Fatalf("decode revertSeqLookup failed: %v", err)
	}
	expectedSeqKey := fmt.Sprintf("%d", conflict.Peers[0].JobSequenceID)
	if revertSeq[expectedSeqKey] != 1 {
		t.Fatalf("unexpected revert sequence count: %d", revertSeq[expectedSeqKey])
	}

	var cleared []uint64
	if err := json.Unmarshal(payload["cleared"], &cleared); err != nil {
		t.Fatalf("decode cleared failed: %v", err)
	}
	if len(cleared) != 1 || cleared[0] != conflict.Self.GetTx() {
		t.Fatalf("unexpected cleared payload: %v", cleared)
	}
}
