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
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/arcology-network/common-lib/crdt/commutative"
	"github.com/arcology-network/common-lib/crdt/noncommutative"
	"github.com/arcology-network/common-lib/crdt/statecell"
	commontypes "github.com/arcology-network/common-lib/types"
)

func newNonceTestNormalizer(sender [20]byte) *TransactionNormalizer {
	return NewTransactionNormalizer(0, [20]byte{}, &commontypes.TransactionView{
		From:     sender,
		GasPrice: big.NewInt(0),
	})
}

func newNonceTestTransition(sender [20]byte, delta uint64) *statecell.StateCell {
	return statecell.NewStateCell(
		0,
		"blcc:/"+hex.EncodeToString(sender[:])+"/nonce",
		0,
		0,
		1,
		commutative.NewUint64Delta(delta),
		nil,
	)
}

// An independent blockchain transaction owns its envelope nonce increment.
// The transition remains raw and is also marked conflict-immune.
func TestNormalizeIndependentExecutionKeepsSenderNonce(t *testing.T) {
	sender := [20]byte{1}
	nonce := newNonceTestTransition(sender, 1)

	raw, immuned := newNonceTestNormalizer(sender).Normalize(
		nil,
		[]*statecell.StateCell{nonce},
		IndependentExecution,
	)

	if len(raw) != 1 || raw[0] != nonce {
		t.Fatalf("independent execution lost its sender nonce transition")
	}
	if len(immuned) != 1 || immuned[0] != nonce {
		t.Fatalf("independent sender nonce was not made conflict-immune")
	}
	if !nonce.Property.IfSkipConflictCheck() {
		t.Fatalf("independent sender nonce still participates in conflict checks")
	}
}

// Internal subwork is a nested execution branch. It must not consume an
// additional transaction-envelope nonce, while unrelated records stay intact.
func TestNormalizeInternalSubworkRemovesSenderNonceIncrement(t *testing.T) {
	sender := [20]byte{1}
	nonce := newNonceTestTransition(sender, 1)
	other := statecell.NewStateCell(
		0,
		"blcc:/unrelated/value",
		0,
		1,
		0,
		noncommutative.NewString("value"),
		nil,
	)

	raw, immuned := newNonceTestNormalizer(sender).Normalize(
		nil,
		[]*statecell.StateCell{nonce, other},
		InternalSubworkExecution,
	)

	if len(raw) != 1 || raw[0] != other {
		t.Fatalf("internal normalization removed or retained the wrong transition")
	}
	if len(immuned) != 0 {
		t.Fatalf("internal sender nonce must not become conflict-immune")
	}
}
