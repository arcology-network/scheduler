/*
 *   Copyright (c) 2023 Arcology Network

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

package arbitrator

import (
	"encoding/json"
	"fmt"

	statecell "github.com/arcology-network/common-lib/crdt/statecell"
	"github.com/arcology-network/common-lib/exp/slice"
	profile "github.com/arcology-network/scheduler/callee"
	"github.com/arcology-network/scheduler/workload"
)

// Conflict represents a detected write or state conflict between transactions
// during parallel execution.
type Conflict struct {
	Self   *statecell.StateCell   `json:"self"`   // The current transaction.
	Peers  []*statecell.StateCell `json:"peers"`  // The conflicting transactions.
	Reason error                  `json:"reason"` // Why the conflict happens.
}

func (this *Conflict) MarshalJSON() ([]byte, error) {
	type conflictAlias struct {
		Self   *statecell.StateCell   `json:"self"`
		Peers  []*statecell.StateCell `json:"peers"`
		Reason string                 `json:"reason"`
	}

	alias := conflictAlias{Self: this.Self, Peers: this.Peers}
	if this.Reason != nil {
		alias.Reason = this.Reason.Error()
	}

	return json.Marshal(&alias)
}

// GetRevertIDs returns the unique transaction IDs of all conflicting
// peer transactions that must be reverted to resolve this conflict.
func (this *Conflict) GetRevertTxIDs() []uint64 {
	return slice.Transform(this.Peers, func(_ int, v *statecell.StateCell) uint64 {
		return v.GetTx()
	})
}

// GetConflictJobSeqences returns the unique job sequence IDs of all conflicting.
// peer transactions involved in this conflict.
func (this *Conflict) GetConflictJobSeqenceIDs() []uint64 {
	return slice.Transform(this.Peers, func(_ int, v *statecell.StateCell) uint64 {
		return v.GetSequence()
	})
}

// Map the conflicting transactions to their corresponding message callee UIDs.
func (this *Conflict) MapConflictToCallee(jobLookup map[uint64]*workload.Job) (*profile.ID, []*profile.ID) {
	addr, selector := jobLookup[this.Self.GetTx()].StdMsg.GetAddressAndSelector()
	selfID := profile.NewID(this.Self.GetTx(), addr, selector)

	peerIDs := slice.Transform(this.Peers, func(_ int, v *statecell.StateCell) *profile.ID {
		addr, selector := jobLookup[v.GetTx()].StdMsg.GetAddressAndSelector()
		return profile.NewID(v.GetTx(), addr, selector)
	})
	return selfID, peerIDs
}

func (this *Conflict) Equal(other *Conflict) bool {
	if !this.Self.Equal(other.Self) {
		return false
	}

	if len(this.Peers) != len(other.Peers) {
		return false
	}

	if !statecell.StateCells(this.Peers).Equal(statecell.StateCells(other.Peers)) {
		return false
	}
	return this.Reason.Error() == other.Reason.Error()
}

// Print outputs the conflicting state cells and the conflict reason
// to standard output for debugging.
func (this *Conflict) Print() {
	this.Self.Print()
	fmt.Println(" ----- conflict with ----- ")

	trans := slice.Transform(this.Peers, func(_ int, v *statecell.StateCell) *statecell.StateCell {
		return v
	})
	statecell.StateCells(trans).Print()
	fmt.Println("Reason: ", this.Reason)
}
