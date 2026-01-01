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
	"fmt"
	"sort"

	statecell "github.com/arcology-network/common-lib/crdt/statecell"
	mapi "github.com/arcology-network/common-lib/exp/map"
	"github.com/arcology-network/common-lib/exp/slice"
	profile "github.com/arcology-network/scheduler/callee"
	"github.com/arcology-network/scheduler/workload"
)

// Conflict represents a detected write or state conflict between transactions
// during parallel execution.
type Conflict struct {
	self   *statecell.StateCell   // The current transaction.
	peers  []*statecell.StateCell // The conflicting transactions.
	Reason error                  // Why the conflict happens.
}

// GetRevertIDs returns the unique transaction IDs of all conflicting
// peer transactions that must be reverted to resolve this conflict.
func (this *Conflict) GetRevertTxIDs() []uint64 {
	return slice.Transform(this.peers, func(_ int, v *statecell.StateCell) uint64 {
		return v.GetTx()
	})
}

// GetConflictJobSeqences returns the unique job sequence IDs of all conflicting.
// peer transactions involved in this conflict.
func (this *Conflict) GetConflictJobSeqenceIDs() []uint64 {
	return slice.Transform(this.peers, func(_ int, v *statecell.StateCell) uint64 {
		return v.GetSequence()
	})
}

// Map the conflicting transactions to their corresponding message callee UIDs.
func (this *Conflict) MapConflictToCallee(jobLookup map[uint64]*workload.Job) (*profile.ID, []*profile.ID) {
	selfID := profile.NewID(jobLookup[this.self.GetTx()].StdMsg.GetAddressAndSelector())

	peerIDs := slice.Transform(this.peers, func(_ int, v *statecell.StateCell) *profile.ID {
		addr, selector := jobLookup[v.GetTx()].StdMsg.GetAddressAndSelector()
		return profile.NewID(addr, selector)
	})
	return selfID, peerIDs
}

func (this *Conflict) Equal(other *Conflict) bool {
	if !this.self.Equal(other.self) {
		return false
	}

	if len(this.peers) != len(other.peers) {
		return false
	}

	if !statecell.StateCells(this.peers).Equal(statecell.StateCells(other.peers)) {
		return false
	}
	return this.Reason.Error() == other.Reason.Error()
}

// Print outputs the conflicting state cells and the conflict reason
// to standard output for debugging.
func (this *Conflict) Print() {
	this.self.Print()
	fmt.Println(" ----- conflict with ----- ")

	trans := slice.Transform(this.peers, func(_ int, v *statecell.StateCell) *statecell.StateCell {
		return v
	})
	statecell.StateCells(trans).Print()
	fmt.Println("Reason: ", this.Reason)
}

// Conflicts is a collection of Conflict pointers.
type Conflicts struct {
	Conflicts       []*Conflict
	RevertTxLookup  map[uint64]bool
	RevertSeqLookup map[uint64]uint64
	Cleared         []uint64 // The IDs of the transactions that are cleared.
}

func NewConflicts(trans []*statecell.StateCell, conflicts []*Conflict) *Conflicts {
	revertTxLookup := make(map[uint64]bool) // Unique transaction IDs to revert.
	seqLookup := make(map[uint64]uint64)    // Unique job sequence IDs that contain the transactions to revert.
	for _, conflict := range conflicts {
		IDs := conflict.GetRevertTxIDs()

		// The IDs of all the conflicting transactions that share the same state cell and
		// affected by the conflict. They all need to be reverted.
		slice.Foreach(IDs, func(_ int, txId *uint64) {
			revertTxLookup[*txId] = true

			// Map to the job sequence IDs as well.
			for _, peer := range conflict.peers {
				seqLookup[peer.GetSequence()]++
			}
		})
	}

	// Get unique transaction IDs.
	uniquesTx := make(map[uint64]uint64)
	for _, tran := range trans {
		uniquesTx[tran.GetTx()]++
	}

	// Sort the transaction IDs and sequence IDs.
	txs := mapi.Keys(uniquesTx)
	sort.SliceStable(txs, func(i, j int) bool { return txs[i] < txs[j] }) // Sort the transaction IDs.

	return &Conflicts{
		Conflicts:       conflicts,
		RevertTxLookup:  revertTxLookup,
		RevertSeqLookup: seqLookup,
		Cleared:         slice.Exclude(slice.Clone(mapi.Keys(uniquesTx)), mapi.Keys(revertTxLookup)),
	}
}

func (this *Conflicts) IsEmpty() bool {
	return len(this.Conflicts) == 0
}

// Get the transaction IDs that need to be reverted.
func (this *Conflicts) GetRevertTxs() []uint64 {
	reverts := mapi.Keys(this.RevertTxLookup)
	sort.SliceStable(reverts, func(i, j int) bool { return reverts[i] < reverts[j] })
	return reverts
}

// Get conflicted free transaction IDs.
func (this *Conflicts) GetClearedTxs() []uint64 {
	clearTxs := make([]uint64, 0, len(this.Conflicts)-len(this.RevertTxLookup))
	for _, conflict := range this.Conflicts {
		if _, ok := this.RevertTxLookup[conflict.self.GetTx()]; !ok {
			clearTxs = append(clearTxs, conflict.self.GetTx())
		}
	}
	return clearTxs
}

func (this Conflicts) Print() {
	for _, v := range this.Conflicts {
		v.Print()
		fmt.Println()
	}
}
