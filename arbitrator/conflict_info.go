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

	statecell "github.com/arcology-network/common-lib/crdt/statecell"
	"github.com/arcology-network/common-lib/exp/slice"
)

// Conflict represents a detected write or state conflict between transactions
// during parallel execution.
type Conflict struct {
	self   *statecell.StateCell   // The current transaction.
	peers  []*statecell.StateCell // The conflicting transactions.
	Reason error                  // Why the conflict happens.
}

// GetRevertTxIDs returns the unique transaction IDs of all conflicting
// peer transactions that must be reverted to resolve this conflict.
func (this *Conflict) GetRevertTxIDs() []uint64 {
	return slice.Transform(this.peers, func(_ int, v *statecell.StateCell) uint64 {
		return v.GetTx()
	})
}

// GetConflictJobSeqences returns the unique job sequence IDs of all conflicting.
// peer transactions involved in this conflict.
func (this *Conflict) GetConflictJobSeqences() []uint64 {
	return slice.Transform(this.peers, func(_ int, v *statecell.StateCell) uint64 {
		return v.GetSequence()
	})
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
type Conflicts []*Conflict

// ToDict aggregates conflict statistics:
//
// 1. txDict: number of conflicts per transaction ID
// 2. numConflicts: number of conflicts per job sequence ID
// 3. Unique conflict transaction IDs. These transactions will be reverted.
func (this Conflicts) CollectConflictMetrics() (map[uint64]uint64, map[uint64]uint64) {
	byTX := make(map[uint64]uint64)     // The number of conflicts per transaction.
	byJobSeq := make(map[uint64]uint64) // The number of conflicts per job sequence.
	for _, v := range this {
		peerTxIDs := v.GetRevertTxIDs()
		slice.Foreach(peerTxIDs, func(_ int, v *uint64) {
			byTX[*v]++
			byJobSeq[*v]++
		})
	}
	return byTX, byJobSeq
}

func (this Conflicts) Print() {
	for _, v := range this {
		v.Print()
		fmt.Println()
	}
}
