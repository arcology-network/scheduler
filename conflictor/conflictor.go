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

package conflictor

import (
	"errors"

	statecell "github.com/arcology-network/common-lib/crdt/statecell"
	mapi "github.com/arcology-network/common-lib/exp/map"
	"github.com/arcology-network/common-lib/exp/slice"
	schedulercommon "github.com/arcology-network/scheduler/common"
	"golang.org/x/exp/maps"
)

type Conflictor struct {
	dict            map[string]*[]*statecell.StateCell // Using any instead of []*statecell.StateCell is because most of time the there is only one element.
	wildcards       *Wildcard                          // Wildcard elements, which are used to replace the original elements.
	transBySequence map[uint64][]*statecell.StateCell  // Transactions grouped by job sequence ID.
	uniqueTx        map[uint64]bool                    // Unique transaction IDs.
}

func NewConflictor() *Conflictor {
	return &Conflictor{
		dict:            make(map[string]*[]*statecell.StateCell),
		wildcards:       NewWildcard(),
		transBySequence: make(map[uint64][]*statecell.StateCell),
		uniqueTx:        make(map[uint64]bool),
	}
}

func (this *Conflictor) Insert(trans []*statecell.StateCell) int {
	trans = this.wildcards.Filter(trans) // Filter the wildcards out.
	for i, tran := range trans {
		if vArr, ok := this.dict[*trans[i].GetPath()]; !ok {
			this.dict[*trans[i].GetPath()] = &([]*statecell.StateCell{trans[i]}) // First time insert, using the element itself to save memory.
		} else {
			*vArr = append(*vArr, tran)
		}

		v := this.transBySequence[tran.JobSequenceID]
		this.transBySequence[tran.JobSequenceID] = append(v, tran)
		this.uniqueTx[tran.GetTx()] = true // Record the unique transaction ID.
	}
	return len(this.dict)
}

// Detects all the `DIRECT` conflicts in the inserted transactions.
func (this *Conflictor) Detect() ([]*Collision, []uint64, []uint64) {
	tranSet := mapi.Values(this.dict)
	for _, trans := range tranSet {
		// Insert the wildcards into the transition set before detection.
		this.wildcards.Expand(trans)
	}

	keys := maps.Keys(this.dict)
	collisions := make([]*Collision, len(keys))

	// Search for conflicts in parallel within each key.
	// for i, k := range keys {
	slice.ParallelForeach(keys, 8, func(i int, k *string) {
		if vArr, ok := this.dict[*k]; ok && len(*vArr) > 1 {
			collisions[i] = this.LookupForConflict(*vArr)
		}
	})

	// Get the direct collisions only.
	directCollisions := slice.Remove(&collisions, nil)

	// Collect all the transactions directly or indirectly affected by the conflicts.
	revertTxs := make(map[uint64]bool) // Transactions that need to be reverted.
	for _, collions := range directCollisions {
		for _, cell := range collions.Peers {
			transInSeq := this.transBySequence[cell.JobSequenceID] // Get the transactions in the same sequence.
			for _, tran := range transInSeq {
				// Add all the transactions after the conflicting one to the revert list.
				// Because they are all POTENTIALLY affected by the collision.
				if tran.JobID >= cell.JobID {
					revertTxs[tran.GetTx()] = true
				}
			}
		}
	}
	return directCollisions, // Detected direct collisions.
		mapi.Keys(revertTxs), // To be reverted transactions.
		mapi.Keys(mapi.Diff(this.uniqueTx, revertTxs)) // collision-free transactions.
}

func (this *Conflictor) Move(trans []*statecell.StateCell) []*statecell.StateCell {
	slice.Foreach(trans, func(i int, v **statecell.StateCell) { (*v).HasConflictWith = !(*v).IsReadOnly() })
	return slice.MoveIf(&trans, func(i int, v *statecell.StateCell) bool { return v.HasConflictWith })
}

// Looks for conflicts in the array with the same path key.
func (this *Conflictor) LookupForConflict(trans []*statecell.StateCell) *Collision {
	statecell.StateCells(trans).SortByTx()

	first := trans[0]
	// Different transactions inthe same sequence are not conflicting, even they access the same state cell.
	idx, _ := slice.FindFirstIf(trans, func(i int, v *statecell.StateCell) bool {
		return first.JobSequenceID != v.JobSequenceID
	})

	if idx == -1 {
		return nil // All the transitions are from the same sequence, no conflict.
	}

	otherTrans := trans[idx:]

	// Assume all the transitions are in conflict at the beginning.
	// Unless proven otherwise, we will return all the subsequent
	// transitions as conflicts.
	var conflictPeers []*statecell.StateCell
	var err error
	if first.IsReadOnly() { // Read only
		slice.Foreach(otherTrans, func(i int, v **statecell.StateCell) { (*v).HasConflictWith = !(*v).IsReadOnly() })
		conflictPeers = slice.MoveIf(&otherTrans, func(i int, v *statecell.StateCell) bool { return v.HasConflictWith })
		err = errors.New("Read with non read only")
	} else if first.IsCumulativeWriteOnly(first) { // Initialization of commutative values only
		slice.Foreach(otherTrans, func(i int, v **statecell.StateCell) { (*v).HasConflictWith = !(*v).IsCumulativeWriteOnly(first) })
		conflictPeers = slice.MoveIf(&otherTrans, func(i int, v *statecell.StateCell) bool { return v.HasConflictWith })
		err = errors.New("Commutative Initialization with non commutative initialization")

	} else if first.IsDeltaWriteOnly() { // Delta write only
		slice.Foreach(otherTrans, func(i int, v **statecell.StateCell) { (*v).HasConflictWith = !(*v).IsDeltaWriteOnly() })
		conflictPeers = slice.MoveIf(&otherTrans, func(i int, v *statecell.StateCell) bool { return v.HasConflictWith })
		err = errors.New("Delta write with non delta write only")

	} else if first.IsDeleteOnly() { // Delta write only
		slice.Foreach(otherTrans, func(i int, v **statecell.StateCell) { (*v).HasConflictWith = !(*v).IsDeleteOnly() })
		conflictPeers = slice.MoveIf(&otherTrans, func(i int, v *statecell.StateCell) bool { return v.HasConflictWith })
		err = errors.New("Delete with non delete only")

	} else if first.IsNilInitOnly() { // Initialization with nil only.
		slice.Foreach(otherTrans, func(i int, v **statecell.StateCell) { (*v).HasConflictWith = !(*v).IsNilInitOnly() })
		conflictPeers = slice.MoveIf(&otherTrans, func(i int, v *statecell.StateCell) bool { return v.HasConflictWith })
		err = errors.New("Nil initialization with non nil initialization")
	} else {
		// The first transition doesn't belong to any `special` category that can avoid at least some conflicts.
		// Thus, we mark all the subsequent transitions as conflicts.
		conflictPeers = otherTrans
		otherTrans = []*statecell.StateCell{}
	}

	// No access conflict found, move on to check the under/over limit conflicts.
	if len(conflictPeers) == 0 {
		return (&Accumulator{}).CheckMinMax(trans)
	}

	// There are some access conflicts, check if the remaining transitions are within limits.
	conflictFree := slice.PushFront(first, &otherTrans)
	if outOfLimit := (&Accumulator{}).CheckMinMax(conflictFree); outOfLimit != nil {
		return outOfLimit
	}

	// offset++ // The offet is actually the index of the origina index minus 1, because the first
	// was used as the reference. Here we add it back.
	return &Collision{
		Self:   trans[0],
		Peers:  conflictPeers,
		Reason: errors.Join(schedulercommon.WARN_ACCESS_CONFLICT, err),
	}
}

func (this *Conflictor) Clear() {
	clear(this.dict)
}

// Test function, the production version is doing insertion separately from detection.
func (this *Conflictor) DebugInsertAndDetect(trans []*statecell.StateCell) (*CollisionSummary, []uint64, []uint64) {
	this.Insert(trans)
	collision, txToRevert, txToRemain := this.Detect()
	return NewCollisionSummary(trans, collision), txToRevert, txToRemain
}
