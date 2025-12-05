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
	"errors"

	statecell "github.com/arcology-network/common-lib/crdt/statecell"
	mapi "github.com/arcology-network/common-lib/exp/map"
	"github.com/arcology-network/common-lib/exp/slice"
	"golang.org/x/exp/maps"
)

type Arbitrator struct {
	dict      map[string]*[]*statecell.StateCell // Using any instead of []*statecell.StateCell is because most of time the there is only one element.
	wildcards *Wildcard                          // Wildcard elements, which are used to replace the original elements.

}

func NewArbitrator() *Arbitrator {
	return &Arbitrator{
		dict:      make(map[string]*[]*statecell.StateCell),
		wildcards: NewWildcard(),
	}
}

func (this *Arbitrator) Insert(trans []*statecell.StateCell) int {
	trans = this.wildcards.Filter(trans) // Filter the wildcards out.
	for i, tran := range trans {
		if vArr, ok := this.dict[*trans[i].GetPath()]; !ok {
			this.dict[*trans[i].GetPath()] = &([]*statecell.StateCell{trans[i]}) // First time insert, using the element itself to save memory.
		} else {
			*vArr = append(*vArr, tran)
		}
	}
	return len(this.dict)
}

func (this *Arbitrator) Detect() []*Conflict {
	tranSet := mapi.Values(this.dict)
	for _, trans := range tranSet {
		// Insert the wildcards into the transition set before detection.
		this.wildcards.Expand(trans)
	}

	keys := maps.Keys(this.dict)
	conflists := make([]*Conflict, len(keys))

	// Search for conflicts in parallel within each key.
	slice.ParallelForeach(keys, 8, func(i int, k *string) {
		if vArr, ok := this.dict[*k]; ok && len(*vArr) > 1 {
			conflists[i], _ = this.LookupForConflict(*vArr)
		}
	})
	return slice.Remove(&conflists, nil)
}

func (this *Arbitrator) Move(trans []*statecell.StateCell) []*statecell.StateCell {
	slice.Foreach(trans, func(i int, v **statecell.StateCell) { (*v).IsInConflict = !(*v).IsReadOnly() })
	return slice.MoveIf(&trans, func(i int, v *statecell.StateCell) bool { return v.IsInConflict })
}

// Looks for conflicts in the array with the same path key.
func (this *Arbitrator) LookupForConflict(trans []*statecell.StateCell) (*Conflict, error) {
	statecell.StateCells(trans).SortByTx()

	first := trans[0]
	otherTrans := trans[1:]

	// Assume all the transitions are in conflict at the beginning.
	// Unless proven otherwise, we will return all the subsequent
	// transitions as conflicts.
	var conflictPeers []*statecell.StateCell
	var err error
	if first.IsReadOnly() { // Read only
		slice.Foreach(otherTrans, func(i int, v **statecell.StateCell) { (*v).IsInConflict = !(*v).IsReadOnly() })
		conflictPeers = slice.MoveIf(&otherTrans, func(i int, v *statecell.StateCell) bool { return v.IsInConflict })
		err = errors.New("Read with non read only")
	} else if first.IsCumulativeWriteOnly(first) { // Initialization of commutative values only
		slice.Foreach(otherTrans, func(i int, v **statecell.StateCell) { (*v).IsInConflict = !(*v).IsCumulativeWriteOnly(first) })
		conflictPeers = slice.MoveIf(&otherTrans, func(i int, v *statecell.StateCell) bool { return v.IsInConflict })
		err = errors.New("Commutative Initialization with non commutative initialization")

	} else if first.IsDeltaWriteOnly() { // Delta write only
		slice.Foreach(otherTrans, func(i int, v **statecell.StateCell) { (*v).IsInConflict = !(*v).IsDeltaWriteOnly() })
		conflictPeers = slice.MoveIf(&otherTrans, func(i int, v *statecell.StateCell) bool { return v.IsInConflict })
		err = errors.New("Delta write with non delta write only")

	} else if first.IsDeleteOnly() { // Delta write only
		slice.Foreach(otherTrans, func(i int, v **statecell.StateCell) { (*v).IsInConflict = !(*v).IsDeleteOnly() })
		conflictPeers = slice.MoveIf(&otherTrans, func(i int, v *statecell.StateCell) bool { return v.IsInConflict })
		err = errors.New("Delete with non delete only")

	} else if first.IsNilInitOnly() { // Initialization with nil only.
		slice.Foreach(otherTrans, func(i int, v **statecell.StateCell) { (*v).IsInConflict = !(*v).IsNilInitOnly() })
		conflictPeers = slice.MoveIf(&otherTrans, func(i int, v *statecell.StateCell) bool { return v.IsInConflict })
		err = errors.New("Nil initialization with non nil initialization")
	} else {
		// The first transition doesn't belong to any `special` category that can avoid at least some conflicts.
		// Thus, we mark all the subsequent transitions as conflicts.
		conflictPeers = otherTrans
		otherTrans = []*statecell.StateCell{}
	}

	// No access conflict found, move on to check the under/over limit conflicts.
	if len(conflictPeers) == 0 {
		return (&Accumulator{}).CheckMinMax(trans), nil
	}

	conflictFree := slice.PushFront(first, &otherTrans)
	if outOfLimit := (&Accumulator{}).CheckMinMax(conflictFree); outOfLimit != nil {
		return outOfLimit, nil
	}

	// offset++ // The offet is actually the index of the origina index minus 1, because the first was used as the reference. Here we add it back.
	return &Conflict{
		self:  trans[0],
		peers: conflictPeers,
	}, err
}

func (this *Arbitrator) Clear() {
	clear(this.dict)
}

// Test function
func (this *Arbitrator) InsertAndDetect(sequenceIDs []uint64, trans []*statecell.StateCell) []*Conflict {
	for i := range trans {
		trans[i].SetSequence(sequenceIDs[i])
	}

	this.Insert(trans)
	return this.Detect()
}
