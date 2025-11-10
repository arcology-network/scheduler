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

	mapi "github.com/arcology-network/common-lib/exp/map"
	"github.com/arcology-network/common-lib/exp/slice"
	univalue "github.com/arcology-network/storage-committer/type/univalue"
	"golang.org/x/exp/maps"
)

type Arbitrator struct {
	dict      map[string]*[]*univalue.Univalue // Using any instead of []*univalue.Univalue is because most of time the there is only one element.
	wildcards *Wildcard                        // Wildcard elements, which are used to replace the original elements.

}

func NewArbitrator() *Arbitrator {
	return &Arbitrator{
		dict:      make(map[string]*[]*univalue.Univalue),
		wildcards: NewWildcard(),
	}
}

func (this *Arbitrator) Insert(trans []*univalue.Univalue) int {
	trans = this.wildcards.Filter(trans) // Filter the wildcards out.
	for i, tran := range trans {
		if vArr, ok := this.dict[*trans[i].GetPath()]; !ok {
			this.dict[*trans[i].GetPath()] = &([]*univalue.Univalue{trans[i]}) // First time insert, using the element itself to save memory.
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
			conflists[i] = this.LookupForConflict(*vArr)
		}
	})
	return slice.Remove(&conflists, nil)
}

func (this *Arbitrator) Move(trans []*univalue.Univalue) []*univalue.Univalue {
	slice.Foreach(trans, func(i int, v **univalue.Univalue) { (*v).IsInConflict = !(*v).IsReadOnly() })
	return slice.MoveIf(&trans, func(i int, v *univalue.Univalue) bool { return v.IsInConflict })
}

// Looks for conflicts in the array with the same path key.
func (this *Arbitrator) LookupForConflict(trans []*univalue.Univalue) *Conflict {
	univalue.Univalues(trans).SortByTx()

	first := trans[0]
	otherTrans := trans[1:]

	// Asume all the transitions are in conflict at the beginning.1
	// Unless proven otherwise, we will return all the transitions as conflicts.
	conflictWith := otherTrans
	var err error
	if first.IsReadOnly() { // Read only
		slice.Foreach(otherTrans, func(i int, v **univalue.Univalue) { (*v).IsInConflict = !(*v).IsReadOnly() })
		conflictWith = slice.MoveIf(&otherTrans, func(i int, v *univalue.Univalue) bool { return v.IsInConflict })
		err = errors.New("Read with non read only")
	} else if first.IsCumulativeWriteOnly(first) { // Initialization of commutative values only
		slice.Foreach(otherTrans, func(i int, v **univalue.Univalue) { (*v).IsInConflict = !(*v).IsCumulativeWriteOnly(first) })
		conflictWith = slice.MoveIf(&otherTrans, func(i int, v *univalue.Univalue) bool { return v.IsInConflict })
		err = errors.New("Commutative Initialization with non commutative initialization")

	} else if first.IsDeltaWriteOnly() { // Delta write only
		slice.Foreach(otherTrans, func(i int, v **univalue.Univalue) { (*v).IsInConflict = !(*v).IsDeltaWriteOnly() })
		conflictWith = slice.MoveIf(&otherTrans, func(i int, v *univalue.Univalue) bool { return v.IsInConflict })
		err = errors.New("Delta write with non delta write only")

	} else if first.IsDeleteOnly() { // Delta write only
		slice.Foreach(otherTrans, func(i int, v **univalue.Univalue) { (*v).IsInConflict = !(*v).IsDeleteOnly() })
		conflictWith = slice.MoveIf(&otherTrans, func(i int, v *univalue.Univalue) bool { return v.IsInConflict })
		err = errors.New("Delete with non delete only")

	} else if first.IsNilInitOnly() { // Initialization with nil only.
		slice.Foreach(otherTrans, func(i int, v **univalue.Univalue) { (*v).IsInConflict = !(*v).IsNilInitOnly() })
		conflictWith = slice.MoveIf(&otherTrans, func(i int, v *univalue.Univalue) bool { return v.IsInConflict })
		err = errors.New("Nil initialization with non nil initialization")
	}

	// No access conflict found, move on to check the under/over limit conflicts.
	if len(conflictWith) == 0 {
		return (&Accumulator{}).CheckMinMax(trans)
	}

	conflictFree := slice.PushFront(first, &otherTrans)
	if outOfLimit := (&Accumulator{}).CheckMinMax(conflictFree); outOfLimit != nil {
		return outOfLimit
	}

	// offset++ // The offet is actually the index of the origina index minus 1, because the first was used as the reference. Here we add it back.
	return &Conflict{
		key:          *trans[0].GetPath(),
		self:         trans[0].GetTx(),
		tran:         trans[0],
		sequenceID:   slice.Transform(conflictWith, func(_ int, v *univalue.Univalue) uint64 { return v.GetSequence() }),
		conflictWith: conflictWith,
		txIDs:        slice.Transform(conflictWith, func(_ int, v *univalue.Univalue) uint64 { return (*v).GetTx() }),
		Reason:       err,
	}
}

func (this *Arbitrator) Clear() {
	clear(this.dict)
}

// Test function
func (this *Arbitrator) InsertAndDetect(sequenceIDs []uint64, trans []*univalue.Univalue) []*Conflict {
	for i := range trans {
		trans[i].SetSequence(sequenceIDs[i])
	}

	this.Insert(trans)
	return this.Detect()
}
