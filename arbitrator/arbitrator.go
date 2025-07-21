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

	common "github.com/arcology-network/common-lib/common"
	mapi "github.com/arcology-network/common-lib/exp/map"
	"github.com/arcology-network/common-lib/exp/slice"
	univalue "github.com/arcology-network/storage-committer/type/univalue"
	"golang.org/x/exp/maps"
)

type Arbitrator struct {
	dict      map[string][]*univalue.Univalue // Using any instead of []*univalue.Univalue is because most of time the there is only one element.
	wildcards *Wildcard                       // Wildcard elements, which are used to replace the original elements.

}

func NewArbitrator() *Arbitrator {
	return &Arbitrator{
		dict:      make(map[string][]*univalue.Univalue),
		wildcards: NewWildcard(),
	}
}

func (this *Arbitrator) Insert(newTrans []*univalue.Univalue) int {
	newTrans = this.wildcards.Filter(newTrans) // Filter the wildcards out.

	for i, tran := range newTrans {
		if vArr, ok := this.dict[*newTrans[i].GetPath()]; !ok {
			this.dict[*newTrans[i].GetPath()] = []*univalue.Univalue{newTrans[i]} // First time insert, using the element itself to save memory.
		} else {
			this.dict[*newTrans[i].GetPath()] = append(vArr, tran)
		}
	}
	return len(this.dict)
}

func (this *Arbitrator) Detect() []*Conflict {
	tranSet := mapi.Values(this.dict)
	for _, trans := range tranSet {
		this.wildcards.Substitute(trans) // Insert the wildcards into the transition set.
	}

	keys := maps.Keys(this.dict)
	conflists := make([]*Conflict, len(keys))
	slice.ParallelForeach(keys, 8, func(i int, k *string) {
		if vArr, ok := this.dict[*k]; ok && common.IsType[[]*univalue.Univalue](vArr) {
			conflists[i] = this.LookupForConflict(vArr)
		}
	})
	return slice.Remove(&conflists, nil)
}

func (this *Arbitrator) LookupForConflict(newTrans []*univalue.Univalue) *Conflict {
	univalue.Univalues(newTrans).SortByTx()

	first := newTrans[0]
	subTrans := newTrans[1:]

	var err error
	var offset int
	if first.IsReadOnly() { // Read only
		offset, _ = slice.FindFirstIf(subTrans, func(_ int, v *univalue.Univalue) bool { return !v.IsReadOnly() })
		err = errors.New("read with non read only")
	} else if first.IsCumulativeWriteOnly(first) { // Initialization of commutative values only
		offset, _ = slice.FindFirstIf(subTrans, func(_ int, v *univalue.Univalue) bool { return !v.IsCumulativeWriteOnly(first) })
		err = errors.New("Commutative Initialization with non commutative initialization")
	} else if first.IsDeltaWriteOnly() { // Delta write only
		offset, _ = slice.FindFirstIf(subTrans, func(_ int, v *univalue.Univalue) bool { return !v.IsDeltaWriteOnly() })
		err = errors.New("Delta write with non delta write only")
	} else if first.IsDeleteOnly() { // Delta write only
		offset, _ = slice.FindFirstIf(subTrans, func(_ int, v *univalue.Univalue) bool { return !v.IsDeleteOnly() })
		err = errors.New("Delete with non delete only")
	} else if first.IsNilInitOnly() { // Initialization with nil only.
		offset, _ = slice.FindFirstIf(subTrans, func(_ int, v *univalue.Univalue) bool { return !v.IsNilInitOnly() })
		err = errors.New("Nil initialization with non nil initialization")
	}

	if offset < 0 {
		return (&Accumulator{}).CheckMinMax(newTrans)
	}

	if outOfLimit := (&Accumulator{}).CheckMinMax(newTrans[:offset]); outOfLimit != nil {
		return outOfLimit
	}

	offset++ // The offet is actually the index of the origina index minus 1, because the first was used as the reference. Here we add it back.
	return &Conflict{
		key:           *newTrans[0].GetPath(),
		self:          newTrans[0].GetTx(),
		selfTran:      newTrans[0],
		sequenceID:    slice.Transform(newTrans[offset:], func(_ int, v *univalue.Univalue) uint64 { return v.GetSequence() }),
		conflictTrans: newTrans[offset:],
		txIDs:         slice.Transform(newTrans[offset:], func(_ int, v *univalue.Univalue) uint64 { return (*v).GetTx() }),
		Err:           err,
	}
}

func (this *Arbitrator) Clear() {
	clear(this.dict)
}

// Test function
func (this *Arbitrator) InsertAndDetect(sequenceIDs []uint64, newTrans []*univalue.Univalue) []*Conflict {
	for i, _ := range newTrans {
		newTrans[i].SetSequence(sequenceIDs[i])
	}

	this.Insert(newTrans)
	return this.Detect()
}
