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
	"github.com/arcology-network/common-lib/exp/slice"
	univalue "github.com/arcology-network/storage-committer/type/univalue"
	"golang.org/x/exp/maps"
)

type Arbitrator struct {
	dict map[string]any
}

func NewArbitrator() *Arbitrator {
	return &Arbitrator{
		dict: make(map[string]any),
	}
}

func (this *Arbitrator) Insert(sequenceIDs []uint64, newTrans []*univalue.Univalue) int {
	for i, tran := range newTrans {
		tran.Setsequence(sequenceIDs[i])
		if vArr, ok := this.dict[*newTrans[i].GetPath()]; !ok {
			this.dict[*newTrans[i].GetPath()] = newTrans[i]
		} else {
			if common.IsType[*univalue.Univalue](vArr) {
				this.dict[*newTrans[i].GetPath()] = []*univalue.Univalue{vArr.(*univalue.Univalue), tran}
				continue
			}
			this.dict[*newTrans[i].GetPath()] = append(vArr.([]*univalue.Univalue), tran)
		}
	}
	return len(this.dict)
}

func (this *Arbitrator) Detect() []*Conflict {
	keys := maps.Keys(this.dict)
	conflists := make([]*Conflict, len(keys))
	slice.ParallelForeach(keys, 8, func(i int, k *string) {
		if vArr, ok := this.dict[*k]; ok && !common.IsType[*univalue.Univalue](vArr) {
			conflists[i] = this.LookupForConflict(vArr.([]*univalue.Univalue))
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
		sequenceID:    slice.Transform(newTrans[offset:], func(_ int, v *univalue.Univalue) uint64 { return v.Getsequence() }),
		conflictTrans: newTrans[offset:],
		txIDs:         slice.Transform(newTrans[offset:], func(_ int, v *univalue.Univalue) uint64 { return (*v).GetTx() }),
		Err:           err,
	}
}

func (this *Arbitrator) Clear() {
	clear(this.dict)
}

func (this *Arbitrator) InsertAndDetect(sequenceIDs []uint64, newTrans []*univalue.Univalue) []*Conflict {
	this.Insert(sequenceIDs, newTrans)
	return this.Detect()
}
