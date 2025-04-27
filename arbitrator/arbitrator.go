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
)

type Arbitrator struct {
	groupIDs    []uint64
	transitions []*univalue.Univalue
}

func (this *Arbitrator) Insert(groupIDs []uint64, newTrans []*univalue.Univalue) int {
	this.transitions = append(this.transitions, newTrans...)
	this.groupIDs = append(this.groupIDs, groupIDs...)
	return len(this.groupIDs)
}

func (this *Arbitrator) Detect(groupIDs []uint64, newTrans []*univalue.Univalue) []*Conflict {
	if this.Insert(groupIDs, newTrans) == 0 {
		return []*Conflict{}
	}

	// t0 := time.Now()
	univalue.Univalues(newTrans).Sort(groupIDs)

	ranges := slice.FindAllIndics(newTrans, func(lhv, rhv *univalue.Univalue) bool {
		return *lhv.GetPath() == *rhv.GetPath()
	})

	conflicts := []*Conflict{}
	for i := 0; i < len(ranges)-1; i++ {
		if ranges[i]+1 == ranges[i+1] {
			continue // Only one entry
		}

		first := newTrans[ranges[i]]
		subTrans := newTrans[ranges[i]+1 : ranges[i+1]]

		var err error
		var offset int
		if first.IsReadOnly() { // Read only
			offset, _ = slice.FindFirstIf(subTrans, func(_ int, v *univalue.Univalue) bool { return !v.IsReadOnly() })
			err = errors.New("read with non read only")
		} else if first.IsCommutativeInitOrWriteOnly(first) { // Initialization of commutative values only
			offset, _ = slice.FindFirstIf(subTrans, func(_ int, v *univalue.Univalue) bool { return !v.IsCommutativeInitOrWriteOnly(first) })
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

		if offset <= 0 {
			offset = common.IfThen(offset < 0, ranges[i+1]-ranges[i], offset+1) // ONLY offset == -1 means no conflict found
		}

		if ranges[i]+offset == ranges[i+1] {
			continue
		}

		conflictTxs := []uint64{}
		slice.Foreach(newTrans[ranges[i]+offset:ranges[i+1]], func(_ int, v **univalue.Univalue) {
			conflictTxs = append(conflictTxs, (*v).GetTx())
		})

		conflicts = append(conflicts,
			&Conflict{
				key:           *newTrans[ranges[i]].GetPath(),
				self:          newTrans[ranges[i]].GetTx(),
				selfTran:      newTrans[ranges[i]],
				groupID:       groupIDs[ranges[i]+offset : ranges[i+1]],
				conflictTrans: newTrans[ranges[i]+offset : ranges[i+1]],
				txIDs:         conflictTxs,
				Err:           err,
			},
		)

		// Check if the cumulative value is out of limits
		if len(conflicts) > 0 {
			if newTrans[ranges[i]].Writes() == 0 {
				if newTrans[ranges[i]].IsDeltaWriteOnly() { // Delta write only
					offset, _ = slice.FindFirstIf(newTrans[ranges[i]+1:ranges[i+1]],
						func(_ int, v *univalue.Univalue) bool {
							return !v.IsDeltaWriteOnly()
						})
				} else { // Read only
					offset, _ = slice.FindFirstIf(newTrans[ranges[i]+1:ranges[i+1]],
						func(_ int, v *univalue.Univalue) bool {
							return v.Writes() > 0 || v.DeltaWrites() > 0
						})
				}
				offset = common.IfThen(offset < 0, ranges[i+1]-ranges[i], offset+1) // offset == -1 means no conflict found
			}
		}

		dict := common.MapFromSlice(conflictTxs, true) //Conflict dict
		trans := slice.CopyIf(newTrans[ranges[i]+offset:ranges[i+1]], func(_ int, v *univalue.Univalue) bool { return (*dict)[v.GetTx()] })

		if outOfLimits := (&Accumulator{}).CheckMinMax(trans); outOfLimits != nil {
			conflicts = append(conflicts, outOfLimits...)
		}
	}

	if len(conflicts) > 0 {
		Conflicts(conflicts).Print()
	}
	return conflicts
}
