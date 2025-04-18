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
	intf "github.com/arcology-network/storage-committer/common"
	stgcommon "github.com/arcology-network/storage-committer/common"
	univalue "github.com/arcology-network/storage-committer/type/univalue"
	"github.com/holiman/uint256"
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

		offset := int(1)
		subTrans := newTrans[ranges[i]+1 : ranges[i+1]]
		if newTrans[ranges[i]].Writes() == 0 {
			// Whyen write == 0, there are only 2 possibilities:
			// 1. delta write only
			// 2. read only
			if newTrans[ranges[i]].IsReadOnly() || newTrans[ranges[i]].IsDeltaWriteOnly() || newTrans[ranges[i]].IsDeleteOnly() || newTrans[ranges[i]].IsCommutativeWriteOnly() {

				// To skip the numeric commutative writes as long as there is no reads.
				if newTrans[ranges[i]].IsCommutativeWriteOnly() {
					offset, _ = slice.FindFirstIf(subTrans, func(_ int, v *univalue.Univalue) bool { return !v.IsCommutativeWriteOnly() })
				}

				// Read only
				if newTrans[ranges[i]].IsReadOnly() {
					offset, _ = slice.FindFirstIf(subTrans, func(_ int, v *univalue.Univalue) bool { return !v.IsReadOnly() })
				}

				// Delta write only
				if newTrans[ranges[i]].IsDeltaWriteOnly() {
					offset, _ = slice.FindFirstIf(subTrans, func(_ int, v *univalue.Univalue) bool { return !v.IsDeltaWriteOnly() })
				}

				offset = common.IfThen(offset < 0, ranges[i+1]-ranges[i], offset+1) // offset == -1 means no conflict found
			}
		} else {
			// Idempotent write (delete only / new only, for now)
			if newTrans[ranges[i]].IsDeleteOnly() { // Delta write only
				offset, _ = slice.FindFirstIf(subTrans, func(_ int, v *univalue.Univalue) bool { return !v.IsDeleteOnly() })
				offset = common.IfThen(offset < 0, ranges[i+1]-ranges[i], offset+1) // offset == -1 means no conflict found
			}

			// This is a special treatment for commutative initialization only. A commutative initialization can be broken into
			// two operations: 1. Initialization 2. delta write. but No READ.
			// Two initializations aren't conflicting if they have the same min/max, because they are commutative.
			// two delta writes aren't conflicting either. So two initializations with inital values aren't conflicting either.
			// But if one of them is a read, then it is a conflict.
			if newTrans[ranges[i]].IsCommutativeWriteOnly() {
				offset, _ = slice.FindFirstIf(subTrans, func(_ int, v *univalue.Univalue) bool {
					flag := v.IsCommutativeWriteOnly()

					// If the input min/max is different, then it is a conflict.
					if flag {
						return newTrans[ranges[i]].Value().(intf.Type).Min().(uint256.Int) != v.Value().(intf.Type).Min().(uint256.Int) ||
							newTrans[ranges[i]].Value().(intf.Type).Max().(uint256.Int) != v.Value().(intf.Type).Max().(uint256.Int)
					}
					return false
				})
				offset = common.IfThen(offset < 0, ranges[i+1]-ranges[i], offset+1) // offset == -1 means no conflict found
			}
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
				key:     *newTrans[ranges[i]].GetPath(),
				self:    newTrans[ranges[i]].GetTx(),
				groupID: groupIDs[ranges[i]+offset : ranges[i+1]],
				txIDs:   conflictTxs,
				Err:     errors.New(stgcommon.WARN_ACCESS_CONFLICT),
			},
		)

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

	// if len(conflicts) > 0 {
	// 	fmt.Println("range: ", ranges)
	// }
	return conflicts
}
