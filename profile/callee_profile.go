/*
 *   Copyright (c) 2025 Arcology Network

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

package profile

import (
	"errors"
	"slices"

	//  "github.com/arcology-network/scheduler"

	"github.com/arcology-network/common-lib/codec"
	stgcommon "github.com/arcology-network/storage-committer/common"
	"github.com/arcology-network/storage-committer/type/noncommutative"
)

// The callee struct stores the information of a contract function that is called by the EOA initiated transactions.
// It is mainly used to optimize the execution of the transactions. A callee is uniquely identified by a
// combination of the contract's address and the function signature.
type Callee struct {
	LastVisit uint64 `json:"lastVisit"` // Last visit block height

	UID      uint64   `json:"uid"`      // Unique identifier of the callee (derived from address + selector)
	Contract [20]byte `json:"contract"` // Contract address
	Selector [4]byte  `json:"selector"` // Function selector

	// Only need to load these three fields from the storage
	ParallelismDegree uint32   `json:"parallelismDegree"` // Execution parallelism, 1 for sequential, otherwise parallel.
	IsDeferrable      bool     `json:"deferredPayment"`   // Required prepayment amount for the deferrable functions
	ConflictWith      []uint64 `json:"conflictWith"`      // ConflictWith of the conflicting callee indices.
	Dirty             bool     `json:"Dirty"`             // Whether the conflicts in callee profile has been modified.
}

func (this *Callee) SortConflicts() { slices.Sort(this.ConflictWith) } // Sort the callees by the indices in ascending order.

// Determine whether this callee is in conflict with another callee.
func (this *Callee) IsInConflict(other *Callee) bool {
	return slices.IndexFunc(this.ConflictWith, func(i uint64) bool { return i == uint64(other.UID) }) != -1
}

// Initialize the callee profile from the storage if exists.
func NewCalleeFromStorage(pathBuiler *stgcommon.PathBuilder, schStorage *SchedulerStorage) *Callee {
	this := &Callee{}

	// UID for quick matching
	this.Contract = pathBuiler.Address
	this.Selector = pathBuiler.Selector
	this.UID = DeriveUID(pathBuiler.Address[:], pathBuiler.Selector[:])

	// Get the parallelism degree
	path := pathBuiler.UnderCalleeProfile(stgcommon.PARALLELISM_DEGREE)
	if v, err := schStorage.Retrive(path, uint64(0)); err == nil {
		this.ParallelismDegree = v.(uint32)
	}

	// Get the minimum prepayment amount for deferred execution
	// If the amount is zero, it means the function is not deferrable.
	path = pathBuiler.UnderCalleeProfile(stgcommon.DEFERRED_PAYMENT)
	if prepayment, err := schStorage.Retrive(path, uint64(0)); err == nil {
		this.IsDeferrable = prepayment.(uint64) > 0
	}

	// Get the parallelism degree
	path = pathBuiler.UnderCalleeProfile(stgcommon.PARALLELISM_DEGREE)
	if Indices, err := schStorage.Retrive(path, []byte{}); err == nil {
		buffer := Indices.([]byte)
		this.ConflictWith = codec.Uint64s{}.Decode(buffer).(codec.Uint64s)
	}
	return this
}

func (this *Callee) Save(schStorage *SchedulerStorage) error {
	if !this.Dirty {
		return nil
	}
	var err error
	pathBuiler := stgcommon.PathBuilder{Address: this.Contract, Selector: this.Selector, Platform: stgcommon.ETH_PATH}

	// Mark as sequential if too many conflicts.
	if len(this.ConflictWith) > stgcommon.MAX_NUM_CONFLICTS {
		this.ParallelismDegree = 1
		this.ConflictWith = []uint64{} // Clear the conflict list since it is no longer needed.

		path := pathBuiler.UnderCalleeProfile(stgcommon.PARALLELISM_DEGREE)
		v := noncommutative.NewUint64(uint64(this.ParallelismDegree))
		saveErr := schStorage.Write(path, v) // Save the parallelism degree
		return errors.Join(err, saveErr)
	}

	// Still within the conflict limit, save the conflict list.
	path := pathBuiler.UnderCalleeProfile(stgcommon.CONFLICT_INFO_PATH)
	v := codec.Uint64s(this.ConflictWith).Encode()
	return schStorage.Write(path, noncommutative.NewBytes(v)) // Save the conflict list
}
