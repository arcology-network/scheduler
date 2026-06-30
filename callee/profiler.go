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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unsafe"

	"github.com/arcology-network/common-lib/codec"
	commutative "github.com/arcology-network/common-lib/crdt/commutative"
	"github.com/arcology-network/common-lib/crdt/noncommutative"
	"github.com/arcology-network/common-lib/crdt/statecell"
	statecommon "github.com/arcology-network/state-engine/common"
)

//  "github.com/arcology-network/scheduler"

// The callee struct stores the information of a contract function that is called by the EOA initiated transactions.
// It is mainly used to optimize the execution of the transactions. A callee is uniquely identified by a
// combination of the contract's address and the function signature.
type Profile struct {
	ID *ID

	parallelismDegree uint64   // Execution parallelism, 1 for sequential, otherwise parallel.
	prepayment        uint64   // Required prepayment amount for the deferrable functions
	ConflictPeers     []uint64 // ConflictPeers of the conflicting callee indices.
	profileStore      *ProfileStore
}

func NewProfile(Tx uint64, addr [20]byte, selector [4]byte, store *ProfileStore) *Profile {
	// Get the unique ID for the callee.
	return &Profile{
		ID:           NewID(Tx, addr, selector),
		profileStore: store,
	}
}

// If the callee is supposed to be executed sequentially only.
func (this *Profile) IsSequentialOnly() bool {
	return this.parallelismDegree == 1
}

func (this *Profile) IsEmpty() bool {
	return this.parallelismDegree == 0 &&
		this.prepayment == 0 &&
		len(this.ConflictPeers) == 0
}

func (this *Profile) SetParallelismDegree(n uint64) {
	this.parallelismDegree = n
	this.profileStore.addToDirty(this)
}

func (this *Profile) SetPrepayment(prepayment uint64) {
	this.prepayment = prepayment
	this.profileStore.addToDirty(this)
}

// Determine whether this callee profile can be deferred for later execution.
func (this *Profile) IsDeferrable() bool { return this.prepayment > 0 }

func (this *Profile) CrossLink(other *Profile) {
	this.addConflictPeers([]uint64{other.ID.UID})
	other.addConflictPeers([]uint64{this.ID.UID})
}

func (this *Profile) addConflictPeers(list []uint64) {
	if len(this.ConflictPeers)+len(list) > statecommon.MAX_NUM_CONFLICTS {
		this.ConflictPeers = this.ConflictPeers[:0]
		this.parallelismDegree = 1 // Too many conflicts, mark as sequential only.
	} else {
		this.ConflictPeers = append(this.ConflictPeers, list...)
	}
	this.profileStore.addToDirty(this)
}

// Determine whether this callee profile already has the conflict with another callee profile.
func (this *Profile) IsMutuallyConflicting(other *Profile) bool {
	lft := this.HasConflictWith(other)
	rgt := other.HasConflictWith(this)

	if lft != rgt {
		panic("Conflict list inconsistent" + this.PrintToString() + other.PrintToString())
	}
	return lft && rgt
}

// Determine whether this callee is in conflict with another callee.
func (this *Profile) HasConflictWith(other *Profile) bool {
	if other == nil {
		return false
	}

	return slices.IndexFunc(this.ConflictPeers,
		func(i uint64) bool {
			return i == uint64(other.ID.UID)
		}) != -1
}

// Scheduler will update the calleed profile based on the conflict information returned by the
// conflict detection module after analyzing the transaction execution traces.
func (this *Profile) Commit() error {
	// Sequential function shouldn't exist in conflict list. The only reason for them to be
	// in the list is that they were previously marked as parallel but later changed to sequential because
	// of too many conflicts. So this must be dirty now.
	pathBuiler := &statecommon.PathBuilder{
		Address:  this.ID.Address,
		Selector: this.ID.Selector, Platform: statecommon.ETH_PATH}

	// This execution store will help generate the state changes in the form of state cells,
	// which will be committed together with the transaction execution later by the scheduler.
	execStore := this.profileStore.execStore

	setter := func(cell *statecell.StateCell, _ int64) {
		// Skip conflict check for the callee profile properties,
		// since they are only used for scheduling and do not affect the transaction execution results.
		cell.Property.SkipConflictCheck(true)
	}

	// Ensure the parent path exists.
	parentPath := pathBuiler.ProfileField("") // Get the path to write.
	if v, _, _ := execStore.Read(this.ID.Tx, pathBuiler.ProfileField(""), nil); v == nil {
		// Create the parent path if not exists.
		if _, err := execStore.Write(this.ID.Tx, parentPath, commutative.NewPath(), setter); err != nil {
			return err
		}
	}

	var err error
	// By default, all the tx are parellelizable, we only write the parallelism degree
	// when it is sequential only, to save storage and also avoid unnecessary conflicts.
	if this.parallelismDegree == 1 { // sequential only.
		path := pathBuiler.ProfileField(statecommon.PATH_PARALLELISM_DEGREE) // Get the path to write.
		v := noncommutative.NewUint64(this.parallelismDegree)
		_, wError := execStore.Write(this.ID.Tx, path, v, setter)
		err = errors.Join(err, wError)
	}

	// Write conflict list to storage.
	path := pathBuiler.ProfileField(statecommon.PATH_CONFLICT_INFO) // Get the path to write.
	buffer := codec.Uint64s(this.ConflictPeers).Encode()
	v := noncommutative.NewBytes(buffer)
	_, wError := execStore.Write(this.ID.Tx, path, v, setter)
	return errors.Join(err, wError)
}

// Get the estimate memory size of a callee profile for cache management.
func SizeOf(this *Profile) uint64 {
	if this == nil {
		return 0
	}

	size := uint64(unsafe.Sizeof(this.ID))
	size += 8                                         // parallelismDegree uint64
	size += 8                                         // prepayment uint64
	size += uint64(unsafe.Sizeof(this.ConflictPeers)) // slice header for ConflictPeers
	size += uint64(unsafe.Sizeof(this.profileStore))  // pointer to the owning profile store

	if this.ID != nil {
		size += 8  // Tx uint64
		size += 20 // Address [20]byte
		size += 4  // Selector [4]byte
		size += 8  // UID uint64
	}

	size += uint64(len(this.ConflictPeers)) * 8 // conflict peer payload entries
	return size
}

func (this *Profile) PrintToString() string {
	var b strings.Builder

	// Contract and Selector as hex
	contract := hex.EncodeToString(this.ID.Address[:])
	selector := hex.EncodeToString(this.ID.Selector[:])

	b.WriteString("Profile {\n")
	b.WriteString(fmt.Sprintf("  UID: %d\n", this.ID.UID))
	b.WriteString(fmt.Sprintf("  Contract: 0x%s\n", contract))
	b.WriteString(fmt.Sprintf("  Selector: 0x%s\n", selector))
	b.WriteString(fmt.Sprintf("  ParallelismDegree: %d\n", this.parallelismDegree))
	b.WriteString(fmt.Sprintf("  Prepayment: %d\n", this.prepayment))
	b.WriteString(fmt.Sprintf("  ConflictPeers: %v\n", this.ConflictPeers))
	b.WriteString("}")

	return b.String()
}

func (this *Profile) MarshalJSON() ([]byte, error) {
	type profileAlias struct {
		ID                *ID      `json:"id"`
		ParallelismDegree uint64   `json:"parallelismDegree"`
		Prepayment        uint64   `json:"prepayment"`
		ConflictPeers     []uint64 `json:"conflictPeers"`
	}

	alias := profileAlias{
		ID:                this.ID,
		ParallelismDegree: this.parallelismDegree,
		Prepayment:        this.prepayment,
		ConflictPeers:     slices.Clone(this.ConflictPeers),
	}

	return json.Marshal(&alias)
}
