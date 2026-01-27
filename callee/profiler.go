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

	"github.com/arcology-network/common-lib/codec"
	commutative "github.com/arcology-network/common-lib/crdt/commutative"
	"github.com/arcology-network/common-lib/crdt/noncommutative"
	statecommon "github.com/arcology-network/state-engine/common"
)

//  "github.com/arcology-network/scheduler"

// The callee struct stores the information of a contract function that is called by the EOA initiated transactions.
// It is mainly used to optimize the execution of the transactions. A callee is uniquely identified by a
// combination of the contract's address and the function signature.
type Profile struct {
	ID *ID

	parallelismDegree uint32   // Execution parallelism, 1 for sequential, otherwise parallel.
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

// Load the callee profile from the storage.
func LoadProfile(id *ID, profileStore *ProfileStore) (*Profile, error) {
	// Get the unique ID for the callee.
	pathBuiler := &statecommon.PathBuilder{
		Address:  id.Address,
		Selector: id.Selector,
		Platform: statecommon.ETH_PATH}

	// Check if the profile path exists
	if v, err := profileStore.backend.ReadOnlyStore().Retrieve(pathBuiler.ProfileField(""), new(commutative.Path)); v == nil || err != nil {
		return nil, err
	}

	// Get the parallelism degree
	profile := NewProfile(id.Tx, id.Address, id.Selector, profileStore)
	path := pathBuiler.ProfileField(statecommon.PATH_PARALLELISM_DEGREE)
	if paraDegree, err := profileStore.backend.ReadOnlyStore().Retrieve(path, noncommutative.NewUint64(0)); paraDegree != nil && err == nil {
		profile.SetParallelismDegree(uint32(*paraDegree.(*noncommutative.Uint64)))
	}

	// Get the minimum prepayment amount for deferred execution
	// If the amount is zero, it means the function is not deferrable.
	path = pathBuiler.ProfileField(statecommon.PATH_DEFERRED_PAYMENT)

	if prepayment, err := profileStore.backend.ReadOnlyStore().Retrieve(path, noncommutative.NewUint64(0)); prepayment != nil && err == nil {
		profile.SetPrepayment((uint64(*prepayment.(*noncommutative.Uint64))))
	}

	// Get the conflict peers
	path = pathBuiler.ProfileField(statecommon.PATH_CONFLICT_INFO)
	if Indices, err := profileStore.backend.ReadOnlyStore().Retrieve(path, noncommutative.NewBytes([]byte{})); Indices != nil && err == nil {
		buffer := Indices.([]byte)
		profile.AddConflictPeers(codec.Uint64s{}.Decode(buffer).(codec.Uint64s))
	}
	return profile, nil
}

func (this *Profile) SetParallelismDegree(n uint32) {
	this.parallelismDegree = n
	this.profileStore.AddToDirty(this)
}

func (this *Profile) GetParallelismDegree() uint32 { return this.parallelismDegree }

// Determine whether this callee profile can be deferred for later execution.
func (this *Profile) IsDeferrable() bool { return this.prepayment > 0 }

func (this *Profile) SetPrepayment(prepayment uint64) {
	this.prepayment = prepayment
	this.profileStore.AddToDirty(this)
}

func (this *Profile) GetPrepayment() uint64 { return this.prepayment }

func (this *Profile) CrossLink(other *Profile) {
	this.AddConflictPeers([]uint64{other.ID.UID})
	other.AddConflictPeers([]uint64{this.ID.UID})
}

func (this *Profile) AddConflictPeers(list []uint64) {
	if len(this.ConflictPeers)+len(list) > statecommon.MAX_NUM_CONFLICTS {
		this.ConflictPeers = this.ConflictPeers[:0]
		this.parallelismDegree = 1 // Too many conflicts, mark as sequential only.
	} else {
		this.ConflictPeers = append(this.ConflictPeers, list...)
	}
	this.profileStore.AddToDirty(this)
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
	return slices.IndexFunc(this.ConflictPeers, func(i uint64) bool { return i == uint64(other.ID.UID) }) != -1
}

func (this *Profile) Commit() error {
	// Sequential function shouldn't exist in conflict list. The only reason for them to be
	// in the list is that they were previously marked as parallel but later changed to sequential because
	// of too many conflicts. So this must be dirty now.
	pathBuiler := &statecommon.PathBuilder{
		Address:  this.ID.Address,
		Selector: this.ID.Selector, Platform: statecommon.ETH_PATH}

	// Get the storage
	store := this.profileStore.Backend()

	// Ensure the parent path exists.
	parentPath := pathBuiler.ProfileField("") // Get the path to write.
	if v, _, _ := store.Read(this.ID.Tx, pathBuiler.ProfileField(""), nil); v == nil {
		// Create the parent path if not exists.
		if _, err := store.Write(this.ID.Tx, parentPath, commutative.NewPath()); err != nil {
			return err
		}
	}

	var err error
	if this.parallelismDegree == 1 {
		path := pathBuiler.ProfileField(statecommon.PATH_PARALLELISM_DEGREE) // Get the path to write.
		v := noncommutative.NewUint32(this.parallelismDegree)
		_, wError := store.Write(this.ID.Tx, path, v)
		err = errors.Join(err, wError)
	}

	// Write conflict list to storage.
	path := pathBuiler.ProfileField(statecommon.PATH_CONFLICT_INFO) // Get the path to write.
	buffer := codec.Uint64s(this.ConflictPeers).Encode()
	v := noncommutative.NewBytes(buffer)
	_, wError := store.Write(this.ID.Tx, path, v)
	return errors.Join(err, wError)
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
		ParallelismDegree uint32   `json:"parallelismDegree"`
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
