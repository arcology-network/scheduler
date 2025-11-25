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

package scheduler

import (
	"errors"

	"github.com/arcology-network/common-lib/codec"
	"github.com/arcology-network/common-lib/crdt/noncommutative"
	eucommon "github.com/arcology-network/common-lib/types"
	schcommon "github.com/arcology-network/scheduler/common"
	stateengine "github.com/arcology-network/state-engine"
	statecommon "github.com/arcology-network/state-engine/common"
)

type ProfileManager struct {
	LocalCache  map[uint64]*Profile
	schStorage  *stateengine.StateStore
	maxCapacity uint64 // Maximum number of profiles to cache in memory
}

func NewProfileManager(schStorage *stateengine.StateStore, maxCapacity uint64) *ProfileManager {
	return &ProfileManager{
		LocalCache:  make(map[uint64]*Profile),
		schStorage:  schStorage,
		maxCapacity: maxCapacity,
	}
}

// Preload the scheduler with the given message profiles.
func (this *ProfileManager) Preload(stdMsgs []*eucommon.StandardMessage) {
	for _, v := range stdMsgs {
		if len(v.Native.Data) == 0 || v.Native.To == nil { // Transfer tx, no LocalCache
			continue
		}
		profile := this.LoadProfile(*v.Native.To, new(codec.Bytes4).FromBytes(v.Native.Data))
		this.LocalCache[profile.UID] = profile
	}
}

// Initialize the callee profile from the storage if exists.
func (this *ProfileManager) LoadProfile(addr [20]byte, selector [4]byte) *Profile {
	pathBuiler := &statecommon.PathBuilder{Address: addr, Selector: selector, Platform: statecommon.ETH_PATH}

	UID := schcommon.DeriveUID(pathBuiler.Address[:], pathBuiler.Selector[:]) // Get the unique ID for the callee.
	if profile := this.LocalCache[UID]; profile != nil {
		profile.UsageCount++
		return profile // Profile already exists
	}

	// Load the profile from the storage.
	profile := &Profile{
		UID:        schcommon.DeriveUID(pathBuiler.Address[:], pathBuiler.Selector[:]), // UID for quick matching
		Contract:   pathBuiler.Address,
		Selector:   pathBuiler.Selector,
		UsageCount: 1,
	}

	// Get the parallelism degree
	path := pathBuiler.ProfileField(statecommon.PARALLELISM_DEGREE)
	this.schStorage.ReadOnlyStore().IfExists(path)
	if paraDegree, err := this.schStorage.ReadOnlyStore().Retrieve(path, uint64(0)); paraDegree != nil && err == nil {
		profile.ParallelismDegree = paraDegree.(uint32)
	}

	// Get the minimum prepayment amount for deferred execution
	// If the amount is zero, it means the function is not deferrable.
	path = pathBuiler.ProfileField(statecommon.DEFERRED_PAYMENT)
	if prepayment, err := this.schStorage.ReadOnlyStore().Retrieve(path, uint64(0)); prepayment != nil && err == nil {
		profile.IsDeferrable = prepayment.(uint64) > 0
	}

	// Get the parallelism degree
	path = pathBuiler.ProfileField(statecommon.PARALLELISM_DEGREE)
	if Indices, err := this.schStorage.ReadOnlyStore().Retrieve(path, []byte{}); Indices != nil && err == nil {
		buffer := Indices.([]byte)
		profile.ConflictWith = codec.Uint64s{}.Decode(buffer).(codec.Uint64s)
	}

	this.LocalCache[UID] = profile
	return profile
}

func (this *ProfileManager) Find(UID uint64) (*Profile, bool) {
	v, ok := this.LocalCache[UID]
	return v, ok
}

// Write back the modified callee profiles to the storage.
func (this *ProfileManager) Save() error {
	var err error
	for _, profile := range this.LocalCache {
		if !profile.Dirty {
			continue
		}

		// Sequential function shouldn't exist in conflict list. The only reason for them to be
		// in the list is that they were previously marked as parallel but later changed to sequential because
		// of too many conflicts. So this must be dirty now.
		pathBuiler := &statecommon.PathBuilder{Address: profile.Contract, Selector: profile.Selector, Platform: statecommon.ETH_PATH}

		if profile.ParallelismDegree == 1 {
			path := pathBuiler.ProfileField(statecommon.PARALLELISM_DEGREE) // Get the path to write.
			v := noncommutative.NewUint32(profile.ParallelismDegree)
			_, wError := this.schStorage.Write(statecommon.SYSTEM, path, v)
			err = errors.Join(err, wError)
		}

		// Write conflict list to storage.
		path := pathBuiler.ProfileField(statecommon.CONFLICT_INFO_PATH) // Get the path to write.
		buffer := codec.Uint64s(profile.ConflictWith).Encode()
		v := noncommutative.NewBytes(buffer)
		_, wError := this.schStorage.Write(statecommon.SYSTEM, path, v)
		err = errors.Join(err, wError)
	}
	return err
}

// Clear the least frequently used profiles from the local cache to free up memory.
func (this *ProfileManager) Clear() {
	if len(this.LocalCache)-int(this.maxCapacity) <= 0 {
		return
	}

	totalAccess := 0
	for _, v := range this.LocalCache {
		totalAccess += int(v.UsageCount)
	}

	// Remove the profiles with usage count less than the average usage count.!
	threshold := totalAccess / len(this.LocalCache)
	for k, v := range this.LocalCache {
		if v.UsageCount <= uint64(threshold) {
			delete(this.LocalCache, k)
		}
	}
}

// Register a conflict pair into the scheduler.
// The conflict pairs are usually returned by the conflict detection module
// after analyzing the transaction execution traces.
func (this *ProfileManager) RegisterNewConflict(lftAddr [20]byte, lftSig [4]byte, rgtAddr [20]byte, rgtSig [4]byte) {
	seedCallee := this.LoadProfile(lftAddr, lftSig)
	otherCallee := this.LoadProfile(rgtAddr, rgtSig)

	// The conflict exists already.
	if seedCallee.IfConflictExists(otherCallee) {
		panic("Schduler: Conflict already exists! " + seedCallee.PrintToString() + otherCallee.PrintToString())
	}

	// Add the conflict entries both ways.
	seedCallee.AddConflict(otherCallee)
}
