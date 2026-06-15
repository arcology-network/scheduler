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
	"sync"

	"github.com/arcology-network/common-lib/codec"
	"github.com/arcology-network/common-lib/crdt/noncommutative"
	"github.com/arcology-network/common-lib/storage/cache"
	statecommon "github.com/arcology-network/state-engine/common"
	stateengine "github.com/arcology-network/state-engine/state/cache"
)

type ProfileStore struct {
	// The modified profiles that need to be committed back to the storage.
	// The key is the UID of the callee (derived from address + selector).
	dirties map[uint64]*Profile
	dirtyMu sync.Mutex

	// Cache is used to store the loaded profiles to avoid repeated loading from the storage.
	// It is another layer of cache on top of the storage.
	cache      *cache.Cache[uint64, *Profile]
	stateStore *stateengine.ExecutionStateStore
}

func NewProfileStore(stateStore *stateengine.ExecutionStateStore, maxCapacity uint64) *ProfileStore {
	return &ProfileStore{
		dirties: make(map[uint64]*Profile),
		cache: cache.NewCache(
			1000,
			func(k uint64) uint64 { return k },
			cache.NewCachePolicy(
				maxCapacity,
				func(p *Profile) uint64 {
					return SizeOf(p)
				},
			), // Each profile counts against the capacity by its estimated size.
		),

		stateStore: stateStore,
	}
}

func (this *ProfileStore) StateStore() *stateengine.ExecutionStateStore { return this.stateStore }
func (this *ProfileStore) Dirties() map[uint64]*Profile                 { return this.dirties }

func (this *ProfileStore) LoadIfExists(tx uint64, addr [20]byte, selector [4]byte) (*Profile, error) {
	if addr == [20]byte{} || selector == [4]byte{} {
		return nil, nil // Transfers / Deployment. Can be seen as incomplete callee identity.
	}

	id := NewID(tx, addr, selector)
	if profile, err := this.cache.Get(id.UID); err == nil {
		return profile.(*Profile), nil // Profile exists in cache.
	}

	profile, _ := this.loadProfile(id)
	if profile == nil {
		return nil, nil // Profile does not exist
	}

	// Set a placeholder to prevent cache stampede for the same profile.
	// this.cache.Set(id.UID, profile)
	return profile, this.cache.Set(id.UID, profile)
}

// Write back the modified callee profiles back to the storage.
func (this *ProfileStore) Commit() error {
	var err error
	for _, dirtyProfile := range this.dirties {
		if dirtyProfile.IsEmpty() {
			continue // Skip empty profiles to save storage space.
		}
		err = errors.Join(err, dirtyProfile.Commit()) // Save to the conflict storage.
	}
	this.Reset()
	// this.Clear() // Clear the local cache after commit to free up memory.
	return err
}

func (this *ProfileStore) Reset() {
	this.dirties = make(map[uint64]*Profile)
}

// Add a modified callee profile into the dirties.
func (this *ProfileStore) addToDirty(profile *Profile) {
	this.dirtyMu.Lock()
	defer this.dirtyMu.Unlock()
	this.dirties[profile.ID.UID] = profile
}

// Load the callee profile from the storage if exists, otherwise create a new one.
// This is used when updating the callee profile storage after some conflicts are detected.
func (this *ProfileStore) LoadOrCreate(id *ID) (*Profile, error) {
	if profile, _ := this.cache.Get(id.UID); profile != nil {
		return profile.(*Profile), nil // Profile already exists in cache.
	}

	// Load from storage if exists.
	profile, _ := this.loadProfile(id)
	if profile == nil {
		profile = NewProfile(id.Tx, id.Address, id.Selector, this)
		this.addToDirty(profile)
	}

	// Mark the profile as dirty for commit later.
	return profile, this.cache.Set(id.UID, profile) // New profile.
}

// Load the callee profile from the storage.
func (this *ProfileStore) loadProfile(id *ID) (*Profile, error) {
	// Get the unique ID for the callee.
	pathBuiler := &statecommon.PathBuilder{
		Address:  id.Address,
		Selector: id.Selector,
		Platform: statecommon.ETH_PATH}

	// Check if the profile path exists
	if v, err := this.stateStore.CommittedStore().Get(pathBuiler.ProfileField("")); v == nil || err != nil {
		return nil, err
	}

	// Get the parallelism degree
	profile := NewProfile(id.Tx, id.Address, id.Selector, this)
	path := pathBuiler.ProfileField(statecommon.PATH_PARALLELISM_DEGREE)
	if paraDegree, err := this.stateStore.CommittedStore().Get(path); paraDegree != nil && err == nil {
		profile.SetParallelismDegree(uint64(*paraDegree.(*noncommutative.Uint64)))
	}

	// Get the minimum prepayment amount for deferred execution
	// If the amount is zero, it means the function is not deferrable.
	path = pathBuiler.ProfileField(statecommon.PATH_DEFERRED_PAYMENT)

	if prepayment, err := this.stateStore.CommittedStore().Get(path); prepayment != nil && err == nil {
		profile.SetPrepayment((uint64(*prepayment.(*noncommutative.Uint64))))
	}

	// Get the conflict peers
	path = pathBuiler.ProfileField(statecommon.PATH_CONFLICT_INFO)
	if Indices, err := this.stateStore.CommittedStore().Get(path); Indices != nil && err == nil {
		buffer := Indices.(*noncommutative.Bytes)
		profile.AddConflictPeers(codec.Uint64s{}.Decode(*buffer).(codec.Uint64s))
	}
	return profile, nil
}
