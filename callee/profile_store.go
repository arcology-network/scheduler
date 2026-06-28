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
	"math"
	"sync"

	"github.com/arcology-network/common-lib/codec"
	crdtcommon "github.com/arcology-network/common-lib/crdt/common"
	"github.com/arcology-network/common-lib/crdt/noncommutative"
	"github.com/arcology-network/common-lib/storage/cache"
	"github.com/cespare/xxhash"

	// cachedstore "github.com/arcology-network/common-lib/storage/cachedstore"
	// stgcodec "github.com/arcology-network/common-lib/storage/codec"
	stgintf "github.com/arcology-network/common-lib/storage/interface"
	statecommon "github.com/arcology-network/state-engine/common"
	statecache "github.com/arcology-network/state-engine/state/cache"
)

// ProfileBackend := [uint64, *Profile, string, crdtcommon.CRDT]

type ProfileStore struct {
	// The modified profiles that need to be committed back to the storage.
	// The key is the UID of the callee (derived from address + selector).
	dirties map[uint64]*Profile
	dirtyMu sync.Mutex

	// Cache is used to store the loaded profiles to avoid repeated loading from the storage.
	// It is another layer of cache on top of the storage.
	cache      *cache.Cache[uint64, *Profile]
	stateStore stgintf.ReadOnlyStore[string, crdtcommon.CRDT]

	// For transition generation only. It has the same underlying storage as
	// stateStore.
	execStore *statecache.ExecutionStateStore
}

func NewProfileStore(readonlyStore stgintf.ReadOnlyStore[string, crdtcommon.CRDT]) *ProfileStore {
	pStore := &ProfileStore{
		dirties: make(map[uint64]*Profile),
		cache: cache.NewCache(
			2,
			func(k uint64) uint64 { return k },
			cache.NewCachePolicy(
				math.MaxUint64,
				func(p *Profile) uint64 {
					return SizeOf(p)
				},
			), // Each profile counts against the capacity by its estimated size.
		),
		stateStore: readonlyStore,
		execStore: statecache.NewExecutionStateStore( // For Transition generation only.
			readonlyStore,
			16,
			1,
			func(k string) uint64 {
				return xxhash.Sum64String(k)
			},
		),
	}

	return pStore
}

func (this *ProfileStore) ExecStore() *statecache.ExecutionStateStore { return this.execStore }
func (this *ProfileStore) StateStore() stgintf.ReadOnlyStore[string, crdtcommon.CRDT] {
	return this.stateStore
}

func (this *ProfileStore) Dirties() map[uint64]*Profile { return this.dirties }

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

// Write back the modified callee profiles back to an instance of ExecutionStateStore
// for transition generation. It doesn't write back to the original state store directly.
// The actual commit to the original state store happens together with other transitions.
func (this *ProfileStore) Clear() error {
	// Clear the dirty list and cache to free up memory.
	this.dirties = make(map[uint64]*Profile)
	this.cache = cache.NewCache(
		2,
		func(k uint64) uint64 { return k },
		cache.NewCachePolicy(
			math.MaxUint64,
			func(p *Profile) uint64 {
				return SizeOf(p)
			},
		), // Each profile counts against the capacity by its estimated size.
	)

	this.execStore.Clear()
	return nil
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
	if v, err := this.stateStore.Get(pathBuiler.ProfileField("")); v == nil || err != nil {
		return nil, err
	}

	// Get the parallelism degree
	profile := NewProfile(id.Tx, id.Address, id.Selector, this)
	path := pathBuiler.ProfileField(statecommon.PATH_PARALLELISM_DEGREE)
	if paraDegree, err := this.stateStore.Get(path); paraDegree != nil && err == nil {
		profile.parallelismDegree = uint64(*paraDegree.(*noncommutative.Uint64))
	}

	// Get the minimum prepayment amount for deferred execution
	// If the amount is zero, it means the function is not deferrable.
	path = pathBuiler.ProfileField(statecommon.PATH_DEFERRED_PAYMENT)

	if prepayment, err := this.stateStore.Get(path); prepayment != nil && err == nil {
		profile.prepayment = uint64(*prepayment.(*noncommutative.Uint64))
	}

	// Get the conflict peers
	path = pathBuiler.ProfileField(statecommon.PATH_CONFLICT_INFO)
	if Indices, err := this.stateStore.Get(path); Indices != nil && err == nil {
		buffer := Indices.(*noncommutative.Bytes)
		profile.ConflictPeers = append(profile.ConflictPeers,
			codec.Uint64s{}.Decode(*buffer).(codec.Uint64s)...)

	}
	return profile, nil
}

// Write the dirty profiles back to the execution store for transition generation.
// It doesn't write back to the original state store directly.
func (this *ProfileStore) WriteToExeStore() error {
	var err error
	for _, dirty := range this.dirties {
		if !dirty.IsEmpty() {
			err = errors.Join(err, dirty.Commit()) // Save to the conflict storage.
		}
	}
	return err
}
