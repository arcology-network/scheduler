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

	stateengine "github.com/arcology-network/state-engine/state/cache"
)

type ProfileStore struct {
	dirties      map[uint64]*Profile
	profileCache map[uint64]*Profile
	maxCapacity  uint64 // Maximum number of profiles to cache in memory
	backend      *stateengine.ExecutionStateStore
	profileMu    sync.Mutex
	dirtyMu      sync.Mutex
}

func NewProfileManager(backend *stateengine.ExecutionStateStore, maxCapacity uint64) *ProfileStore {
	return &ProfileStore{
		dirties:      make(map[uint64]*Profile),
		profileCache: make(map[uint64]*Profile),
		backend:      backend,
		maxCapacity:  maxCapacity,
	}
}

func (this *ProfileStore) Backend() *stateengine.ExecutionStateStore {
	return this.backend
}

// Check if the callee profile exists in the local cache or storage.
func (this *ProfileStore) LoadIfExists(tx uint64, addr [20]byte, selector [4]byte) *Profile {
	this.profileMu.Lock()
	defer this.profileMu.Unlock()

	if addr == [20]byte{} || selector == [4]byte{} {
		return nil // Transfers / Deployment. Can be seen as incomplete callee identity.
	}

	id := NewID(tx, addr, selector)
	if profile := this.profileCache[id.UID]; profile != nil {
		return profile // Profile already exists
	}

	profile, _ := LoadProfile(id, this)
	if profile == nil {
		return nil // Profile does not exist
	}

	this.profileCache[id.UID] = profile
	return profile
}

// Load the callee profile from the storage if exists, otherwise create a new one.
// This is used when updating the callee profile storage after some conflicts are detected.
func (this *ProfileStore) LoadOrCreate(id *ID) (*Profile, error) {
	if profile := this.profileCache[id.UID]; profile != nil {
		return profile, nil // Profile already exists in cache.
	}

	// Load from storage if exists.
	if profile, err := LoadProfile(id, this); profile != nil && err == nil {
		this.profileCache[id.UID] = profile
		return profile, nil // Profile loaded from storage.
	}

	//Create a new profile.
	profile := NewProfile(id.Tx, id.Address, id.Selector, this)
	this.profileCache[id.UID] = profile
	return profile, nil // New profile.
}

// Write back the modified callee profiles back to the storage.
func (this *ProfileStore) Commit() error {
	var err error
	for _, dirtyProfile := range this.dirties {
		err = errors.Join(err, dirtyProfile.Commit()) // Save to the conflict storage.
	}
	return err
}

// Clear the least frequently used profiles from the local cache to free up memory.
func (this *ProfileStore) Clear() {
	this.profileCache = make(map[uint64]*Profile)
}

// Add a modified callee profile into the dirties.
func (this *ProfileStore) AddToDirty(profile *Profile) {
	this.dirtyMu.Lock()
	defer this.dirtyMu.Unlock()
	this.dirties[profile.ID.UID] = profile
}

// Register a conflict pair into the scheduler.
// The conflict pairs are usually returned by the conflict detection module
// after analyzing the transaction execution traces.
func (this *ProfileStore) RegisterNewConflict(lftID *ID, rgtID *ID) error {
	selfCallee, err := this.LoadOrCreate(lftID)
	if err != nil {
		panic("Scheduler: Failed to load or create callee profile! " + err.Error())
	}

	peerCallee, err := this.LoadOrCreate(rgtID)
	if err != nil {
		panic("Scheduler: Failed to load or create callee profile! " + err.Error())
	}

	// The conflict exists already.
	if selfCallee.IsMutuallyConflicting(peerCallee) {
		panic("Schduler: Conflict already exists! " + selfCallee.PrintToString() + peerCallee.PrintToString())
	}

	// Add the conflict entries both ways.
	selfCallee.CrossLink(peerCallee)

	// Mark both profiles as dirty for commit later.
	this.dirties[lftID.UID] = selfCallee
	this.dirties[rgtID.UID] = peerCallee
	return nil
}
