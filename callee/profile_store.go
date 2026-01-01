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

	stateengine "github.com/arcology-network/state-engine"
)

type ProfileStore struct {
	dirties      map[uint64]*Profile
	profileCache map[uint64]*Profile
	maxCapacity  uint64 // Maximum number of profiles to cache in memory
	backend      *stateengine.StateStore
	mu           sync.Mutex
}

func NewProfileManager(backend *stateengine.StateStore, maxCapacity uint64) *ProfileStore {
	return &ProfileStore{
		dirties:      make(map[uint64]*Profile),
		profileCache: make(map[uint64]*Profile),
		backend:      backend,
		maxCapacity:  maxCapacity,
	}
}

func (this *ProfileStore) Backend() *stateengine.StateStore {
	return this.backend
}

// Check if the callee profile exists in the local cache or storage.
func (this *ProfileStore) LoadIfExists(addr [20]byte, selector [4]byte) *Profile {
	if addr == [20]byte{} || selector == [4]byte{} {
		return nil // Transfers / Deployment. Can be seen as incomplete callee identity.
	}

	id := NewID(addr, selector)

	if profile := this.profileCache[id.UID]; profile != nil {
		return profile // Profile already exists
	}

	profile, _ := LoadProfile(id, this)
	if profile == nil {
		return nil // Profile does not exist
	}

	this.mu.Lock()
	this.profileCache[id.UID] = profile
	this.mu.Unlock()
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
	profile := NewProfile(id.Address, id.Selector, this)
	this.profileCache[id.UID] = profile
	return profile, nil // New profile.
}

// Write back the modified callee profiles back to the storage.
func (this *ProfileStore) Commit() error {
	var err error
	for _, dirtyProfile := range this.dirties {
		err = errors.Join(err, dirtyProfile.Commit(this.backend)) // Save to the conflict storage.
	}
	return err
}

// Clear the least frequently used profiles from the local cache to free up memory.
func (this *ProfileStore) Clear() {
	this.profileCache = make(map[uint64]*Profile)
}

// Add a modified callee profile into the dirties.
func (this *ProfileStore) AddToDirty(profile *Profile) {
	this.dirties[profile.ID.UID] = profile
}

// Register a conflict pair into the scheduler.
// The conflict pairs are usually returned by the conflict detection module
// after analyzing the transaction execution traces.
func (this *ProfileStore) RegisterNewConflict(lftID *ID, rgtID *ID) {
	selfCallee, _ := this.LoadOrCreate(lftID)
	peerCallee, _ := this.LoadOrCreate(rgtID)

	// The conflict exists already.
	if selfCallee.IsMutuallyConflicting(peerCallee) {
		panic("Schduler: Conflict already exists! " + selfCallee.PrintToString() + peerCallee.PrintToString())
	}

	// Add the conflict entries both ways.
	selfCallee.CrossLink(peerCallee)

	// Mark both profiles as dirty for commit later.
	this.dirties[lftID.UID] = selfCallee
	this.dirties[rgtID.UID] = peerCallee
}
