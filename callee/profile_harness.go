/*
 *   Copyright (c) 2026 Arcology Network

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

import "errors"

// Register a conflict pair into the scheduler.
// The conflict pairs are usually returned by the conflict detection module
// after analyzing the transaction execution traces.
func DebugRegisterNewConflict(this *ProfileStore, lftID *ID, rgtID *ID) (*Profile, *Profile, error) {
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
	return selfCallee, peerCallee, nil
}

func DebugSetPrePayment(this *ProfileStore, id *ID, amount uint64) (*Profile, error) {
	selfCallee, err := this.LoadOrCreate(id)
	if err != nil {
		panic("Scheduler: Failed to load or create callee profile! " + err.Error())
	}
	selfCallee.SetPrepayment(amount)
	this.dirties[id.UID] = selfCallee
	return selfCallee, nil
}

// Write back the modified callee profiles back to the storage.
func DebugCommit(this *ProfileStore) error {
	var err error
	for _, dirtyProfile := range this.Dirties() {
		err = errors.Join(err, dirtyProfile.Commit()) // Save to the conflict storage.
	}
	this.dirties = make(map[uint64]*Profile)
	// this.Clear() // Clear the local cache after commit to free up memory.
	return err
}
