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

package scheduler

import "github.com/arcology-network/scheduler/conflictor"

// Test harness for the scheduler.
// Should never be used in production code, and is only for testing and debugging purposes.
func DebugPrecommit(this *Scheduler, conflictSet *conflictor.CollisionSummary) {
	// Map the conflict info to the original callee profiles
	// using UID as the key.
	for _, conflictInfo := range conflictSet.Collisions {
		// Map back to their orginal callee profile IDs
		selfID, peerIDs := conflictInfo.MapConflictToCallee(this.latest.JobIDLookup)

		// Get the profiles by IDs and add the conflict peers.
		selfProfile, _ := this.ProfileStore.LoadOrCreate(selfID)
		for _, peerID := range peerIDs {
			peerProfile, _ := this.ProfileStore.LoadOrCreate(peerID)
			peerProfile.CrossLink(selfProfile) // Add each other as conflict peers.
		}
	}
}

// Commit the scheduler's conflict database based on the latest conflict info.
func DebugCommit(this *Scheduler) error {
	return this.ProfileStore.Precommit() // Save the updated profiles to the storage.
}
