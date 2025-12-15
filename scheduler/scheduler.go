/*
 *   Copyright (c) 2024 Arcology Network

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
	"sort"

	assoc "github.com/arcology-network/common-lib/exp/associative"
	"github.com/arcology-network/common-lib/exp/slice"
	libtypes "github.com/arcology-network/common-lib/types"

	mapi "github.com/arcology-network/common-lib/exp/map"
	"github.com/arcology-network/scheduler/arbitrator"
	profile "github.com/arcology-network/scheduler/callee"
	workload "github.com/arcology-network/scheduler/workload"
)

type Scheduler struct {
	latest *workload.ExecutionSchedule
	*profile.ProfileStore
}

// Initialize a new scheduler, the fileName is the file path to the scheduler's conflict database and the deferByDefault
// instructs the scheduler to schedule the deferred transactions if it is true.
func NewScheduler(manager *profile.ProfileStore) (*Scheduler, error) {
	return &Scheduler{
		ProfileStore: manager,
	}, nil
}

// The scheduler will optimize the given transactions and return a schedule.
// The schedule will contain the transactions that can be executed in parallel and the ones that have to
// be executed sequentially.
func (this *Scheduler) New(stdMsgs []*libtypes.StandardMessage) *workload.ExecutionSchedule {
	// Get the static schedule for the given transactions first.
	sch, profiledMsgs := this.StaticSchedule(stdMsgs) // The profiledMsgs are the transactions that need to be scheduled to avoid conflicts.
	if len(profiledMsgs) == 0 {
		this.latest = sch
		return sch // No known conflicts and no deferred transactions.
	}

	// Sort the profiles by the number of conflicts and the profile index in ascending order.
	sort.SliceStable(profiledMsgs, func(i, j int) bool {
		if lft, rgt := profiledMsgs[i].First.NumConflicts(), profiledMsgs[j].First.NumConflicts(); lft != rgt {
			return lft < rgt
		}
		return profiledMsgs[i].Second.ID < profiledMsgs[j].Second.ID
	})

	// The code below will search for the parallel transaction set from a set of conflicting transactions.
	// Whataever left is the sequential transaction set after this.
	start := 0
	for {
		// The conflict dictionary of all indices of the current transaction set.
		profiledMsgs = profiledMsgs[start:] // Shrink the search space for better performance.
		var seed **assoc.Pair[*profile.Profile, *libtypes.StandardMessage]

		start, seed = slice.FindFirstIf(profiledMsgs,
			func(i int, v *assoc.Pair[*profile.Profile, *libtypes.StandardMessage]) bool {
				return v != nil
			},
		)

		if start == -1 {
			break // Nothing left to process.
		}
		paraSet := []*assoc.Pair[*profile.Profile, *libtypes.StandardMessage]{*seed} // A set of conflict free transactions, starting with the first profile.
		profiledMsgs = profiledMsgs[start+1:]

		// This set containes all the conflicts of the current parallel transaction set.
		// Any transaction that conflicts with any of them cannot be added to the parallel set.
		conflictLookup := mapi.FromSlice((*seed).First.ConflictPeers, func(k uint64) bool { return true })

		// Look for the parallel transactions that aren't conflicting with the current set of transactions.
		for i := 0; i < len(profiledMsgs); i++ {
			if profiledMsgs[i] == nil {
				continue // Already processed.
			}

			candidate := profiledMsgs[i]

			// The current profile isn't conflicting with any transaction in the unique profile set
			// so it can be added to the parallel transaction set. Because the conflict info is always symmetric,
			// we only need to check one way.
			if _, ok := conflictLookup[candidate.First.ID.UID]; !ok { // Not in the conflict dictionary
				// Merge the new profile's conflicts to the conflict dictionary.
				mapi.Insert(conflictLookup, candidate.First.ConflictPeers, func(_ int, k uint64) (uint64, bool) {
					return k, true
				})

				paraSet = append(paraSet, candidate) // Add the current profile to the parallel transaction set.
				profiledMsgs[i] = nil                // Mark as processed.
			}
		}

		// Only one transaction in the parallel set, no need to proceed with planning deferred execution.
		if len(paraSet) == 1 {
			msgs := assoc.NewPairs(paraSet).Seconds()            // Extract the message from the pair.
			sch.WithConflict = append(sch.WithConflict, msgs...) // Add to the conflict set.
		}

		// Look for the deferred transactions and add them to the deferred transaction set.
		paraGen, deferredGen := this.ScheduleWithDeferred(paraSet)

		sch.RawMsgSet = append(sch.RawMsgSet, paraGen)     // Insert the parallel transaction first
		sch.RawMsgSet = append(sch.RawMsgSet, deferredGen) // Insert the deferred transaction generation.

		// Remove the already scheduled transaction from the profiledMsgs slice.
		if len(profiledMsgs) == 0 {
			break // Nothing left to process.
		}
	}

	this.latest = sch // Keep the current schedule for reference.
	return this.latest
}

// The scheduler will scan through and look for multipl instances of the same profile and put one of them in the second
// consecutive set of transactions for deferred execution.
func (this *Scheduler) ScheduleWithDeferred(paraMsgs []*assoc.Pair[*profile.Profile, *libtypes.StandardMessage]) ([][]*libtypes.StandardMessage, [][]*libtypes.StandardMessage) {
	// Group by profile UID.
	_, msgSets := slice.GroupBy(paraMsgs, func(_ int, pair *assoc.Pair[*profile.Profile, *libtypes.StandardMessage]) *uint64 {
		return &pair.First.ID.UID
	})

	paraGen := [][]*libtypes.StandardMessage{}     // The parallel transaction generation.
	deferredGen := [][]*libtypes.StandardMessage{} // The deferred transaction generation.
	for _, msgs := range msgSets {
		paraMsgs := slice.Transform(msgs,
			func(i int, msgPair *assoc.Pair[*profile.Profile, *libtypes.StandardMessage]) []*libtypes.StandardMessage {
				return []*libtypes.StandardMessage{msgPair.Second}
			},
		)

		// There is more than one transaction for the same profile and the first one is marked as deferrable.
		if len(msgs) > 1 && msgs[0].Second.IsDeferred {
			def := *slice.PopBack(&paraMsgs)       // Use the last one as the deferred transaction.
			deferredGen = append(deferredGen, def) // Add the deferred transaction to the new generation.
		}
		paraGen = append(paraGen, paraMsgs...)
	}
	return paraGen, deferredGen
}

// The scheduler will StaticSchedule base on some predefined rules for specific transaction types,
// such as transfers and contract deployments.
func (this *Scheduler) StaticSchedule(stdMsgs []*libtypes.StandardMessage) (*workload.ExecutionSchedule, []*assoc.Pair[*profile.Profile, *libtypes.StandardMessage]) {
	sch := &workload.ExecutionSchedule{}
	if len(stdMsgs) == 0 {
		return sch, nil // No transactions to process.
	}

	// Transfers won't have any conflicts, as long as they have enough balances.
	// Deployments are conflict-free as well.
	sch.Transfers = slice.MoveIf(&stdMsgs, func(i int, msg *libtypes.StandardMessage) bool { return len(msg.Native.Data) == 0 })
	sch.Deployments = slice.MoveIf(&stdMsgs, func(i int, msg *libtypes.StandardMessage) bool { return msg.Native.To == nil })
	if len(stdMsgs) == 0 {
		return sch, nil // All the transactions are transfers.
	}

	// Get the IDs for the given addresses and signatures, which will be used to find the profile index.
	// To save memory, the profiles are stored in the dictionary by their IDs, not by their addresses and signatures.
	profiledMsgs := slice.ParallelTransform(
		stdMsgs,
		8,
		func(i int, msg *libtypes.StandardMessage) *assoc.Pair[*profile.Profile, *libtypes.StandardMessage] {
			profile := this.ProfileStore.LoadIfExists(profile.NewID(msg.GetAddressAndSelector()))
			// Convert the address and signature to a unique key.
			return assoc.NewPair(profile, msg)
		})

	// Move the transactions that have no known conflicts to the parallel trasaction array first.
	// If a profile has no known conflicts with anyone else, it is either a conflict-free implementation or
	// has been fortunate enough to avoid conflicts so far.
	unknowns := slice.MoveIf(&profiledMsgs, func(_ int, v *assoc.Pair[*profile.Profile, *libtypes.StandardMessage]) bool {
		return v.First == nil // No profile.
	})
	sch.Unknowns = assoc.Pairs[*profile.Profile, *libtypes.StandardMessage](unknowns).Seconds()

	// Move Sequential only profiles to the sequential array.
	sequentials := slice.MoveIf(&profiledMsgs, func(_ int, v *assoc.Pair[*profile.Profile, *libtypes.StandardMessage]) bool {
		return len(v.First.ConflictPeers) == 0
	})
	sch.Sequentials = assoc.Pairs[*profile.Profile, *libtypes.StandardMessage](sequentials).Seconds()

	return sch, profiledMsgs
}

// Precommit the scheduler's conflict database based on the latest conflict info.
func (this *Scheduler) Precommit(conflictSet *arbitrator.Conflicts) {
	// Map the conflict info to the original callee profiles
	// using UID as the key.
	for _, conflictInfo := range conflictSet.Conflicts {
		// Map back to their orginal callee profile IDs
		selfID, peerIDs := conflictInfo.MapToCallees(this.latest.MsgLookup)

		// Get the profiles by IDs and add the conflict peers.
		selfProfile, _ := this.ProfileStore.LoadOrCreate(selfID)
		slice.Foreach(peerIDs, func(i int, peerID **profile.ID) {
			peerProfile, _ := this.ProfileStore.LoadOrCreate(*peerID)
			peerProfile.CrossLink(selfProfile) // Add each other as conflict peers.
		})
	}
}

// Commit the scheduler's conflict database based on the latest conflict info.
func (this *Scheduler) Commit() error {
	return this.ProfileStore.Commit() // Save the updated profiles to the storage.
}
