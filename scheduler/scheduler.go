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
	mapi "github.com/arcology-network/common-lib/exp/map"
	"github.com/arcology-network/common-lib/exp/slice"

	slices "github.com/arcology-network/common-lib/exp/slice"
	eucommon "github.com/arcology-network/common-lib/types"

	callee "github.com/arcology-network/scheduler/callee"
	schcommon "github.com/arcology-network/scheduler/common"
	workload "github.com/arcology-network/scheduler/workload"
)

type Scheduler struct {
	profileManager *callee.ProfileManager
}

// Initialize a new scheduler, the fileName is the file path to the scheduler's conflict database and the deferByDefault
// instructs the scheduler to schedule the deferred transactions if it is true.
func NewScheduler(profileManager *callee.ProfileManager) (*Scheduler, error) {
	return &Scheduler{
		profileManager: profileManager,
	}, nil
}

// The scheduler will optimize the given transactions and return a schedule.
// The schedule will contain the transactions that can be executed in parallel and the ones that have to
// be executed sequentially.
func (this *Scheduler) New(stdMsgs []*eucommon.StandardMessage) *workload.Schedule {
	// Get the static schedule for the given transactions first.
	sch, profiledMsgs := this.StaticSchedule(stdMsgs) // The profiledMsgs are the transactions that need to be scheduled to avoid conflicts.
	if len(profiledMsgs) == 0 {
		return sch // No known conflicts and no deferred transactions.
	}

	// Sort the callees by the number of conflicts and the profile index in ascending order.
	sort.Slice(profiledMsgs, func(i, j int) bool {
		lft, _ := this.profileManager.Find(profiledMsgs[i].First)
		rgt, _ := this.profileManager.Find(profiledMsgs[j].First)

		if lft.NumConflicts() != rgt.NumConflicts() {
			return lft.NumConflicts() < rgt.NumConflicts()
		}
		return profiledMsgs[i].Second.ID < profiledMsgs[j].Second.ID
	})

	// The code below will search for the parallel transaction set from a set of conflicting transactions.
	// Whataever left is the sequential transaction set after this.
	for {
		// The conflict dictionary of all indices of the current transaction set.
		seedMsg := *slice.PopFront(&profiledMsgs)
		paraMsgs := assoc.Pairs[uint64, *eucommon.StandardMessage]{seedMsg} // A set of conflict free transactions, starting with the first profile.

		// Load the conflict dictionary with the conflicts of the FIRST profile, from which the search will start.
		// Add the first profile's conflicts to the conflict dictionary.
		seedCallee, _ := this.profileManager.Find(seedMsg.First)
		lftConflictList := mapi.FromSlice(seedCallee.ConflictWith, func(k uint64) bool { return true })
		paraMsgIds := mapi.FromSlice(paraMsgs.Firsts(), func(_ uint64) bool {
			return true
		})

		// Look for the parallel transactions that aren't conflicting with the current set of transactions.
		for i := 0; i < len(profiledMsgs); i++ {
			targetMsg := profiledMsgs[i]

			// The current profile isn't conflicting with any transaction in the unique profile set
			otherCallee, _ := this.profileManager.Find(targetMsg.First)
			if !lftConflictList[targetMsg.First] && !mapi.ContainsAny(paraMsgIds, otherCallee.ConflictWith) {
				// Add the new profile's conflicts to the conflict dictionary.
				mapi.Insert(lftConflictList, otherCallee.ConflictWith, func(_ int, k uint64) (uint64, bool) {
					return k, true
				})

				paraMsgs = append(paraMsgs, targetMsg) // Add the current profile to the parallel transaction set.
				paraMsgIds[targetMsg.First] = true
				slice.RemoveAt(&profiledMsgs, i) // Remove the current profile, since it is already in the parallel set.
				i--
			}
		}

		// One transaction, no need to continue.
		if len(paraMsgs) == 1 {
			sch.WithConflict = append(sch.WithConflict, paraMsgs.Seconds()...)
			break
		}

		// Look for the deferred transactions and add them to the deferred transaction set.
		deferredGen := this.ScheduleDeferred(&paraMsgs)

		paraGen := slices.Transform(paraMsgs.Seconds(), func(i int, v *eucommon.StandardMessage) []*eucommon.StandardMessage {
			return []*eucommon.StandardMessage{v}
		})

		sch.MsgSet = append(sch.MsgSet, paraGen)
		sch.MsgSet = append(sch.MsgSet, deferredGen) // Insert the parallel transaction first

		// new(workload.JobSequence).FromStandardMessages(0, paraGen)

		// Remove the already schedule transaction from the profiledMsgs slice.
		if len(profiledMsgs) == 0 {
			break // Nothing left to process.
		}
	}

	// Whatever left in the profiledMsgs array is the sequential transaction set.
	sch.WithConflict = append(sch.WithConflict, (*assoc.Pairs[uint64, *eucommon.StandardMessage])(&profiledMsgs).Seconds()...)
	return sch
}

// The scheduler will scan through and look for multipl instances of the same profile and put one of them in the second
// consecutive set of transactions for deferred execution.
func (this *Scheduler) ScheduleDeferred(paraMsgInfo *assoc.Pairs[uint64, *eucommon.StandardMessage]) [][]*eucommon.StandardMessage {
	sort.SliceStable(*paraMsgInfo, func(i, j int) bool {
		if (*paraMsgInfo)[i].First != (*paraMsgInfo)[j].First {
			return (*paraMsgInfo)[i].First < (*paraMsgInfo)[j].First
		}
		return (*paraMsgInfo)[i].Second.ID < (*paraMsgInfo)[j].Second.ID
	})

	array := ([]*assoc.Pair[uint64, *eucommon.StandardMessage])(*paraMsgInfo)
	_, msgSets := slice.GroupBy(array, func(i int, pair *assoc.Pair[uint64, *eucommon.StandardMessage]) *string {
		key := schcommon.DeriveKey(pair.Second.Native.To[:], pair.Second.Native.Data[:])
		v := string(key[:])
		return &v
	})

	// Remove single instances or non-deferable ones.
	slice.RemoveIf(&msgSets, func(i int, pairs []*assoc.Pair[uint64, *eucommon.StandardMessage]) bool {
		return len(pairs) == 1 || !pairs[0].Second.IsDeferred
	})

	deferredGen := [][]*eucommon.StandardMessage{} // The deferred transaction generation.
	for _, msgs := range msgSets {
		def := *slice.PopBack(&msgs)
		deferredGen = append(deferredGen, []*eucommon.StandardMessage{def.Second}) // Add the deferred transaction to the new generation.
	}
	return deferredGen
}

// The scheduler will StaticSchedule base on some predefined rules for specific transaction types,
// such as transfers and contract deployments.
func (this *Scheduler) StaticSchedule(stdMsgs []*eucommon.StandardMessage) (*workload.Schedule, []*assoc.Pair[uint64, *eucommon.StandardMessage]) {
	sch := &workload.Schedule{}
	if len(stdMsgs) == 0 {
		return sch, []*assoc.Pair[uint64, *eucommon.StandardMessage]{} // No transactions to process.
	}

	// Transfers won't have any conflicts, as long as they have enough balances.
	// Deployments are conflict-free as well.
	sch.Transfers = slice.MoveIf(&stdMsgs, func(i int, msg *eucommon.StandardMessage) bool { return len(msg.Native.Data) == 0 })
	sch.Deployments = slice.MoveIf(&stdMsgs, func(i int, msg *eucommon.StandardMessage) bool { return msg.Native.To == nil })

	if len(stdMsgs) == 0 {
		return sch, []*assoc.Pair[uint64, *eucommon.StandardMessage]{} // All the transactions are transfers.
	}

	// Get the IDs for the given addresses and signatures, which will be used to find the profile index.
	// To save memory, the callees are stored in the dictionary by their IDs, not by their addresses and signatures.
	msgPairs := slice.ParallelTransform(stdMsgs, 8, func(i int, msg *eucommon.StandardMessage) *assoc.Pair[uint64, *eucommon.StandardMessage] {
		// Convert the address and signature to a unique key.
		uid := schcommon.DeriveUID((*msg.Native.To)[:], msg.Native.Data[:])
		return &assoc.Pair[uint64, *eucommon.StandardMessage]{First: uid, Second: stdMsgs[i]}
	})

	if len(msgPairs) == 0 {
		return sch, msgPairs
	}

	// Move the transactions that have no known conflicts to the parallel trasaction array first.
	// If a profile has no known conflicts with anyone else, it is either a conflict-free implementation or
	// has been fortunate enough to avoid conflicts so far.
	unknows := slice.MoveIf(&msgPairs, func(_ int, v *assoc.Pair[uint64, *eucommon.StandardMessage]) bool {
		_, ok := this.profileManager.Find(v.First)
		return !ok // No profile found in the dictionary.
	})
	sch.Unknowns = (*assoc.Pairs[uint64, *eucommon.StandardMessage])(&unknows).Seconds()

	// Sequential only callees.
	sequentialOnly := slice.MoveIf(&msgPairs, func(_ int, v *assoc.Pair[uint64, *eucommon.StandardMessage]) bool {
		// The profile isn't new, otherwise the v.First would be math.MaxUint64.
		if profile, ok := this.profileManager.Find(v.First); ok {
			return profile.ParallelismDegree == 1
		}
		return false
	})
	sch.Unknowns = (*assoc.Pairs[uint64, *eucommon.StandardMessage])(&unknows).Seconds()
	sch.Sequentials = (*assoc.Pairs[uint64, *eucommon.StandardMessage])(&sequentialOnly).Seconds()

	return sch, msgPairs
}
