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
	"math"
	"sort"

	"github.com/arcology-network/common-lib/codec"
	associative "github.com/arcology-network/common-lib/exp/associative"
	mapi "github.com/arcology-network/common-lib/exp/map"
	slice "github.com/arcology-network/common-lib/exp/slice"
	eucommon "github.com/arcology-network/common-lib/types"
	stgcommon "github.com/arcology-network/storage-committer/common"
	"github.com/arcology-network/storage-committer/type/univalue"
)

type Scheduler struct {
	calleeDict     map[string]uint32 // A calleeDict table to find the index of a calleeDict by its address + signature.
	callees        []*Callee
	deferByDefault bool // If the scheduler should schedule the deferred transactions by default.
}

// Initialize a new scheduler, the fildb is the file path to the scheduler's conflict database and the deferByDefault
// instructs the scheduler to schedule the deferred transactions if it is true.
func NewScheduler(fildb string, deferByDefault bool) (*Scheduler, error) {
	return &Scheduler{
		calleeDict:     make(map[string]uint32),
		deferByDefault: deferByDefault,
	}, nil
}

func (this *Scheduler) Import(propertyTransitions []*univalue.Univalue) {
	for _, v := range propertyTransitions {
		_, addr, sign := new(Callee).parseCalleeSignature(*v.GetPath())
		_, callee, _ := this.Find(codec.Bytes20{}.FromBytes(addr[:]), codec.Bytes4{}.FromSlice(sign[:]))
		callee.init(v)
	}
}

// The function will find the index of the entry by its address and signature.
// If the entry is found, the index will be returned. If the entry is not found, the index will be added to the scheduler.
func (this *Scheduler) Find(addr [20]byte, sig [4]byte) (uint32, *Callee, bool) {
	lftKey := string(append(addr[:stgcommon.SHORT_CONTRACT_ADDRESS_LENGTH], sig[:]...)) // Join the address and signature to create a unique key.
	idx, ok := this.calleeDict[lftKey]
	if !ok {
		idx = uint32(len(this.callees))
		this.callees = append(this.callees, NewCallee(idx, addr[:], sig[:]))
		this.calleeDict[lftKey] = idx
	}
	return idx, this.callees[idx], ok
}

// Add a conflict pair to the scheduler
func (this *Scheduler) Add(lftAddr [20]byte, lftSig [4]byte, rgtAddr [20]byte, rgtSig [4]byte) bool {
	lftIdx, _, lftExist := this.Find(lftAddr, lftSig)
	rgtIdx, _, rgtExist := this.Find(rgtAddr, rgtSig)

	if lftExist && rgtExist {
		return false // The conflict pair is already recorded.
	}

	this.callees[lftIdx].Indices = append(this.callees[lftIdx].Indices, rgtIdx)
	this.callees[rgtIdx].Indices = append(this.callees[rgtIdx].Indices, lftIdx)
	return true
}

// Add a deferred function to the scheduler.
func (this *Scheduler) AddDeferred(addr [20]byte, funSig [4]byte) {
	idx, _, _ := this.Find(addr, funSig)
	this.callees[idx].Deferrable = true
}

// The scheduler will optimize the given transactions and return a schedule.
// The schedule will contain the transactions that can be executed in parallel and the ones that have to
// be executed sequentially.
func (this *Scheduler) New(stdMsgs []*eucommon.StandardMessage) *Schedule {
	// Get the static schedule for the given transactions first.
	sch, msgPairs := this.StaticSchedule(stdMsgs)
	if len(msgPairs) == 0 {
		return sch // No known conflicts and no deferred transactions.
	}

	// Sort the callees by the number of conflicts and the callee index in ascending order.
	// Need to use pairs not msgPairs
	sort.Slice(msgPairs, func(i, j int) bool {
		lft, rgt := len(this.callees[(msgPairs)[i].First].Indices), len(this.callees[(msgPairs)[j].First].Indices)
		if lft < rgt {
			return lft < rgt
		}
		return (msgPairs)[i].Second.ID < (msgPairs)[j].Second.ID
	})

	// The code below will search for the parallel transaction set from a set of conflicting transactions.
	// Whataever left is the sequential transaction set after this.
	for {
		// The conflict dictionary of all indices of the current transaction set.
		paraSet := map[uint32]*associative.Pair[uint32, *eucommon.StandardMessage]{} // All entries in the set are conflict free.
		paraSet[(msgPairs)[0].First] = (msgPairs)[0]                                 // Start with adding the first callee to the set.
		paraMsgs := associative.Pairs[uint32, *eucommon.StandardMessage]{(msgPairs)[0]}

		// Load the conflict dictionary with the conflicts of the FIRST callee, from which the seach will start.
		// Add the first callee's conflicts to the conflict dictionary.
		conflictDict := mapi.FromSlice(this.callees[(msgPairs)[0].First].Indices, func(k uint32) bool { return true })

		// Look for the parallel transactions that aren't conflicting with the current set of transactions.
		for i := 1; i < len(msgPairs); i++ {
			msgToInclude := msgPairs[i]
			// If the current callee is already in the set, then skip it.
			calleeInfo := paraSet[msgToInclude.First]
			if calleeInfo != nil {
				continue // Will conflict with the current set. Skip it.
			}

			// The current callee isn't conflicting with any transaction in the current set
			if !conflictDict[msgToInclude.First] && !mapi.ContainsAny(paraSet, this.callees[msgToInclude.First].Indices) {
				// Add the new callee's conflicts to the conflict dictionary.
				mapi.Insert(conflictDict, this.callees[msgToInclude.First].Indices, func(_ int, k uint32) (uint32, bool) {
					return k, true
				})

				// Record conflict info
				paraSet[msgToInclude.First] = msgToInclude // Add the current callee to the set.
				paraMsgs = append(paraMsgs, msgToInclude)  // Add the current callee to the parallel transaction set.
				slice.RemoveAt(&msgPairs, i)               // Remove the current callee, since it is already in the parallel set.
			}
		}

		// If it only contains one transaction, then there is no need to continue.
		if len(paraMsgs) == 1 {
			break
		}

		// Look for the deferred transactions and add them to the deferred transaction set.
		sch.Generations = append(sch.Generations, paraMsgs.Seconds()) // Insert the parallel transaction first

		deferred := this.ScheduleDeferred(&paraMsgs)
		if len(deferred) > 0 {
			sch.Generations = append(sch.Generations, deferred.Seconds()) // Insert the deferred transaction set to the next generation.
		}

		// Remove the already schedule transaction from the msgPairs slice.
		if len(slice.RemoveAt(&msgPairs, 0)) == 0 {
			break // Nothing left to process.
		}
	}

	// Deferred array can be empty, so remove it if it is.
	slice.RemoveIf(&sch.Generations, func(i int, v []*eucommon.StandardMessage) bool {
		return len(v) == 0
	})

	// Whatever left in the msgPairs array is the sequential transaction set.
	sch.WithConflict = (*associative.Pairs[uint32, *eucommon.StandardMessage])(&msgPairs).Seconds()
	return sch
}

// The scheduler will scan through and look for multipl instances of the same callee and put one of them in the second
// consecutive set of transactions for deferred execution.
func (this *Scheduler) ScheduleDeferred(paraMsgInfo *associative.Pairs[uint32, *eucommon.StandardMessage]) associative.Pairs[uint32, *eucommon.StandardMessage] {
	sort.Slice(*paraMsgInfo, func(i, j int) bool {
		if (*paraMsgInfo)[i].First != (*paraMsgInfo)[j].First {
			return (*paraMsgInfo)[i].First < (*paraMsgInfo)[j].First
		}
		return (*paraMsgInfo)[i].Second.ID < (*paraMsgInfo)[j].Second.ID
	})

	deferredMsgs := associative.Pairs[uint32, *eucommon.StandardMessage]{} // An empty deferred transaction set.
	for i := 0; i < len(*paraMsgInfo); i++ {
		// Find the first and last instance of the same callee.
		first, _ := slice.FindFirstIf(*paraMsgInfo, func(_ int, v *associative.Pair[uint32, *eucommon.StandardMessage]) bool {
			return (*paraMsgInfo)[i].First == v.First
		})

		// Find the first and last instance of the same callee.
		last, deferred := slice.FindLastIf(*paraMsgInfo, func(_ int, v *associative.Pair[uint32, *eucommon.StandardMessage]) bool {
			return (*paraMsgInfo)[i].First == v.First
		})

		// If the first and last index of the same callee are different, then
		// more than one instance of the same callee is there.
		if first != last && this.deferByDefault {
			key := Compact((*paraMsgInfo)[i].Second.Native.To[:], (*paraMsgInfo)[i].Second.Native.Data[:])

			if v, ok := this.calleeDict[string(key)]; ok && this.callees[v].Deferrable {
				deferredMsgs = append(deferredMsgs, *deferred)
				slice.RemoveAt(paraMsgInfo.Slice(), last) // Move the last call to the second generation as a deferred call.
			}
		}
	}
	return deferredMsgs
}

// The scheduler will StaticSchedule base on some predefined rules for specific transaction types.
func (this *Scheduler) StaticSchedule(stdMsgs []*eucommon.StandardMessage) (*Schedule, []*associative.Pair[uint32, *eucommon.StandardMessage]) {
	sch := &Schedule{}
	if len(stdMsgs) == 0 {
		return sch, []*associative.Pair[uint32, *eucommon.StandardMessage]{} // No transactions to process.
	}

	// Transfers won't have any conflicts, as long as they have enough balances. Deployments are less likely to have conflicts, but it's not guaranteed.
	sch.Transfers = slice.MoveIf(&stdMsgs, func(i int, msg *eucommon.StandardMessage) bool { return len(msg.Native.Data) == 0 })
	sch.Deployments = slice.MoveIf(&stdMsgs, func(i int, msg *eucommon.StandardMessage) bool { return msg.Native.To == nil })

	if len(stdMsgs) == 0 {
		return sch, []*associative.Pair[uint32, *eucommon.StandardMessage]{} // All the transactions are transfers.
	}

	// Get the IDs for the given addresses and signatures, which will be used to find the callee index.
	// To save memory, the callees are stored in the dictionary by their IDs, not by their addresses and signatures.
	pairs := slice.ParallelTransform(stdMsgs, 8, func(i int, msg *eucommon.StandardMessage) *associative.Pair[uint32, *eucommon.StandardMessage] {

		// Convert the address and signature to a unique key.
		key := Compact((*msg.Native.To)[:], msg.Native.Data[:])
		idx, ok := this.calleeDict[string(key)] // Check if the key is already in the calleeDict.
		if !ok {
			idx = math.MaxUint32 // The callee is new, no known conflicts.
		}
		return &associative.Pair[uint32, *eucommon.StandardMessage]{First: idx, Second: stdMsgs[i]}
	})

	if len(pairs) == 0 {
		return sch, pairs
	}

	// Move the transactions that have no known conflicts to the parallel trasaction array first.
	// If a callee has no known conflicts with anyone else, it is either a conflict-free implementation or
	// has been fortunate enough to avoid conflicts so far.
	unknows := slice.MoveIf(&pairs, func(_ int, v *associative.Pair[uint32, *eucommon.StandardMessage]) bool {
		return v.First == math.MaxUint32
	})
	sch.Unknows = (*associative.Pairs[uint32, *eucommon.StandardMessage])(&unknows).Seconds()

	// Deployments are less likely to have conflicts, but it's not guaranteed.
	sequentialOnly := slice.MoveIf(&pairs, func(_ int, v *associative.Pair[uint32, *eucommon.StandardMessage]) bool {
		if v.First < uint32(len(this.callees)) {
			return this.callees[v.First].Sequential
		}
		return false
	})
	sch.Unknows = (*associative.Pairs[uint32, *eucommon.StandardMessage])(&unknows).Seconds()
	sch.Sequentials = (*associative.Pairs[uint32, *eucommon.StandardMessage])(&sequentialOnly).Seconds()

	return sch, pairs
}
