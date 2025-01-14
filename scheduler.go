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
	callees        []*Callee         // For deterministic scheduling results!!!!
	deferByDefault bool              // If the scheduler should schedule the deferred transactions by default.
}

// Initialize a new scheduler, the fildb is the file path to the scheduler's conflict database and the deferByDefault
// instructs the scheduler to schedule the deferred transactions if it is true.
func NewScheduler(fildb string, deferByDefault bool) (*Scheduler, error) {
	return &Scheduler{
		calleeDict:     make(map[string]uint32),
		deferByDefault: deferByDefault,
	}, nil
}

// Import callee information from the given property transitions generated by the EUs.
func (this *Scheduler) Import(propertyTransitions []*univalue.Univalue) {
	for _, v := range propertyTransitions {
		_, addr, sign := new(Callee).parseCalleeSignature(*v.GetPath())
		if len(addr) == 0 || len(sign) == 0 {
			continue
		}
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
	lftIdx, _, _ := this.Find(lftAddr, lftSig)
	rgtIdx, _, _ := this.Find(rgtAddr, rgtSig)

	if !slice.Contains(this.callees[lftIdx].Indices, rgtIdx, func(a uint32, b uint32) bool {
		return a == b
	}) {
		this.callees[lftIdx].Indices = append(this.callees[lftIdx].Indices, rgtIdx)
	}
	if !slice.Contains(this.callees[rgtIdx].Indices, lftIdx, func(a uint32, b uint32) bool {
		return a == b
	}) {
		this.callees[rgtIdx].Indices = append(this.callees[rgtIdx].Indices, lftIdx)
	}
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
		seedCallee := *slice.PopFront(&msgPairs)
		paraMsgs := associative.Pairs[uint32, *eucommon.StandardMessage]{seedCallee} // A set of conflict free transactions, starting with the first callee.

		// Load the conflict dictionary with the conflicts of the FIRST callee, from which the search will start.
		// Add the first callee's conflicts to the conflict dictionary.
		blacklist := mapi.FromSlice(this.callees[seedCallee.First].Indices, func(k uint32) bool { return true })
		paraMsgIds := mapi.FromSlice(paraMsgs.Firsts(), func(_ uint32) bool {
			return true
		})
		// Look for the parallel transactions that aren't conflicting with the current set of transactions.
		for i := 0; i < len(msgPairs); i++ {
			targetMsg := msgPairs[i]

			// The current callee isn't conflicting with any transaction in the unique callee set
			if !blacklist[targetMsg.First] && !mapi.ContainsAny(paraMsgIds, this.callees[targetMsg.First].Indices) {
				// Add the new callee's conflicts to the conflict dictionary.
				mapi.Insert(blacklist, this.callees[targetMsg.First].Indices, func(_ int, k uint32) (uint32, bool) {
					return k, true
				})

				paraMsgs = append(paraMsgs, targetMsg) // Add the current callee to the parallel transaction set.
				paraMsgIds[targetMsg.First] = true
				slice.RemoveAt(&msgPairs, i) // Remove the current callee, since it is already in the parallel set.
				i--
			}
		}

		// If it only contains one transaction, then there is no need to continue.
		if len(paraMsgs) == 1 {
			sch.WithConflict = append(sch.WithConflict, paraMsgs.Seconds()...)
			break
		}

		// Look for the deferred transactions and add them to the deferred transaction set.

		deferred := this.ScheduleDeferred(&paraMsgs)

		paraGen := slice.Transform(paraMsgs.Seconds(), func(_ int, msg *eucommon.StandardMessage) []*eucommon.StandardMessage {
			return []*eucommon.StandardMessage{msg}
		})

		sch.Generations = append(sch.Generations, paraGen) // Insert the parallel transaction first
		if len(deferred) > 0 {
			deferGen := slice.Transform(deferred.Seconds(), func(_ int, msg *eucommon.StandardMessage) []*eucommon.StandardMessage {
				return []*eucommon.StandardMessage{msg}
			})

			sch.Generations = append(sch.Generations, deferGen) // Insert the deferred transaction set to the next generation.
		}

		// Remove the already schedule transaction from the msgPairs slice.
		if len(msgPairs) == 0 {
			break // Nothing left to process.
		}
	}

	// Deferred array can be empty, so remove it if it is.
	slice.RemoveIf(&sch.Generations, func(i int, stdMsgs [][]*eucommon.StandardMessage) bool {
		return len(stdMsgs) == 0
	})

	// Whatever left in the msgPairs array is the sequential transaction set.
	sch.WithConflict = append(sch.WithConflict, (*associative.Pairs[uint32, *eucommon.StandardMessage])(&msgPairs).Seconds()...)
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
	i := 0

	first := 0
	last := 0
	for {

		// Find the first and last instance of the same callee.
		tmpfirst, _ := slice.FindFirstIf(*paraMsgInfo, func(_ int, v *associative.Pair[uint32, *eucommon.StandardMessage]) bool {
			return (*paraMsgInfo)[i].First == v.First
		})

		// Find the first and last instance of the same callee.
		tmplast, deferred := slice.FindLastIf(*paraMsgInfo, func(_ int, v *associative.Pair[uint32, *eucommon.StandardMessage]) bool {
			return (*paraMsgInfo)[i].First == v.First
		})

		if last != 0 && last == tmplast {
			break
		}
		first = tmpfirst
		last = tmplast

		// If the first and last index of the same callee are different, then
		// more than one instance of the same callee is there.
		// if first != last && this.deferByDefault {
		if first != last {
			key := Compact((*paraMsgInfo)[i].Second.Native.To[:], (*paraMsgInfo)[i].Second.Native.Data[:])

			if v, ok := this.calleeDict[string(key)]; this.deferByDefault || (ok && this.callees[v].Deferrable) {
				deferredMsgs = append(deferredMsgs, *deferred)
				slice.RemoveAt(paraMsgInfo.Slice(), last) // Move the last call to the second generation as a deferred call.
				i = last
			}
		} else {
			i = last + 1
		}
		if i >= len(*paraMsgInfo) {
			break
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
