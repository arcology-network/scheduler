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

package workload

import (
	"errors"

	common "github.com/arcology-network/common-lib/common"
	statecell "github.com/arcology-network/common-lib/crdt/statecell"
	"github.com/arcology-network/common-lib/exp/slice"
	statecommon "github.com/arcology-network/state-engine/common"
)

// ┌───────────────────────────────────────────────┐   ┌───────────────────────────────────────────────┐
// │ generation 0                                  │   │ generation 1 (executes after generation 0)    │
// │ (all parallel Sequences run concurrently)     │   │ (all parallel Sequences run concurrently)     │
// │                                               │   │                                               │
// │   ┌─────────────────────────────────────────┐ │   │   ┌─────────────────────────────────────────┐ │
// │   │ parallel Job Sequence 0                 │ │   │   │ parallel Job Sequence 0                 │ │
// │   │   tx_0_0_0 → tx_0_0_1 → tx_0_0_2 → ...  │ │   │   │   tx_1_0_0 → tx_1_0_1 → tx_1_0_2 → ...  │ │
// │   │   (Tx are processed sequentially)       │ │   │   │   (Tx are processed sequentially)       │ │
// │   └─────────────────────────────────────────┘ │   │   └─────────────────────────────────────────┘ │
// │                                               │   │                                               │
// │   ┌─────────────────────────────────────────┐ │   │   ┌─────────────────────────────────────────┐ │
// │   │ parallel Job Sequence 1                 │ │   │   │ parallel Job Sequence 1                 │ │
// │   │   tx_0_1_0 → tx_0_1_1 → tx_0_1_2 → ...  │ │   │   │   tx_1_1_0 → tx_1_1_1 → tx_1_1_2 → ...  │ │
// │   │   (Tx are processed sequentially)       │ │   │   │   (Tx are processed sequentially)       │ │
// │   └─────────────────────────────────────────┘ │   │   └─────────────────────────────────────────┘ │
// │                                               │   │                                               │
// │   ┌─────────────────────────────────────────┐ │   │   ┌─────────────────────────────────────────┐ │
// │   │ parallel Job Sequence 2                 │ │   │   │ parallel Job Sequence 2                 │ │
// │   │   tx_0_2_0 → tx_0_2_1 → tx_0_2_2 → ...  │ │   │   │   tx_1_2_0 → tx_1_2_1 → tx_1_2_2 → ...  │ │
// │   │   (Tx are processed sequentially)       │ │   │   │   (Tx are processed sequentially)       │ │
// │   └─────────────────────────────────────────┘ │   │   └─────────────────────────────────────────┘ │
// │                                               │   │                                               │
// │   ... more parallel Sequences                 │   │   ... more parallel Sequences                 │
// └───────────────────────────────────────────────┘   └───────────────────────────────────────────────┘

type Generation struct {
	ID           uint64
	numThreads   uint32
	JobSeqs      []*JobSequence          // para jobSeqs
	JobSeqLookup map[uint64]*JobSequence // lookup by message ID in job sequences. Multiple messages may map to the same job sequence.

	// CalleeFreq tracks how many job sequences invoke the same (address, selector)
	// as their first transaction. Used to identify high-contention callees for
	// scheduling and conflict-resolution heuristics.
	CalleeFreq map[uint64]int
}

func NewGeneration(id uint64, numThreads uint32, jobSeqs []*JobSequence) *Generation {
	gen := &Generation{
		ID:           id,
		numThreads:   numThreads,
		JobSeqs:      jobSeqs,
		JobSeqLookup: make(map[uint64]*JobSequence),
		CalleeFreq:   make(map[uint64]int),
	}

	// Build the message lookup map. So we can use it later to find the transactions to revert.
	for _, seq := range jobSeqs {
		for _, job := range seq.Jobs {
			gen.JobSeqLookup[job.StdMsg.ID] = seq
		}
	}
	return gen
}

// CountCalleeCalleeFreq computes how many job sequences invoke the same
// (address, selector) pair for their *first* transaction. The first job in a
// sequence defines the sequence’s callee identity and is treated as the
// representative callee for conflict hotspot detection.
//
// Rationale:
// - Sequences operate as atomic units in the scheduler.
// - Conflicts across sequences originate from their initial callee.
// - Later jobs in a sequence are irrelevant to inter-sequence contention.
//
// For each JobSequence:
//  1. Extract the callee (address + selector) of the first job.
//  2. Derive its UID (stable identifier).
//  3. Increment the occurrence count for that UID.
//
// The resulting CalleeFreq map is used to identify high-contention callees
// and guide downstream scheduling and conflict-resolution heuristics.
func (this *Generation) CountCalleeCalleeFreq() {
	pathBuilder := statecommon.PathBuilder{}
	for _, seq := range this.JobSeqs {
		for _, job := range seq.Jobs {
			pathBuilder.Address, pathBuilder.Selector = job.StdMsg.GetAddressAndSelector()
			this.CalleeFreq[pathBuilder.DeriveUID()]++ // Only count the first one if found
			break
		}
	}
}

// GetClearedTransitions returns all the conflict-free transitions in the generation.
/*
Example:
    seq0:  Tx0 → Tx1★ → Tx2★ → Tx3★
                 ▲
				 └─────┐ conflict (Tx6 → Tx1)
    seq1:  Tx4 → Tx5 → Tx6★ → Tx7★

Rollback rule:
    - All jobs after (and including) Tx1 in seq0 must revert:  [Tx1, Tx2, Tx3] or [Tx6, Tx7] will.
Note:
	This is NOT optimal since no all the jobs after the conflicting job are contaminated.
	But it is simple and effective for now.
*/
func (this *Generation) GetClearedTransitions(txLookup, seqLookup map[uint64]uint64) []*statecell.StateCell {
	cleanTrans := slice.Concate(this.JobSeqs, func(seq *JobSequence) []*statecell.StateCell {
		// Check if the sequence ID is in the conflict list.
		// If yes, locate the conflicting transactions in the sequence and mark all the
		// transactions after the conflicting TX as conflicted as well.
		if _, ok := seqLookup[seq.ID]; ok {
			(*seq).FlagConflict(txLookup, errors.New(statecommon.WARN_ACCESS_CONFLICT))
		}
		return (*seq).GetClearedTransition() // Return the conflict-free transitions
	})
	return cleanTrans
}

func (this *Generation) Length() uint64 { return uint64(len(this.JobSeqs)) }

// Get unique transction IDs in this generation.
func (this *Generation) MsgIDs() []uint64 {
	msgIDs := make([]uint64, 0, len(this.JobSeqs))
	for _, seq := range this.JobSeqs {
		msgIDs = append(msgIDs, seq.MsgIDs()...)
	}
	return msgIDs
}

func (this *Generation) At(idx uint64) *JobSequence {
	return common.IfThenDo1st(idx < uint64(len(this.JobSeqs)), func() *JobSequence { return this.JobSeqs[idx] }, nil)
}

func (*Generation) New(id uint64, numThreads uint32, jobSeqs []*JobSequence) *Generation {
	return NewGeneration(id, numThreads, slice.To[*JobSequence, *JobSequence](jobSeqs))
}

func (this *Generation) Add(jobSeq *JobSequence) bool {
	this.JobSeqs = append(this.JobSeqs, jobSeq)
	return true
}

func (this *Generation) Clear() uint64 {
	length := len(this.JobSeqs)
	this.JobSeqs = this.JobSeqs[:0]
	return uint64(length)
}
