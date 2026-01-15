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
	"sort"

	common "github.com/arcology-network/common-lib/common"
	statecell "github.com/arcology-network/common-lib/crdt/statecell"
	mapi "github.com/arcology-network/common-lib/exp/map"
	"github.com/arcology-network/common-lib/exp/slice"

	// "github.com/arcology-network/scheduler/workload"

	associative "github.com/arcology-network/common-lib/exp/associative"
	ethcommon "github.com/ethereum/go-ethereum/common"
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
	ID         uint64
	numThreads uint32
	JobSeqs    []*JobSequence // para jobSeqs

	// lookup by Tx ID in job sequences. Multiple Tx may map to the same job sequence.
	TxToSeqLookup map[uint64]*JobSequence
	TxToJobLookup map[uint64]*Job // lookup by Tx ID in jobs.

	// CalleeFreq tracks how many job sequences invoke the same (address, selector)
	// as their first transaction. Used to identify high-contention callees for
	// scheduling and conflict-resolution heuristics.
	CalleeFreq map[uint64]int

	// Jobs from the same sender may span multiple job sequences.
	// So we need to group them together for nonce offset insertion.
	//
	// For example:
	// Sequence 0: Tx0(from A) -> Tx1(from A) -> Tx2(from B)
	// Sequence 1: Tx3(from A) -> Tx4(from C)
	//
	// The SenderToSequenceLookup would be:
	// A: [(Sequence 0, [Tx0, Tx1]), (Sequence 1, [Tx3])]
	// B: [(Sequence 0, [Tx2])]
	// C: [(Sequence 1, [Tx4])]
	//
	// This allows us to correctly insert nonce offsets for transactions from the same sender
	// across different job sequences.
	SenderToSequenceLookup map[ethcommon.Address][]associative.Pair[*JobSequence, []*Job]
}

func NewGeneration(numThreads uint32, jobSeqs []*JobSequence) *Generation {
	gen := &Generation{
		numThreads:    numThreads,
		JobSeqs:       jobSeqs,
		TxToSeqLookup: make(map[uint64]*JobSequence),
		TxToJobLookup: make(map[uint64]*Job),
		CalleeFreq:    make(map[uint64]int),
	}

	// Build the message lookup map. So we can use it later to find the transactions to revert.
	for _, seq := range jobSeqs {
		for _, job := range seq.Jobs {
			gen.TxToSeqLookup[job.StdMsg.ID] = seq // Multiple jobs may map to the same job sequence.
			gen.TxToJobLookup[job.StdMsg.ID] = job
		}
	}
	return gen
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
func (this *Generation) GetClearRecords() []*statecell.StateCell {
	cleanRecords := slice.Concate(this.JobSeqs, func(seq *JobSequence) []*statecell.StateCell {
		return (*seq).GetSuccessfulTxRecords() // Return the conflict-free transitions
	})
	return cleanRecords
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

func (*Generation) New(numThreads uint32, jobSeqs []*JobSequence) *Generation {
	return NewGeneration(numThreads, slice.To[*JobSequence, *JobSequence](jobSeqs))
}

func (this *Generation) Add(jobSeq *JobSequence) bool {
	this.JobSeqs = append(this.JobSeqs, jobSeq)
	return true
}

// Return a 2D slice of jobs grouped by sender address.
// Jobs from the same sender may belong to different job sequences.
// So we need to further group them by job sequence, so we can insert
// nonce offsets later.
//
// For example:
// Sequence 0: Tx0(from A) -> Tx1(from A) -> Tx2(from B)
// Sequence 1: Tx3(from A) -> Tx4(from C)
//
// The SenderToSequenceLookup would be:
// A: [(Sequence 0, [Tx0, Tx1]), (Sequence 1, [Tx3])]
// B: [(Sequence 0, [Tx2])]
// C: [(Sequence 1, [Tx4])]
// Each address may map to multiple (job sequence, jobs) pairs.
func (this *Generation) GroupBySenderAndSequence() ([]ethcommon.Address, [][]associative.Pair[*JobSequence, []*Job]) {
	jobs := []*Job{}
	for _, seq := range this.JobSeqs {
		jobs = append(jobs, seq.Jobs...)
	}

	// Get jobs grouped by sender address.
	_, jobsBySenders := slice.GroupBy(jobs,
		func(_ int, job *Job) *ethcommon.Address {
			return &job.StdMsg.Native.From
		})

	// Sub-group jobs by job sequence within each sender group.
	this.SenderToSequenceLookup = make(map[ethcommon.Address][]associative.Pair[*JobSequence, []*Job])
	for _, singleSenderJobs := range jobsBySenders {
		jobSeqs, JobsBySenderSequence := slice.GroupBy(singleSenderJobs,
			func(_ int, job *Job) **JobSequence {
				jobSeq := this.TxToSeqLookup[job.StdMsg.ID]
				return &jobSeq
			})

		groupedJobs := make([]associative.Pair[*JobSequence, []*Job], len(jobSeqs))
		for i, jobSeq := range jobSeqs {
			sort.Slice(JobsBySenderSequence[i], func(j, k int) bool {
				return JobsBySenderSequence[i][j].StdMsg.Native.Nonce <
					JobsBySenderSequence[i][k].StdMsg.Native.Nonce
			})

			groupedJobs[i] = associative.Pair[*JobSequence, []*Job]{
				First:  jobSeq,
				Second: JobsBySenderSequence[i],
			}
		}
		senderAddr := groupedJobs[0].First.Jobs[0].StdMsg.Native.From
		this.SenderToSequenceLookup[senderAddr] = groupedJobs
	}

	return mapi.KVs(this.SenderToSequenceLookup)
}

// Return a 2D slice of jobs grouped by their job sequences.
// func (this *Generation) JobsBySequences(jobs []*Job) []associative.Pair[*JobSequence, []*Job] {
// 	jobs := []*Job{}
// 	for _, seq := range this.JobSeqs {
// 		jobs = append(jobs, seq.Jobs...)
// 	}

// 	_, msgSet := slice.GroupBy(jobs,
// 		func(_ int, job *Job) *ethcommon.Address {
// 			return &job.StdMsg.Native.From
// 		})
// 	return msgSet
// }

func (this *Generation) Clear() uint64 {
	length := len(this.JobSeqs)
	this.JobSeqs = this.JobSeqs[:0]
	return uint64(length)
}
