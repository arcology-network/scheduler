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
	"sort"

	"github.com/arcology-network/common-lib/crdt/commutative"
	queue "github.com/arcology-network/common-lib/exp/queue"
	stateengine "github.com/arcology-network/state-engine"
	statecommon "github.com/arcology-network/state-engine/common"
)

type ExecutionPlan struct {
	Transfers   []*Job // Transfers
	Deployments []*Job // Contract deployments

	Unknowns     []*Job // Messages with unknown conflicts with others
	WithConflict []*Job // Messages with some known conflicts
	Sequentials  []*Job // Callees that are marked as sequential only

	currentGenIdx int
	Generations   []*Generation
	RawMsgSet     [][][]*Job

	// Where each job is located based on its StdMsg.ID. Multiple jobs may map to the same queue.
	// If the queue has multiple jobs, which are ordered by their nonces.
	JobLookup map[uint64]*Job

	// Jobs grouped by their sender addresses, jobs in the same queue are ordered by
	// their nonces.
	JobsBySender []*queue.Queue[*Job]
}

func NewExecutionPlan(gens []*Generation) *ExecutionPlan {
	sch := &ExecutionPlan{
		Generations: gens,
	}
	sch.BuildJobLookup()
	return sch
}

// GetMsgIDs constructs and returns a 3D slice containing the IDs of the messages.
// The structure of the returned slice is as follows:
// schedule[g][s][j]
// where:
// g = Generations run sequentially. Generation g+1 begins only after generation g completes.
// s = Sequences within a generation that can run in parallel.
// j = Transactions inside a job sequence must execute in strict sequential order.
func (this *ExecutionPlan) ExportMsgIDs(store *stateengine.StateStore) [][][]uint64 {
	result := [][][]uint64{}
	for _, gen := range this.Generations {
		genIDs := [][]uint64{}
		for _, seq := range gen.JobSeqs {
			seqIDs := []uint64{}
			for _, job := range seq.Jobs {
				seqIDs = append(seqIDs, job.StdMsg.ID)
			}
			genIDs = append(genIDs, seqIDs)
		}
		result = append(result, genIDs)
	}
	return result
}

// The function returns an optimized execution schedule represented as a 3-dimensional slice.
func (this *ExecutionPlan) Finalize(store *stateengine.StateStore) error {
	// Reassign IDs to generations, sequences, and jobs.
	for i, gen := range this.Generations {
		gen.ID = uint64(i)
		for j, seq := range gen.JobSeqs {
			seq.ID = uint64(j)
			for k, job := range seq.Jobs {
				job.ID = uint64(k)
				job.SeqID = seq.ID
			}
		}
	}

	this.BuildJobLookup() // Rebuild the message lookup.
	return this.InsertNonceOffsets(store)
}

// Insert nonce offsets for each job in the execution plan.
// Rationale: When processing transactions from the same sender that span multiple job sequences, it is possible
// that some transcations may be processed in parallel in different job sequences. They all the see the same initial nonce,
// This will lead to nonce conflicts during execution or nonce too high errors.
// To resolve this, we need to insert nonce offsets for each job based on its position among all jobs
// from the same sender in the generation.
func (this *ExecutionPlan) InsertNonceOffsets(store *stateengine.StateStore) error {
	var aggregatedErr error
	for _, gen := range this.Generations {
		senders, seqsFromSender := gen.GroupBySenderAndSequence() // Group jobs by sender address in the generation.
		for i, jobSeq := range seqsFromSender {
			if len(jobSeq) == 1 {
				continue
			}

			// Sort the job sequences by their IDs to ensure consistent ordering.
			sort.Slice(jobSeq, func(i, j int) bool {
				return jobSeq[i].First.ID < jobSeq[j].First.ID
			})

			// Jobs from the same sender may span multiple job sequences.
			// We need to insert nonce offsets for each job sequencebased on its position
			// among all jobs from the same sender in the generation.
			offset := uint64(0)
			for j, pair := range jobSeq {

				// Only offset nonces after the first entry.
				// The first job nonce is already correct.
				if j == 0 {
					continue
				}

				// Build the nonce offset for the job.
				first := pair.Second[0]

				// Build the nonce path for the sender, so
				// we can write it to the state cache.
				noncePath := (&statecommon.PathBuilder{
					Sender: senders[i],
				}).UnderSenderPath(statecommon.PATH_NONCE)

				if _, err := store.StateCache.Write(
					first.StdMsg.ID,
					noncePath,
					commutative.NewUint64Delta(uint64(offset))); err != nil {
					aggregatedErr = errors.Join(aggregatedErr, err)
				}
				offset += uint64(len(pair.Second))

				// Export the nonce offset state change.
				noncePreOffset := store.StateCache.Export()
				store.StateCache.Clear()

				// Append the nonce offset to the pre-state transitions of the job sequence.
				jobSeq := pair.First
				jobSeq.PreStateTransitions = append(jobSeq.PreStateTransitions, noncePreOffset...)
			}
		}
	}
	return aggregatedErr
}

// BuildJobLookup constructs a mapping from transaction IDs to their corresponding Job structs
// within the execution schedule. This allows for efficient retrieval of Job information
func (this *ExecutionPlan) BuildJobLookup() {
	this.JobLookup = make(map[uint64]*Job)
	// Build the message lookup map for the schedule.
	for _, gen := range this.Generations {
		for _, seq := range gen.JobSeqs {
			for _, job := range seq.Jobs {
				this.JobLookup[job.StdMsg.ID] = job
			}
		}
	}
}

func (this *ExecutionPlan) GetNextGeneration() *Generation {
	this.currentGenIdx++
	return this.Generations[this.currentGenIdx-1]
}
