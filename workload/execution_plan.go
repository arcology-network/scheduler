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

	crdtcommon "github.com/arcology-network/common-lib/crdt/common"
	"github.com/arcology-network/common-lib/crdt/commutative"
	statecell "github.com/arcology-network/common-lib/crdt/statecell"
	queue "github.com/arcology-network/common-lib/exp/queue"
	"github.com/arcology-network/common-lib/exp/slice"
	evmcommon "github.com/ethereum/go-ethereum/common"

	// "github.com/arcology-network/evm/common"
	storageintf "github.com/arcology-network/common-lib/storage/interface"
	statecommon "github.com/arcology-network/state-engine/common"
	statecache "github.com/arcology-network/state-engine/state/cache"
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

	// Jobs by their transaction IDs for quick lookup.
	JobIDLookup map[uint64]*Job

	// Jobs grouped by their sender addresses, jobs in the same queue are ordered by
	// their nonces.
	JobsBySender []*queue.Queue[*Job]

	Store *statecache.ExecutionStateStore
}

func NewExecutionPlan(gens []*Generation, store *statecache.ExecutionStateStore) *ExecutionPlan {
	sch := &ExecutionPlan{
		Generations: gens,
		Store:       store,
	}
	// sch.BuildJobLookup()
	return sch
}

// GetMsgIDs constructs and returns a 3D slice containing the IDs of the messages.
// The structure of the returned slice is as follows:
// schedule[g][s][j]
// where:
// g = Generations run sequentially. Generation g+1 begins only after generation g completes.
// s = Sequences within a generation that can run in parallel.
// j = Transactions inside a job sequence must execute in strict sequential order.
func (this *ExecutionPlan) ExportMsgIDs(store *statecache.ExecutionStateStore) [][][]uint64 {
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

func (this *ExecutionPlan) TotalJobs() uint64 {
	total := uint64(0)
	for _, gen := range this.Generations {
		total += gen.NumJobs()
	}
	return total
}

// func (this *ExecutionPlan) C() error {

// The function returns an optimized execution schedule represented as a 3-dimensional slice.
func (this *ExecutionPlan) Finalize() error {
	for b, baseGen := range this.Generations {
		if baseGen.NumJobs() == 0 {
			continue
		}

		for _, targetGen := range this.Generations[b+1:] {
			for j := 0; j < len(targetGen.JobSeqs); j++ {
				for k, job := range targetGen.JobSeqs[j].Jobs {
					if job.Profile.IsDeferrable() {
						continue
					}

					conflictSeqs := baseGen.HasConflictWith(job)
					if len(conflictSeqs) == 1 { // make sense when the job has conflicts with  one job sequence.
						conflictSeqs[0].Jobs = append(conflictSeqs[0].Jobs, job)
						targetGen.JobSeqs[j].Jobs[k] = nil
					}
				}
			}
			targetGen.ClearEmptySequences()
		}
	}

	slice.RemoveIf(&this.Generations, func(_ int, gen *Generation) bool {
		return gen.NumJobs() == 0
	})

	// Rebuild the transaction ID to job sequence mapping after merging.
	for _, gen := range this.Generations {
		for _, seq := range gen.JobSeqs {
			for _, job := range seq.Jobs {
				gen.TxToSeqLookup[job.StdMsg.ID] = seq // Multiple jobs may map to the same job sequence.
			}
		}
	}

	txID := 0
	// Reassign IDs to generations, sequences, and jobs.
	for i, gen := range this.Generations {
		gen.ID = uint64(i)
		for j, seq := range gen.JobSeqs {
			seq.ID = uint64(j)
			for k, job := range seq.Jobs {
				job.ID = uint64(k)
				job.SeqID = seq.ID
				job.TxId = uint64(txID)
				txID++
			}
		}
	}

	this.BuildJobLookup() // Rebuild the message lookup.
	return this.InsertNonceAdjustment()
}

// Insert nonce offsets for each job in the execution plan.
// Rationale: When processing transactions from the same sender that span multiple job sequences, it is possible
// that some transcations may be processed in parallel in different job sequences. They all the see the same initial nonce,
// This will lead to nonce conflicts during execution or nonce too high errors.
// To resolve this, we need to insert nonce offsets for each job based on its position among all jobs
// from the same sender in the generation.
func (this *ExecutionPlan) InsertNonceAdjustment() error {
	var aggregatedErr error
	for _, gen := range this.Generations {
		// Group jobs by sender address in the generation.
		senders, seqsFromSameSender := gen.GroupBySenderAndSequence()
		for i, jobSeqs := range seqsFromSameSender {
			if len(jobSeqs) == 1 {
				continue
			}

			// Sort the job sequences by their IDs to ensure consistent ordering.
			sort.Slice(jobSeqs, func(i, j int) bool {
				return jobSeqs[i].Second[0].StdMsg.Native.Nonce < jobSeqs[j].Second[0].StdMsg.Native.Nonce
			})

			// Jobs from the same sender may span multiple job sequences.
			// We need to insert nonce offsets for each job sequencebased on its position
			// among all jobs from the same sender in the generation.
			offset := uint64(0)
			for j, pair := range jobSeqs {
				// Only offset nonces AFTER the first entry.
				// The first job nonce is already correct.
				if j == 0 {
					continue
				}

				// Build the nonce offset for the job.
				first := pair.Second[0]

				var err error
				offset += uint64(len(pair.Second))
				noncePreOffset, err := this.GenerateNonceAjustmentTransitions(
					first.StdMsg.ID,
					this.Store.CommittedStore(),
					senders[i],
					offset,
				)

				pair.First.PreTransitions = append(pair.First.PreTransitions, noncePreOffset...)
				aggregatedErr = errors.Join(aggregatedErr, err)
			}
		}
	}
	return aggregatedErr
}

func (*ExecutionPlan) GenerateNonceAjustmentTransitions(
	tx uint64,
	committedStore storageintf.ReadOnlyStore[string, crdtcommon.CRDT],
	callerAddr evmcommon.Address,
	offset uint64) ([]*statecell.StateCell, error) {
	noncePath := (&statecommon.PathBuilder{
		Sender: callerAddr,
	}).UnderSenderPath(statecommon.PATH_NONCE)

	// Calculate the nonce offset for the job sequence.
	// Then write it to the state cache.
	offsetDelta := commutative.NewUint64Delta(uint64(offset))

	// Initialize a temporary state store to write the nonce offset.
	execCache := statecache.NewExecutionStateStore(committedStore, 32, 1)
	_, err := execCache.Write(
		tx,
		noncePath,
		offsetDelta,
	)

	// Export the nonce offset state change.
	noncePreOffset := execCache.Export()
	return noncePreOffset, err
}

// BuildJobLookup constructs a mapping from transaction IDs to their corresponding Job structs
// within the execution schedule. This allows for efficient retrieval of Job information
func (this *ExecutionPlan) BuildJobLookup() {
	this.JobIDLookup = make(map[uint64]*Job)
	// Build the message lookup map for the schedule.
	for _, gen := range this.Generations {
		for _, seq := range gen.JobSeqs {
			for _, job := range seq.Jobs {
				this.JobIDLookup[job.StdMsg.ID] = job
			}
		}
	}
}

func (this *ExecutionPlan) GetNextGeneration() *Generation {
	this.currentGenIdx++
	return this.Generations[this.currentGenIdx-1]
}
