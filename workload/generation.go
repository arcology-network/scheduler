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
	common "github.com/arcology-network/common-lib/common"
	slice "github.com/arcology-network/common-lib/exp/slice"

	profile "github.com/arcology-network/scheduler/profiles"
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

// APIs under the concurrency namespace
type Generation struct {
	ID          uint64
	numThreads  uint32
	JobSeqs     []*JobSequence  // para jobSeqs
	Occurrences *map[uint64]int // occurrence dictionary for all jobs in this generation
}

func (*Generation) OccurrenceDict(jobSeqs []*JobSequence) *map[uint64]int {
	occurrences := map[uint64]int{}
	for _, seq := range jobSeqs {
		for _, job := range seq.Jobs {
			occurrences[profile.ToKey(job.StdMsg)]++ // Only count the first one if found
			break
		}
	}
	return &occurrences
}

func NewGeneration(id uint64, numThreads uint32, jobSeqs []*JobSequence) *Generation {
	gen := &Generation{
		ID:         id,
		numThreads: numThreads,
		JobSeqs:    jobSeqs,
	}
	gen.Occurrences = gen.OccurrenceDict(jobSeqs)
	return gen
}

func (this *Generation) Length() uint64 { return uint64(len(this.JobSeqs)) }

func (this *Generation) At(idx uint64) *JobSequence {
	return common.IfThenDo1st(idx < uint64(len(this.JobSeqs)), func() *JobSequence { return this.JobSeqs[idx] }, nil)
}

func (*Generation) New(id uint64, numThreads uint32, jobSeqs []*JobSequence) *Generation {
	return NewGeneration(id, numThreads, slice.To[*JobSequence, *JobSequence](jobSeqs))
}

func (this *Generation) Add(job *JobSequence) bool {
	this.JobSeqs = append(this.JobSeqs, job)
	return true
}

func (this *Generation) Clear() uint64 {
	length := len(this.JobSeqs)
	this.JobSeqs = this.JobSeqs[:0]
	return uint64(length)
}
