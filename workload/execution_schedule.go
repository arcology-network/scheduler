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
	"runtime"

	libcommon "github.com/arcology-network/common-lib/common"
	"github.com/arcology-network/common-lib/exp/slice"
	libtypes "github.com/arcology-network/common-lib/types"
)

type ExecutionSchedule struct {
	Transfers    []*libtypes.StandardMessage // Transfers
	Deployments  []*libtypes.StandardMessage // Contract deployments
	Unknowns     []*libtypes.StandardMessage // Messages with unknown conflicts with others
	WithConflict []*libtypes.StandardMessage // Messages with some known conflicts
	Sequentials  []*libtypes.StandardMessage // Callees that are marked as sequential only

	Generations []*Generation
	RawMsgSet   [][][]*libtypes.StandardMessage
	MsgLookup   map[uint64]*libtypes.StandardMessage // Message lookup by sequence ID
	// CallCounts  []map[string]int
}

// The function returns an optimized execution schedule represented as a 3-dimensional slice.
//
// schedule[g][p][i]
//
// g = generation index://
//	Generations run sequentially. Generation g+1 begins only after generation g completes.
//
// p = parallel group index within a generation://
//	Each parallel group can be executed concurrently with the other groups in the same generation.
//
// i = transaction index within a parallel group://
//	Transactions inside a group must execute in strict sequential order.

func (this *ExecutionSchedule) Finalize() []*Generation {
	//  Transfers + deployments can be executed in parallel with withConflict + sequentials.
	_1 := slice.ConcateNonEmpty(func(v []*libtypes.StandardMessage) bool { return len(v) > 0 }, this.Transfers, this.Deployments, this.Unknowns)
	_1Gen := slice.Transform(_1, func(i int, msg *libtypes.StandardMessage) []*libtypes.StandardMessage {
		return []*libtypes.StandardMessage{msg}
	})

	if len(_1Gen) > 0 {
		if len(this.RawMsgSet) == 0 {
			this.RawMsgSet = append(this.RawMsgSet, _1Gen)
		} else {
			// Merge with the first generation, since they are all parallel.
			this.RawMsgSet[0] = append(this.RawMsgSet[0], _1Gen...)
		}
	}

	_2 := slice.ConcateNonEmpty(func(v []*libtypes.StandardMessage) bool { return len(v) > 0 }, this.WithConflict, this.Sequentials)
	_2Gen := slice.Transform(_2, func(i int, msg *libtypes.StandardMessage) []*libtypes.StandardMessage {
		return []*libtypes.StandardMessage{msg}
	})

	if len(_2Gen) > 0 {
		if len(this.RawMsgSet) == 0 {
			this.RawMsgSet = append(this.RawMsgSet, _2Gen)
		} else {
			// Merge with the first generation, since they are all parallel.
			this.RawMsgSet[0] = append(this.RawMsgSet[0], _2Gen...)
		}
	}

	slice.RemoveIf(&this.RawMsgSet, func(i int, seq [][]*libtypes.StandardMessage) bool {
		return len(seq) == 0
	})

	// Execute the conversion in parallel.
	libcommon.ParallelExecute(
		func() {
			// Convert to Generation structs.
			numCores := uint32(runtime.NumCPU())
			this.Generations = make([]*Generation, 0, len(this.RawMsgSet))
			for i, msgs := range this.RawMsgSet {
				seqs := make([]*JobSequence, len(msgs))
				for j, msg := range msgs {
					seqs[j] = new(JobSequence).FromStandardMessages(uint64(j), msg)
				}
				this.Generations = append(this.Generations, NewGeneration(uint64(i), numCores, seqs))
			}
		},
		func() {
			// Build the message lookup map for the schedule.
			for _, gen := range this.Generations {
				for _, seq := range gen.JobSeqs {
					for _, job := range seq.Jobs {
						this.MsgLookup[job.StdMsg.ID] = job.StdMsg
					}
				}
			}
		},
	)
	return this.Generations
}
