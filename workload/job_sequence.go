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

	crdtcommon "github.com/arcology-network/common-lib/crdt/common"
	"github.com/arcology-network/common-lib/crdt/commutative"
	statecell "github.com/arcology-network/common-lib/crdt/statecell"
	mapi "github.com/arcology-network/common-lib/exp/map"
	slice "github.com/arcology-network/common-lib/exp/slice"
	commontype "github.com/arcology-network/common-lib/types"
	schedulercommon "github.com/arcology-network/scheduler/common"
	evmcore "github.com/ethereum/go-ethereum/core"
	"github.com/holiman/uint256"
)

// JobSequence represents a sequence of jobs to be executed.
type JobSequence struct {
	ID   uint64 // Job sequence id
	Jobs []*Job // jobs in the sequence

	// Pre-execution state changes must be applied to the first job in the sequence before execution.
	// Notably, these state changes include nonce offsets.
	PreTransitions []*statecell.StateCell
}

func NewJobSequenceFromEthMessages(
	ID uint64,
	ethMsgIDs []uint64,
	evmMsgs []*evmcore.Message,
	txHash [][32]byte) *JobSequence {
	newJobSeq := &JobSequence{
		ID: ID, // Sequence ID
	}

	for i, evmMsg := range evmMsgs {
		newJobSeq.AddJob(&commontype.StandardMessage{
			ID:     ethMsgIDs[i],
			Native: evmMsg,
			TxHash: txHash[i],
		})
	}
	return newJobSeq
}

func NewJobSequenceFromStandardMessages(seqID uint64, stdMsgs ...*commontype.StandardMessage) *JobSequence {
	newJobSeq := &JobSequence{
		ID: seqID, // Sequence ID
	}

	for _, stdMsg := range stdMsgs {
		newJobSeq.AddJob(stdMsg)
	}
	return newJobSeq
}

func (this *JobSequence) MsgIDs() []uint64 {
	unique := make(map[uint64]bool)
	for _, job := range this.Jobs {
		unique[job.StdMsg.ID] = true
	}

	// Extract the unique IDs and sort them.
	ids := mapi.Keys(unique)
	sort.SliceStable(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (*JobSequence) FromStandardMessages(ID uint64, stdMsgs []*commontype.StandardMessage) *JobSequence {
	newJobSeq := &JobSequence{
		ID: ID, // Sequence ID
	}

	for _, stdMsg := range stdMsgs {
		newJobSeq.AddJob(stdMsg)
	}
	return newJobSeq
}

func (*JobSequence) T() *JobSequence { return &JobSequence{} }

func (this *JobSequence) AddJob(msg any) *JobSequence {
	this.Jobs = append(this.Jobs, &Job{
		ID:     uint64(len(this.Jobs)),
		SeqID:  this.ID,
		StdMsg: msg.(*commontype.StandardMessage),
		Result: &Result{},
	})
	return this
}

// GetID returns the ID of the JobSequence.
func (this *JobSequence) GetID() uint64 { return this.ID }

// Length returns the number of standard messages in the JobSequence.
func (this *JobSequence) Length() int { return len(this.Jobs) }

// GetClearRecords returns the cleared transitions of the JobSequence.
func (this *JobSequence) GetClearRecords() []*statecell.StateCell {
	// When there is only one job in the sequence, return its transitions directly.
	if len(this.Jobs) == 1 {
		return this.Jobs[0].Result.GetRawStateRecords()
	}

	trans := slice.Concate(this.Jobs,
		func(job *Job) []*statecell.StateCell {
			return job.Result.GetRawStateRecords()
		},
	)

	uniqueDict := make(map[string]*statecell.StateCell)
	for _, v := range trans {
		uniqueDict[*v.GetPath()] = v
	}

	uniqueTrans := mapi.Values(uniqueDict)
	return statecell.StateCells(uniqueTrans).SortByKey()
}

// MarkJobForRollback flags the transitions after the first conflicting transaction.
func (this *JobSequence) MarkJobForRollback(conflictTxLookup map[uint64]error) {
	// Get the first index of the first conflict transaction.
	// All the transitions after this index aren't usuable any more.
	first, _ := slice.FindFirstIf(this.Jobs, func(_ int, job *Job) bool {
		if job.Result.Err != nil {
			return false // The job failed for other reasons. We only care about conflicts here.
		}
		err, ok := (conflictTxLookup)[job.Result.TxIndex]
		job.Result.Err = err
		return ok
	})

	if first < 0 {
		return
	}

	// Mark all the jobs after the first conflicting transaction for rollback in the sequence.
	// Since they may have read the state written by the conflicting transaction.
	//
	// FIX ME: This is not optimal since not all the jobs after the conflicting job are
	// contaminated.
	for i := first + 1; i < len(this.Jobs); i++ {
		this.Jobs[i].Result.Err = schedulercommon.WARN_UPSTREAM_CONFLICT_IN_SEQUENCE
	}
}

// CalcualteRefund calculates the refund amount for the JobSequence.
func (this *JobSequence) CalcualteRefund() uint64 {
	amount := uint64(0)
	// for _, v := range *seqAPI.WriteCache().(*cache.WriteCache).Cache() {
	// 	typed := v.Value().(crdtcommon.CRDT)
	// 	amount += common.IfThen(
	// 		!v.Preexist(),
	// 		(uint64(typed.Size())/32)*uint64(v.Writes())*ethparams.SstoreSetGas,
	// 		(uint64(typed.Size())/32)*uint64(v.Writes()),
	// 	)
	// }
	return amount
}

// RefundTo refunds the specified amount from the payer to the recipient.
func (this *JobSequence) RefundTo(payer, recipent *statecell.StateCell, amount uint64) (uint64, error) {
	credit := commutative.NewU256Delta(uint256.NewInt(amount), true).(*commutative.U256)
	if _, _, _, _, err := recipent.Value().(crdtcommon.CRDT).Set(credit, nil); err != nil {
		return 0, err
	}
	recipent.IncrementDeltaWrites(1)

	debit := commutative.NewU256Delta(uint256.NewInt(amount), false).(*commutative.U256)
	if _, _, _, _, err := payer.Value().(crdtcommon.CRDT).Set(debit, nil); err != nil {
		return 0, err
	}
	payer.IncrementDeltaWrites(1)
	return amount, nil
}

func SortJobSequences(seqs []*JobSequence) {
	sort.Slice(seqs, func(i, j int) bool {
		return seqs[i].Jobs[0].StdMsg.ID <
			seqs[j].Jobs[0].StdMsg.ID
	})
}
