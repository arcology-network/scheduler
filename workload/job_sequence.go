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
	mapi "github.com/arcology-network/common-lib/exp/map"
	slice "github.com/arcology-network/common-lib/exp/slice"
	commontype "github.com/arcology-network/common-lib/types"
	statecommon "github.com/arcology-network/state-engine/common"
	"github.com/arcology-network/state-engine/type/commutative"
	statecell "github.com/arcology-network/state-engine/type/statecell"
	evmcore "github.com/ethereum/go-ethereum/core"
	"github.com/holiman/uint256"
)

// JobSequence represents a sequence of jobs to be executed.
type JobSequence struct {
	ID   uint64 // group id
	Jobs []*Job // jobs in the sequence
}

func (*JobSequence) FromEthMessages(ID uint64, jobIDs []uint64, evmMsgs []*evmcore.Message, txHash [][32]byte) *JobSequence {
	newJobSeq := &JobSequence{
		ID: ID, // Sequence ID
	}

	for i, evmMsg := range evmMsgs {
		newJobSeq.addJob(&commontype.StandardMessage{
			ID:     jobIDs[i],
			Native: evmMsg,
			TxHash: txHash[i],
		})
	}
	return newJobSeq
}

func (*JobSequence) FromStandardMessage(ID uint64, stdMsg *commontype.StandardMessage) *JobSequence {
	newJobSeq := &JobSequence{
		ID: ID, // Sequence ID
	}

	newJobSeq.addJob(stdMsg)
	return newJobSeq
}

func (*JobSequence) FromStandardMessages(ID uint64, stdMsgs []*commontype.StandardMessage) *JobSequence {
	newJobSeq := &JobSequence{
		ID: ID, // Sequence ID
	}

	for _, stdMsg := range stdMsgs {
		newJobSeq.addJob(stdMsg)
	}
	return newJobSeq
}

func (*JobSequence) T() *JobSequence { return &JobSequence{} }

func (this *JobSequence) addJob(msg any) *JobSequence {
	this.Jobs = append(this.Jobs, &Job{
		ID:     msg.(*commontype.StandardMessage).ID,
		StdMsg: msg.(*commontype.StandardMessage),
		Result: &Result{},
	})
	return this
}

// GetID returns the ID of the JobSequence.
func (this *JobSequence) GetID() uint64 { return this.ID }

// Length returns the number of standard messages in the JobSequence.
func (this *JobSequence) Length() int { return len(this.Jobs) }

// GetClearedTransition returns the cleared transitions of the JobSequence.
func (this *JobSequence) GetClearedTransition() []*statecell.StateCell {
	trans := slice.Concate(this.Jobs,
		func(job *Job) []*statecell.StateCell {
			return job.Result.Transitions()
		},
	)

	uniqueDict := make(map[string]*statecell.StateCell)
	for _, v := range trans {
		uniqueDict[*v.GetPath()] = v
	}

	uniqueTrans := mapi.Values(uniqueDict)
	return statecell.StateCells(uniqueTrans).SortByKey()
}

// FlagConflict flags the transitions after the first conflicting transaction.
func (this *JobSequence) FlagConflict(dict map[uint64]uint64, err error) {
	// Get the first index of the first conflict transaction.
	// All the transitions after this index aren't usuable any more.
	first, _ := slice.FindFirstIf(this.Jobs, func(_ int, job *Job) bool {
		_, ok := (dict)[job.Result.TxIndex]
		return ok
	})

	// The results of the transactions after the first conflict transaction are flagged as conflicting as well.
	// Because they are potentially affected by the conflict by using the conflicting state.
	for i := first; i < len(this.Jobs); i++ {
		this.Jobs[i].Result.Err = err
	}
}

// CalcualteRefund calculates the refund amount for the JobSequence.
func (this *JobSequence) CalcualteRefund() uint64 {
	amount := uint64(0)
	// for _, v := range *seqAPI.WriteCache().(*cache.WriteCache).Cache() {
	// 	typed := v.Value().(statecommon.Type)
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
	if _, _, _, _, err := recipent.Value().(statecommon.Type).Set(credit, nil); err != nil {
		return 0, err
	}
	recipent.IncrementDeltaWrites(1)

	debit := commutative.NewU256Delta(uint256.NewInt(amount), false).(*commutative.U256)
	if _, _, _, _, err := payer.Value().(statecommon.Type).Set(debit, nil); err != nil {
		return 0, err
	}
	payer.IncrementDeltaWrites(1)
	return amount, nil
}
