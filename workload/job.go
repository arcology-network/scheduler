/*
 *   Copyright (c) 2025 Arcology Network

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
	"fmt"

	mapi "github.com/arcology-network/common-lib/exp/map"
	commontype "github.com/arcology-network/common-lib/types"
	callee "github.com/arcology-network/scheduler/callee"
)

type Job struct {
	TxId   uint64 // The global transaction id in the block, not the job id.
	ID     uint64 // Job serial id in the sequence
	SeqID  uint64 // Job sequence id
	StdMsg *commontype.StandardMessage

	InitialGas   *uint64 // Initial gas amount for the contract, used to determine if the contract has enough gas to execute
	GasRemaining *uint64 // Remaining gas for the contract, used to determine if the contract has enough gas to execute
	// PrepaidGas   uint64  // Gas paid for the deferred execution, negative is paying for the others, positive is paied by others.
	IsDeferred bool // If the job can be scheduled for deferred execution.

	// The field is assigned after the job is executed.
	// It has no use before execution.
	Result *Result

	// Callee's execution profile, assigned when the job is created.
	// Need to strip this field when serializing the job, since it has no meaning outside the scheduler.
	Profile *callee.Profile

	// The surcharge rate for this job, based on execution complexity.
	// A Tx requiring the scheduler to plan a deferred execution will
	// have a higher surcharge rate.
	Surcharge int64

	CollidesWith map[uint64]bool // A set of job IDs that this job collides with, for quick lookup.
}

func (this *Job) NumConflicts() int {
	if this.Profile == nil {
		return 0
	}
	return len(this.Profile.ConflictPeers)
}

func (this *Job) IsSequentialOnly() bool {
	return this.Profile != nil && this.Profile.IsSequentialOnly()
}

func (this *Job) GenerateConflictLookup() {
	if this.Profile != nil {
		this.CollidesWith = make(map[uint64]bool)
	}

	this.CollidesWith = mapi.FromSlice(this.Profile.ConflictPeers, func(k uint64) bool {
		return true
	})
}

// Transfer and deployment transactions are always conflict-free.
// So they can be added to the parallel transaction set directly.
func (this *Job) IsFullyParallelizable() bool {
	return len(this.StdMsg.Native.Data) == 0 || this.StdMsg.Native.To == nil
}

// Possible to be parallelized if not marked as sequential only and has no conflicts.
// No guarantee it won't conflict with others at runtime.
func (this *Job) IsPotentiallyParallelizable() bool {
	// THe conflict peers will be cleared if the profile is determined to be sequential only.
	// In that case, it conflicts with everyone else, so no need to keep conflict peers inforamtion anymore.
	return !this.Profile.IsSequentialOnly() && this.NumConflicts() == 0
}

func (this *Job) String() string {
	return fmt.Sprintf("Job ID: %d, SeqID: %d, TxHash: %x, SequentialOnly: %t\n",
		this.ID, this.SeqID, this.StdMsg.TxHash, this.IsSequentialOnly())
}

type Jobs []*Job

func (this Jobs) HasConflictWith(job *Job) bool {
	for j := range this {
		if _, ok := (this)[j].CollidesWith[job.Profile.ID.UID]; ok {
			return true
		}
	}
	return false
}
