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
	commontype "github.com/arcology-network/common-lib/types"
)

type Job struct {
	ID           uint64 // Job id
	StdMsg       *commontype.StandardMessage
	Result       *Result
	Err          error   // Execution error directly from the EVM, not from the receipt.
	InitialGas   *uint64 // Initial gas amount for the contract, used to determine if the contract has enough gas to execute
	GasRemaining *uint64 // Remaining gas for the contract, used to determine if the contract has enough gas to execute
	PrepaidGas   uint64  // Gas paid for the deferred execution, negative is paying for the others, positive is paied by others.
}

func NewJob(stdMsg *commontype.StandardMessage, id uint64) *Job {
	return &Job{
		StdMsg: stdMsg,
		ID:     id,
	}
}

func (this *Job) Successful() bool {
	if this.Result != nil {
		return this.Result.Receipt != nil &&
			this.Result.Receipt.Status == 1 &&
			this.Result.Err == nil
	}
	return this.Err == nil
}
