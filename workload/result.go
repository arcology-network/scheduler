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
	// "github.com/arcology-network/common-lib/codec"

	"fmt"

	"github.com/arcology-network/common-lib/crdt/statecell"
	commontype "github.com/arcology-network/common-lib/types"
	evmcore "github.com/ethereum/go-ethereum/core"
	ethcoretypes "github.com/ethereum/go-ethereum/core/types"
)

// The result of an execution. It includes the group ID, the transaction index, the transaction hash, the sender, the coinbase, the raw state accesses, the immune transitions, the receipt, the EVM result, the standard message, and the error.
type Result struct {
	GroupID         uint32 // == Group ID
	TxIndex         uint64
	TxHash          [32]byte
	RawStateRecords []*statecell.StateCell // Include both access records and transition records.
	Immuned         []*statecell.StateCell //These transitions will take effect anyway even if the execution fails.
	Receipt         *ethcoretypes.Receipt
	EvmResult       *evmcore.ExecutionResult
	StdMsg          *commontype.StandardMessage
	Err             error
}

// If the execution is unsuccessful, only keep the transitions that are immune to failures.
func (this *Result) GetRawStateRecords() []*statecell.StateCell {
	// When there is an execution error, only return the immune transitions.
	// Immune transitions include the gas fee and the nonce, which are independent of the execution status.
	if this.Err != nil {
		return this.Immuned
	}
	return this.RawStateRecords
}

func (this *Result) Print() {
	// fmt.Println("GroupID: ", this.GroupID)
	fmt.Println("TxIndex: ", this.TxIndex)
	fmt.Println("TxHash: ", this.TxHash)
	fmt.Println()
	statecell.StateCells(this.GetRawStateRecords()).Print()
	fmt.Println("Error: ", this.Err)
}

type Results []*Result

func (this Results) Print() {
	fmt.Println("Execution Results: ")
	for _, v := range this {
		v.Print()
		fmt.Println()
	}
}
