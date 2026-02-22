/*
 *   Copyright (c) 2026 Arcology Network

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
	"github.com/arcology-network/common-lib/codec"
	statecell "github.com/arcology-network/common-lib/crdt/statecell"
	commontype "github.com/arcology-network/common-lib/types"
)

// "github.com/arcology-network/common-lib/codec"

// func (this *Result) HeaderSize() uint64 {
// 	// Skip the fixed size fields: GenerationID, JobSequenceID, JobID...
// 	return 6 * codec.UINT64_LEN
// }

func (this *Result) Encode() []byte {
	fixedBuffer := make([]byte,
		8+ // GenerationID
			8+ // JobSequenceID
			8) // JobID

	offset := codec.Uint64(this.GenerationID).EncodeTo(fixedBuffer)
	offset += codec.Uint64(this.JobSequenceID).EncodeTo(fixedBuffer[offset:])
	offset += codec.Uint64(this.JobID).EncodeTo(fixedBuffer[offset:])
	// codec.Uint64(this.TxIndex).EncodeTo(fixedBuffer[offset:])

	msgViewEncoded, _ := this.TxInfo.Encode()
	rawStates := statecell.StateCells(this.RawStateRecords).Encode()
	immunedStates := statecell.StateCells(this.Immuned).Encode()
	receiptEncoded, _ := commontype.EncodeReceipt(this.Receipt)
	evmResultEncoded, _ := commontype.EncodeExecutionResult(this.EvmResult)

	return codec.Byteset([][]byte{
		fixedBuffer, // fixed size items
		msgViewEncoded,
		rawStates,
		immunedStates,
		receiptEncoded,
		evmResultEncoded,
	}).Encode()
}

func (this *Result) Decode(buffer []byte) (any, error) {
	fields := codec.Byteset{}.Decode(buffer).(codec.Byteset)
	fixed := fields[0]
	this.GenerationID = uint64(codec.Uint64(0).Decode(fixed).(codec.Uint64))
	this.JobSequenceID = uint64(codec.Uint64(0).Decode(fixed[8:]).(codec.Uint64))
	this.JobID = uint64(codec.Uint64(0).Decode(fixed[16:]).(codec.Uint64))


	this.TxInfo = (&commontype.TransactionView{}).Decode(fields[1]).(*commontype.TransactionView)
	this.RawStateRecords = []*statecell.StateCell(statecell.StateCells{}.Decode(fields[2]).(statecell.StateCells))
	this.Immuned = []*statecell.StateCell(statecell.StateCells{}.Decode(fields[3]).(statecell.StateCells))
	this.Receipt, _ = commontype.DecodeReceipt(fields[4])
	this.EvmResult, _ = commontype.DecodeExecutionResult(fields[5])
	return this, nil
}
