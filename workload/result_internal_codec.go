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
	buffer := make([]byte,
		8+ // GenerationID
			8+ // JobSequenceID
			8+ // JobID
			8+ // TxIndex
			32) // TxHash

	offset := codec.Uint64(this.GenerationID).EncodeTo(buffer)
	offset += codec.Uint64(this.JobSequenceID).EncodeTo(buffer[offset:])
	offset += codec.Uint64(this.JobID).EncodeTo(buffer[offset:])
	offset += codec.Uint64(this.TxIndex).EncodeTo(buffer[offset:])
	codec.Bytes32(this.TxHash).EncodeTo(buffer[offset:])

	msgViewEncoded, _ := this.MsgView.Encode()
	rawStates := statecell.StateCells(this.RawStateRecords).Encode()
	immunedStates := statecell.StateCells(this.Immuned).Encode()
	receiptEncoded, _ := commontype.EncodeReceipt(this.Receipt)
	evmResultEncoded, _ := commontype.EncodeExecutionResult(this.EvmResult)

	return codec.Byteset([][]byte{
		buffer, // fixed size items
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
	this.TxIndex = uint64(codec.Uint64(0).Decode(fixed[24:]).(codec.Uint64))
	this.TxHash = codec.Bytes32{}.Decode(fixed[32:]).(codec.Bytes32)

	this.MsgView = (&commontype.MessageView{}).Decode(fields[1]).(*commontype.MessageView)
	this.RawStateRecords = []*statecell.StateCell(statecell.StateCells{}.Decode(fields[2]).(statecell.StateCells))
	this.Immuned = []*statecell.StateCell(statecell.StateCells{}.Decode(fields[3]).(statecell.StateCells))
	this.Receipt, _ = commontype.DecodeReceipt(fields[4])
	this.EvmResult, _ = commontype.DecodeExecutionResult(fields[5])
	return this, nil
}
