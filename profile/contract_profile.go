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

package profile

import (
	"bytes"
	"encoding/json"

	"github.com/arcology-network/common-lib/codec"
	"github.com/arcology-network/common-lib/exp/slice"
)

//  "github.com/arcology-network/scheduler"

// The callee struct stores the information of a contract function that is called by the EOA initiated transactions.
// It is mainly used to optimize the execution of the transactions. A callee is uniquely identified by a
// combination of the contract's address and the function signature.
type ContractInfo struct {
	ID      uint32   `json:"id"`      // ID of the contract profile, which is the index in the ContractInfo list
	Address [20]byte `json:"address"` // Contract address
	FuncIdx []uint32 `json:"FuncIdx"` // Indices of the functions in the function profile list
}

func (this *ContractInfo) Size() int {
	return 4 + 20 + 4 + len(this.FuncIdx)*4
}

// 10x faster and 2x smaller than json marshal/unmarshal
func (this *ContractInfo) Encode() []byte {
	buffer := make([]byte, this.Size())
	codec.Uint32(this.ID).EncodeTo(buffer)
	codec.Bytes20(this.Address).EncodeTo(buffer[4:])
	codec.Uint32s(this.FuncIdx).EncodeTo(buffer[24:])
	return buffer
}

func (this *ContractInfo) Decode(data []byte) *ContractInfo {
	fields, _ := codec.Byteset{}.Decode(data).(codec.Byteset)
	this.ID = uint32(codec.Uint32(0).Decode(fields[0]).(codec.Uint32))
	this.Address = [20]byte(codec.Bytes20{}.Decode(fields[1]).(codec.Bytes20))
	this.FuncIdx = new(codec.Uint32s).Decode(fields[2]).(codec.Uint32s)
	return this
}

func (this *ContractInfo) Equal(other *ContractInfo) bool {
	return this.ID == other.ID &&
		this.Address == other.Address &&
		slice.EqualSet(this.FuncIdx, other.FuncIdx)
}

// Marshal serializes Callee                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          to human-readable JSON.
// This is mainly for debugging and testing purposes.
func (this *ContractInfo) Marshal() ([]byte, error) {
	return json.MarshalIndent(this, "", "  ")
}

// Unmarshal parses JSON back into a Callee                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          struct.
func (this *ContractInfo) Unmarshal(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(this)
}

// func (this *ContractInfos) Unmarshal(data []byte) error {
