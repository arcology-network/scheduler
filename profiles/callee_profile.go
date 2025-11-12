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

package scheduler

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"

	//  "github.com/arcology-network/scheduler"
	"github.com/arcology-network/common-lib/codec"
	"github.com/arcology-network/common-lib/exp/slice"
	stgcommon "github.com/arcology-network/storage-committer/common"
	statecell "github.com/arcology-network/storage-committer/type/statecell"
)

// The callee struct stores the information of a contract function that is called by the EOA initiated transactions.
// It is mainly used to optimize the execution of the transactions. A callee is uniquely identified by a
// combination of the contract's address and the function signature.
type CalleeProfile struct {
	UID          uint64   `json:"uid,omitempty"` // Unique ID for the callee 4 bytes from the contract address + func signature [4]byte
	Contract     uint64   `json:"contractId"`    // First 8 bytes of the real contract address
	Sequential   bool     `json:"sequential"`    // A sequential / parallel only calls
	TotalCalls   uint32   `json:"totalCalls"`    // Total number of calls
	MaxGas       uint64   `json:"maxGas"`        // Max gas used
	Deferrable   bool     `json:"deferrable"`    // If one of the calls to this function should be deferred to the second generation.
	Prepayment   uint64   `json:"prepayment"`    // Required prepayment amount for the deferrable functions
	ConflictWith []uint64 `json:"conflictWith"`  // ConflictWith of the conflicting callee indices.
}

func (this *CalleeProfile) SortConflicts() { slices.Sort(this.ConflictWith) } // Sort the callees by the indices in ascending order.

// If the conflict entry is recorded already, return true.
func (this *CalleeProfile) IsInConflictList(idx uint64) bool {
	return slices.IndexFunc(this.ConflictWith, func(i uint64) bool { return i == uint64(idx) }) != -1
}

func NewCalleeProfile(addr []byte, selector []byte) *CalleeProfile {
	return &CalleeProfile{
		UID: DeriveUID(addr, selector),
	}
}

func (*CalleeProfile) IsPropertyPath(path string) bool {
	return len(path) > stgcommon.ETH10_ACCOUNT_FULL_LENGTH &&
		strings.Contains(path[stgcommon.ETH10_ACCOUNT_FULL_LENGTH:], stgcommon.FUNC_PROFILE_PATH)
}

// Extract the callee signature from the path string
func (this *CalleeProfile) ParseKeyFromPath(path string) (string, []byte, []byte) {
	idx := strings.Index(path, stgcommon.FUNC_PROFILE_PATH)
	if idx < 0 {
		return "", []byte{}, []byte{}
	}

	fullPath := path[idx+len(stgcommon.FUNC_PROFILE_PATH):]
	selector, _ := hex.DecodeString(fullPath)

	if len(selector) == 0 {
		return "", []byte{}, []byte{}
	}
	addrStr := path[stgcommon.ETH10_ACCOUNT_PREFIX_LENGTH:]
	idx = strings.Index(addrStr, "/")
	addrStr = strings.TrimPrefix(addrStr[:idx], "0x")

	addr, _ := hex.DecodeString(addrStr)
	return string(append(addr[:stgcommon.SHORT_CONTRACT_ADDRESS_LENGTH], selector...)),
		addr, selector
}

// Initialize from univalues
func (this *CalleeProfile) Init(trans ...*statecell.StateCell) {
	for _, v := range trans {
		if this == nil {
			return
		}

		// Set execution method
		if strings.HasSuffix(*v.GetPath(), stgcommon.PARALLELISM_LEVEL) && v.Value() != nil {
			flag, _, _ := v.Value().(stgcommon.Type).Get()
			this.Sequential = flag.([]byte)[0] == stgcommon.SEQUENTIAL_EXECUTION
		}

		// Set the excepted transitions
		// if strings.HasSuffix(*v.GetPath(), stgcommon.PARALLELISM_LEVEL) {
		// 	subPaths, _, _ := v.Value().(*commutative.Path).Get()
		// 	subPathSet := subPaths.(*deltaset.DeltaSet[string]) // Get all the conflicting ones.
		// 	for _, subPath := range subPathSet.Elements() {
		// 		k := codec.Bytes12{}.FromBytes([]byte(subPath))
		// 		this.Except = append(this.Except, k)
		// 	}
		// }

		// Set the Deferrable value
		if strings.HasSuffix(*v.GetPath(), stgcommon.REQUIRED_PREPAYMENT_AMOUNT) && v.Value() != nil {
			flag, _, _ := v.Value().(stgcommon.Type).Get()
			this.Deferrable = flag.(uint64) > 0
		}
	}
}

// Equal checks if two CalleeProfile are equal
func (this *CalleeProfile) Equal(other *CalleeProfile) bool {
	return this.UID == other.UID &&
		this.Contract == other.Contract &&
		this.Sequential == other.Sequential &&
		this.TotalCalls == other.TotalCalls &&
		this.MaxGas == other.MaxGas &&
		this.Deferrable == other.Deferrable &&
		this.Prepayment == other.Prepayment &&
		slice.EqualSet(this.ConflictWith, other.ConflictWith)
}

//---- Encode serializes CalleeProfile to a byte array -------------------------------------

// 10x faster and 2x smaller than json marshal/unmarshal
func (this *CalleeProfile) Encode() ([]byte, error) {
	return codec.Byteset([][]byte{
		codec.Uint64(this.UID).Encode(),
		codec.Uint64(this.Contract).Encode(),
		codec.Bool(this.Sequential).Encode(),
		codec.Uint32(this.TotalCalls).Encode(),
		codec.Uint64(this.MaxGas).Encode(),
		codec.Bool(this.Deferrable).Encode(),
		codec.Uint64(this.Prepayment).Encode(),
		codec.Uint64s(this.ConflictWith).Encode(),
	}).Encode(), nil
}

func (this *CalleeProfile) Decode(data []byte) *CalleeProfile {
	fields, _ := codec.Byteset{}.Decode(data).(codec.Byteset)

	this.UID = uint64(codec.Uint64(0).FromBytes(slice.Clone(fields[0])[:]))
	this.Contract = uint64(codec.Uint64(0).Decode(fields[1]).(codec.Uint64))
	this.Sequential = bool(new(codec.Bool).Decode(fields[2]).(codec.Bool))
	this.TotalCalls = uint32(codec.Uint32(0).Decode(fields[3]).(codec.Uint32))
	this.MaxGas = uint64(codec.Uint64(0).Decode(fields[4]).(codec.Uint64))
	this.Deferrable = bool(new(codec.Bool).Decode(fields[5]).(codec.Bool))
	this.Prepayment = uint64(codec.Uint64(0).Decode(fields[6]).(codec.Uint64))
	this.ConflictWith = new(codec.Uint64s).Decode(fields[7]).(codec.Uint64s)
	return this
}

// Marshal serializes CalleeProfile                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          to human-readable JSON.
// This is mainly for debugging and testing purposes.
func (this *CalleeProfile) Marshal() ([]byte, error) {
	return json.MarshalIndent(this, "", "  ")
}

// Unmarshal parses JSON back into a CalleeProfile                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          struct.
func (this *CalleeProfile) Unmarshal(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(this)
}
