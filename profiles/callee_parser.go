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
	"github.com/arcology-network/storage-committer/type/statecell"
)

type CalleeParser struct {
	UID          uint64   `json:"uid,omitempty"` // Unique ID for the callee 4 bytes from the contract address + func signature [4]byte
	Idx          uint32   `json:"id"`            // Index in the CalleeProfile list, used by the contract profile to reference the entry.
	ContractID   uint32   `json:"contractId"`    // Idx of the contract this function belongs to
	Sequential   bool     `json:"sequential"`    // A sequential / parallel only calls
	TotalCalls   uint32   `json:"totalCalls"`    // Total number of calls
	MaxGas       uint64   `json:"maxGas"`        // Max gas used
	Deferrable   bool     `json:"deferrable"`    // If one of the calls to this function should be deferred to the second generation.
	Prepayment   uint64   `json:"prepayment"`    // Required prepayment amount for the deferrable functions
	ConflictWith []uint32 `json:"conflictWith"`  // ConflictWith of the conflicting callee indices.
}

func (this *CalleeParser) FromPath(tran *statecell.StateCell) {
	// path := *tran.GetPath()

	// stgcommon.DeriveKey

}

// func (this *CalleeParser) ToUnivalue() *statecell.StateCell {
// 	return codec.Byteset([][]byte{
// 		codec.Uint64(this.UID).Encode(),
// 		codec.Uint32(this.Idx).Encode(),
// 		codec.Uint32(this.ContractID).Encode(),
// 		codec.Bool(this.Sequential).Encode(),
// 		codec.Uint32(this.TotalCalls).Encode(),
// 		codec.Uint64(this.MaxGas).Encode(),
// 		codec.Bool(this.Deferrable).Encode(),
// 		codec.Uint64(this.Prepayment).Encode(),
// 		codec.Uint32s(this.ConflictWith).Encode(),
// 	}).Encode(), nil
// }
