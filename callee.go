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

package scheduler

import (
	"encoding/hex"
	"slices"
	"strings"

	"github.com/arcology-network/common-lib/codec"
	"github.com/arcology-network/common-lib/exp/deltaset"
	"github.com/arcology-network/common-lib/exp/slice"
	commontype "github.com/arcology-network/common-lib/types"

	//  "github.com/arcology-network/scheduler"
	stgcommon "github.com/arcology-network/storage-committer/common"
	commutative "github.com/arcology-network/storage-committer/type/commutative"
	univalue "github.com/arcology-network/storage-committer/type/univalue"
)

// The callee struct stores the information of a contract function that is called by the EOA initiated transactions.
// It is mainly used to optimize the execution of the transactions. A callee is uniquely identified by a
// combination of the contract's address and the function signature.
type Callee struct {
	Index       uint32     // Index of the Callee in the Callee list
	AddrAndSign [12]byte   // Short address of the callee, first 8 bytes from the function + signature [4]byte
	Indices     []uint32   // Indices of the conflicting callee indices.
	Sequential  bool       // A sequential / parallel only calls
	Except      [][12]byte // Sequntial or Paralle call exception the following ones.

	Calls      uint32 // Total number of calls
	AvgGas     uint32 // Average gas used
	Deferrable bool   // If one of the calls should be deferred to the second generation.
}

// If the conflict entry is recorded already, return true.
func (this *Callee) IsInConflictList(idx uint32) bool {
	return slices.IndexFunc(this.Indices, func(i uint32) bool { return i == idx }) != -1
}

func NewCallee(idx uint32, addr []byte, funSign []byte) *Callee {
	return &Callee{
		Index:       idx,
		AddrAndSign: codec.Bytes12{}.FromBytes(Compact(addr, funSign)),
		Except:      [][12]byte{},
		Deferrable:  false,
	}
}

func (*Callee) IsPropertyPath(path string) bool {
	return len(path) > stgcommon.ETH10_ACCOUNT_FULL_LENGTH &&
		strings.Contains(path[stgcommon.ETH10_ACCOUNT_FULL_LENGTH:], stgcommon.FULL_FUNC_PATH)
}

// Extract the callee signature from the path string
func (this *Callee) parseCalleeSignature(path string) (string, []byte, []byte) {
	idx := strings.Index(path, stgcommon.FULL_FUNC_PATH)
	if idx < 0 {
		return "", []byte{}, []byte{}
	}

	fullPath := path[idx+len(stgcommon.FULL_FUNC_PATH):]
	sign, _ := hex.DecodeString(fullPath)

	if len(sign) == 0 {
		return "", []byte{}, []byte{}
	}
	addrStr := path[stgcommon.ETH10_ACCOUNT_PREFIX_LENGTH:]
	idx = strings.Index(addrStr, "/")
	addrStr = strings.TrimPrefix(addrStr[:idx], "0x")

	addr, _ := hex.DecodeString(addrStr)
	return string(append(addr[:stgcommon.SHORT_CONTRACT_ADDRESS_LENGTH], sign...)),
		addr, sign
}

// Initialize from univalues
func (this *Callee) init(trans ...*univalue.Univalue) {
	for _, v := range trans {
		if this == nil {
			return
		}

		// Set execution method
		if strings.HasSuffix(*v.GetPath(), stgcommon.EXECUTION_PARALLELISM) && v.Value() != nil {
			flag, _, _ := v.Value().(stgcommon.Type).Get()
			this.Sequential = flag.([]byte)[0] == stgcommon.SEQUENTIAL_EXECUTION
		}

		// Set the excepted transitions
		if strings.HasSuffix(*v.GetPath(), stgcommon.EXECUTION_EXCEPTED) {
			subPaths, _, _ := v.Value().(*commutative.Path).Get()
			subPathSet := subPaths.(*deltaset.DeltaSet[string]) // Get all the conflicting ones.
			for _, subPath := range subPathSet.Elements() {
				k := codec.Bytes12{}.FromBytes([]byte(subPath))
				this.Except = append(this.Except, k)
			}
		}

		// Set the Deferrable value
		if strings.HasSuffix(*v.GetPath(), stgcommon.REQUIRED_GAS_PREPAYMENT) && v.Value() != nil {
			flag, _, _ := v.Value().(stgcommon.Type).Get()
			this.Deferrable = flag.([]byte)[0] > 0
		}
	}

}

// 10x faster and 2x smaller than json marshal/unmarshal
func (this *Callee) Encode() ([]byte, error) {
	return codec.Byteset([][]byte{
		codec.Uint32(this.Index).Encode(),
		this.AddrAndSign[:],
		codec.Uint32s(this.Indices).Encode(),
		codec.Bool(this.Sequential).Encode(),

		codec.Bytes12s(this.Except).Encode(),

		codec.Uint32(this.Calls).Encode(),
		codec.Uint32(this.AvgGas).Encode(),
		codec.Bool(this.Deferrable).Encode(),
	}).Encode(), nil
}

func (this *Callee) Decode(data []byte) *Callee {
	fields, _ := codec.Byteset{}.Decode(data).(codec.Byteset)
	this.Index = uint32(new(codec.Uint32).Decode(fields[0]).(codec.Uint32))
	this.AddrAndSign = new(codec.Bytes12).FromBytes(slice.Clone(fields[1])[:])
	this.Indices = new(codec.Uint32s).Decode(fields[2]).(codec.Uint32s)
	this.Sequential = bool(new(codec.Bool).Decode(fields[3]).(codec.Bool))
	this.Except = new(codec.Bytes12s).Decode(fields[4]).(codec.Bytes12s)
	this.Calls = uint32(new(codec.Uint32).Decode(fields[5]).(codec.Uint32))
	this.AvgGas = uint32(new(codec.Uint32).Decode(fields[6]).(codec.Uint32))
	this.Deferrable = bool(new(codec.Bool).Decode(fields[7]).(codec.Bool))
	return this
}

func (this *Callee) Equal(other *Callee) bool {
	return this.Index == other.Index &&
		slice.EqualSet(this.AddrAndSign[:], other.AddrAndSign[:]) &&
		slice.EqualSet(this.Indices, other.Indices) &&
		this.Sequential == other.Sequential &&
		slice.EqualSet(this.Except, other.Except) &&
		this.Calls == other.Calls &&
		this.AvgGas == other.AvgGas &&
		this.Deferrable == other.Deferrable
}

type Callees []*Callee

func (this Callees) Encode() []byte {
	buffer := slice.Transform(this, func(i int, callee *Callee) []byte {
		bytes, _ := (callee).Encode()
		return bytes
	})
	return codec.Byteset(buffer).Encode()
}

func (Callees) Decode(buffer []byte) interface{} {
	buffers := new(codec.Byteset).Decode(buffer).(codec.Byteset)
	callees := make(Callees, len(buffers))
	for i, buf := range buffers {
		callees[i] = new(Callee).Decode(buf)
	}
	return Callees(callees)
}

func (Callees) From(addr []byte, funSigns ...[]byte) [][stgcommon.CALLEE_ID_LENGTH]byte {
	callees := make([][stgcommon.CALLEE_ID_LENGTH]byte, len(funSigns))
	for i, funSign := range funSigns {
		callees[i] = new(codec.Bytes12).FromBytes(
			Compact(addr, funSign),
		)
	}
	return callees
}

// Get the callee key from a message
func ToKey(msg *commontype.StandardMessage) string {
	if (*msg.Native).To == nil {
		return ""
	}

	if len(msg.Native.Data) == 0 {
		return string((*msg.Native.To)[:stgcommon.FUNCTION_SIGNATURE_LENGTH])
	}
	return CallToKey((*msg.Native.To)[:], msg.Native.Data[:stgcommon.FUNCTION_SIGNATURE_LENGTH])
}

// func CallToKey(addr []byte, funSign []byte) string {
// 	return string(addr[:.FUNCTION_SIGNATURE_LENGTH]) + string(funSign[:.FUNCTION_SIGNATURE_LENGTH])
// }
