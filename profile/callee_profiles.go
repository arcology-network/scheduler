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
	"github.com/arcology-network/common-lib/codec"
	"github.com/arcology-network/common-lib/exp/slice"
)

// CalleeProfiles                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 is a slice of Callee                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          pointers.
type CalleeProfiles []*Callee

func (CalleeProfiles) From(addr []byte, selectors ...[]byte) [][]byte {
	callees := make([][]byte, len(selectors))
	for i, selector := range selectors {
		callees[i] = DeriveKey(addr, selector)
	}
	return callees
}

func (this CalleeProfiles) Encode() []byte {
	buffer := slice.Transform(this, func(i int, callee *Callee) []byte {
		bytes, _ := (callee).Encode()
		return bytes
	})
	return codec.Byteset(buffer).Encode()
}

func (CalleeProfiles) Decode(buffer []byte) any {
	buffers := new(codec.Byteset).Decode(buffer).(codec.Byteset)
	callees := make(CalleeProfiles, len(buffers))
	for i, buf := range buffers {
		callees[i] = new(Callee).Decode(buf)
	}
	return CalleeProfiles(callees)
}
