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

// scheduletype is a package that contains the constants and functions for the scheduler
// that shareed by other packages.
package scheduler

import (
	"github.com/arcology-network/common-lib/codec"
	commontype "github.com/arcology-network/common-lib/types"

	statecommon "github.com/arcology-network/state-engine/common"
)

// func CallToKey(addr []byte, selector []byte) string {
// 	return string(addr[:statecommon.SELECTOR_LENGTH]) + string(selector[:statecommon.SELECTOR_LENGTH])
// }

// The function creates a unique func Key representation of the callee information
func DeriveKey(addr []byte, selector []byte) []byte {
	return codec.Uint64(DeriveUID(addr, selector)).Encode()
}

func DeriveUID(addr []byte, selector []byte) uint64 {
	address := make([]byte, statecommon.SHORT_CONTRACT_ADDRESS_LENGTH)
	copy(address, addr)

	selectorBytes := make([]byte, statecommon.SELECTOR_LENGTH)
	copy(selectorBytes, selector)

	return uint64(codec.Uint64(0).FromBytes(append(address, selectorBytes...)))
}

// Get the callee key from a message
func ToKey(msg *commontype.StandardMessage) uint64 {
	if (*msg.Native).To == nil {
		return 0
	}

	if len(msg.Native.Data) == 0 {
		return 0
	}

	to := make([]byte, statecommon.SHORT_CONTRACT_ADDRESS_LENGTH)
	if msg.Native.To != nil {
		copy(to, (*msg.Native.To)[:])
	}

	data := make([]byte, statecommon.SELECTOR_LENGTH)
	if msg.Native.Data != nil {
		copy(data, msg.Native.Data)
	}

	return DeriveUID(to, data)
}
