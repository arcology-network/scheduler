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
	libtypes "github.com/arcology-network/common-lib/types"
	statecommon "github.com/arcology-network/state-engine/common"
	ethcommon "github.com/ethereum/go-ethereum/common"
)

type ID struct {
	Tx       uint64 // The transaction that created or loaded this profile.
	Address  ethcommon.Address
	Selector [4]byte
	UID      uint64 // Unique identifier of the callee (derived from address + selector)
}

func NewID(tx uint64, addr ethcommon.Address, selector [4]byte) *ID {
	return &ID{
		Tx:       tx,
		Address:  addr,
		Selector: selector,
		UID:      statecommon.DeriveEthCalleeUID(addr, selector),
	}
}

func NewIDFromStdMsg(msg *libtypes.StandardMessage) *ID {
	var addr [20]byte
	if msg.Native.To != nil {
		copy(addr[:], msg.Native.To.Bytes())
	}

	selector := codec.Bytes4{}.Decode(msg.Native.Data).([4]byte)

	return &ID{
		Address:  addr,
		Selector: selector,
		UID:      statecommon.DeriveEthCalleeUID(addr, selector),
	}
}
