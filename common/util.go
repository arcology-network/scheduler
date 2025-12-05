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

// The function creates a unique func Key representation of the callee information
// func DeriveKey(addr []byte, selector []byte) []byte {
// 	return codec.Uint64(DeriveUID(addr, selector)).Encode()
// }

// Derive the callee UID from address and selector bytes by taking the first
// 8 bytes of their concatenation as a uint64.
// func DeriveUID(addr []byte, selector []byte) uint64 {
// 	address := make([]byte, statecommon.SHORT_CONTRACT_ADDRESS_LENGTH)
// 	copy(address, addr)

// 	selectorBytes := make([]byte, statecommon.SELECTOR_LENGTH)
// 	copy(selectorBytes, selector)

// 	return uint64(codec.Uint64(0).FromBytes(append(address, selectorBytes...)))
// }

// Derive the callee UID from a storage path string.
// func DeriveUIDFromPath(path string) uint64 {
// 	addr, selector, err := statecommon.ParseAddressAndSelector(path)
// 	if err != nil {
// 		return 0
// 	}
// 	return DeriveUID(addr[:], selector[:])
// }

// Get the callee key from a message
// func ToKey(msg *commontype.StandardMessage) uint64 {
// 	toAddr, selector := msg.GetAddressAndSelector()
// 	return DeriveUID(toAddr[:], selector[:])
// }
