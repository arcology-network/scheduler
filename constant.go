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
	"github.com/arcology-network/common-lib/exp/slice"

	stgcommon "github.com/arcology-network/storage-committer/common"
)

func CallToKey(addr []byte, funSign []byte) string {
	return string(addr[:stgcommon.FUNCTION_SIGNATURE_LENGTH]) + string(funSign[:stgcommon.FUNCTION_SIGNATURE_LENGTH])
}

// The function creates a compact representation of the callee information
func Compact(addr []byte, funSign []byte) []byte {
	addr = slice.Clone(addr) // Make sure the original data is not modified
	return append(addr[:stgcommon.SHORT_CONTRACT_ADDRESS_LENGTH], funSign[:stgcommon.FUNCTION_SIGNATURE_LENGTH]...)
}
