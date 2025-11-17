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
	//  "github.com/arcology-network/scheduler"
	libcommon "github.com/arcology-network/common-lib/types"
)

// Callee is a wrapper of StandardMessage with additional profiling information.
// So the scheduler can make better decisions based on the historical conflict profiles.
type Callee struct {
	LastVisit uint64 `json:"lastVisit"` // Last visit block height

	UID        uint64 `json:"uid"` // Unique identifier of the callee (derived from address + selector)
	UsageCount uint64
	Msg        *libcommon.StandardMessage
	profile    *Profile
}

func NewCallee(msg *libcommon.StandardMessage, uid uint64) *Callee {
	return &Callee{
		UID:     uid,
		Msg:     msg,
		profile: nil,
	}
}
