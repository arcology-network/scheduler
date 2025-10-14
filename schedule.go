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
	"github.com/arcology-network/common-lib/exp/slice"
	eucommon "github.com/arcology-network/common-lib/types"
)

type Schedule struct {
	Transfers    []*eucommon.StandardMessage // Transfers
	Deployments  []*eucommon.StandardMessage // Contract deployments
	Unknowns     []*eucommon.StandardMessage // Messages with unknown conflicts with others
	WithConflict []*eucommon.StandardMessage // Messages with some known conflicts
	Sequentials  []*eucommon.StandardMessage // Callees that are marked as sequential only

	Generations [][][]*eucommon.StandardMessage
	CallCounts  []map[string]int
}

// The function outputs the optimized schedule. The shedule is a 3 dimensional array.
// The first dimension is the generation number. The second dimension is a set of
// parallel transaction arrays. These arrays are the transactions that can be executed in parallel.
// The third dimension is the transactions in the sequntial order.
func (this *Schedule) Finalize() [][][]*eucommon.StandardMessage {
	//  Transfers + deployments can be executed in parallel with withConflict + sequentials.
	_1 := slice.ConcateNonEmpty(func(v []*eucommon.StandardMessage) bool { return len(v) > 0 }, this.Transfers, this.Deployments, this.Unknowns)
	_2 := slice.ConcateNonEmpty(func(v []*eucommon.StandardMessage) bool { return len(v) > 0 }, this.WithConflict, this.Sequentials)

	// Reshape to 2D array.
	_1Gen := slice.Transform(_1, func(i int, msg *eucommon.StandardMessage) []*eucommon.StandardMessage {
		return []*eucommon.StandardMessage{msg}
	})

	_2Gen := slice.Transform(_2, func(i int, msg *eucommon.StandardMessage) []*eucommon.StandardMessage {
		return []*eucommon.StandardMessage{msg}
	})

	// sch := [][][]*eucommon.StandardMessage{}
	if len(_1Gen) > 0 {
		if len(this.Generations) == 0 {
			this.Generations = append(this.Generations, _1Gen)
		} else {
			// Merge with the first generation, since they are all parallel.
			this.Generations[0] = append(this.Generations[0], _1Gen...)
		}
	}

	if len(_2Gen) > 0 {
		this.Generations[0] = append(this.Generations[0], _2Gen...)
	}
	return this.Generations
}
