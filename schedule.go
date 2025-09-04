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
	Unknows      []*eucommon.StandardMessage // Messages with unknown conflicts with others
	WithConflict []*eucommon.StandardMessage // Messages with some known conflicts
	Sequentials  []*eucommon.StandardMessage // Callees that are marked as sequential only

	Generations [][][]*eucommon.StandardMessage
	CallCounts  []map[string]int
}

// The function outputs the optimized schedule. The shedule is a 3 dimensional array.
// The first dimension is the generation number. The second dimension is a set of
// parallel transaction arrays. These arrays are the transactions that can be executed in parallel.
// The third dimension is the transactions in the sequntial order.
func (this *Schedule) Optimize(scheduler *Scheduler) [][][]*eucommon.StandardMessage {

	//  Transfers + deployments can be executed in parallel with withConflict + sequentials.
	sch := [][][]*eucommon.StandardMessage{{
		append(this.Transfers, this.Deployments...),    // Transfers and deployments will be executed first in parallel
		append(this.WithConflict, this.Sequentials...), // Sequential only ones, can be empty.
	}}

	sch = append(sch, this.Generations...)

	// Txs with unknown conflicts will be next. Unknow may also need to schedule deferred calls.
	if len(this.Unknows) > 0 {
		_, msgSets := slice.GroupBy(this.Unknows, func(_ int, msg *eucommon.StandardMessage) *string {
			v := string(Compact(msg.Native.To[:], msg.Native.Data[:]))
			return &v
		})

		deferred := []*eucommon.StandardMessage{}
		for i, msgs := range msgSets {
			if len(msgs) >= 1 {
				// Check if a deferred call is needed.
				key := GenerateKey(msgs[0])                           // Key
				if idx, ok := scheduler.calleeDict[string(key)]; ok { // If a known callee is found.
					if !scheduler.callees[idx].Deferrable { // Check if the callee is specifically marked as deferrable.
						continue // No, skip this one.
					}
					// Schedule the deferred call
				} else {
					if !scheduler.deferByDefault { // The callee is not found, use the default value in this case.
						continue
					}
					// Schedule the deferred call
				}

				if len(msgs) == 1 {
					msgs[0].IsDeferred = true // Single deferred message.
					continue                  // Skip empty sets.
				}

				msg := slice.PopBack(&msgs) // Use the last message as the deferred call
				(*msg).IsDeferred = true    // Mark the message as deferred
				deferred = append(deferred, *msg)
				msgSets[i] = msgs
			}
		}

		// Add the preceeding messages to the schedule
		unknowsSeq := slice.Transform(slice.Flatten(msgSets), func(_ int, msg *eucommon.StandardMessage) []*eucommon.StandardMessage {
			return []*eucommon.StandardMessage{msg}
		})
		sch = append(sch, unknowsSeq)

		// Add deferred calls to the schedule
		deferredSeq := slice.Transform(deferred, func(_ int, msg *eucommon.StandardMessage) []*eucommon.StandardMessage {
			return []*eucommon.StandardMessage{msg}
		})
		sch = append(sch, deferredSeq)
	}

	// Remove empty generations if any.
	slice.RemoveIf(&sch, func(i int, gen [][]*eucommon.StandardMessage) bool {
		slice.RemoveIf(&gen, func(_ int, msgs []*eucommon.StandardMessage) bool {
			return len(msgs) == 0
		})
		return len(gen) == 0
	})

	return sch
}
