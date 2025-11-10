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
	"testing"
	"time"
)

func TestCalleeProfileInfo(t *testing.T) {
	numCalls := 10

	callees := make([]*CalleeProfile, numCalls)
	for i := 0; i < numCalls; i++ {
		callee := &CalleeProfile{}
		callee.ConflictWith = []uint32{1, 2, 3, 4, 5, 6, 7, 8, 1, 2}
		callee.Sequential = true
		callees[i] = callee
	}

	t0 := time.Now()
	for i := 0; i < numCalls; i++ {
		encoded, err := callees[i].Encode()
		if err != nil {
			t.Error(err)
		}
		callee2 := &CalleeProfile{}
		callee2.Decode(encoded)

		if !callees[i].Equal(callee2) {
			t.Error("Failed to encode/decode")
		}
	}
	t.Log("Time Spent to encode / Decode ", numCalls, " entries: ", time.Since(t0))
}

func TestCalleeProfile(t *testing.T) {
	numCalls := 1000000

	callees := make([]*CalleeProfile, numCalls)
	for i := 0; i < numCalls; i++ {
		callees[i] = &CalleeProfile{
			UID:          uint64(i),
			ContractID:   uint32(i),
			Sequential:   false,
			TotalCalls:   uint32(i),
			MaxGas:       uint64(i * 100),
			Deferrable:   true,
			Prepayment:   uint64(i * 11),
			ConflictWith: []uint32{1, 2, 3, 4},
		}
	}

	for i := 0; i < numCalls; i++ {
		encoded, err := callees[i].Encode()
		if err != nil {
			t.Error(err)
		}
		callee2 := &CalleeProfile{}
		callee2.Decode(encoded)

		if !callees[i].Equal(callee2) {
			t.Error("Failed to encode/decode")
		}
	}
}

// func BenchmarkTestCalleeProfile(t *testing.B) {
// 	numCalls := 1000000

// 	callees := make([]*CalleeProfile, numCalls)
// 	for i := 0; i < numCalls; i++ {
// 		callees[i] = &CalleeProfile{
// 			Index:        uint32(i),
// 			AddrAndSign:  new(codec.Bytes12).FromBytes([]byte{1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4}),
// 			ConflictWith: []uint32{1, 2, 3, 4},
// 			Sequential:   false,
// 			Calls:        uint32(i),
// 		}
// 	}

// 	t0 := time.Now()
// 	for i := 0; i < numCalls; i++ {
// 		encoded, err := callees[i].Encode()
// 		if err != nil {
// 			t.Error(err)
// 		}
// 		callee2 := &CalleeProfile{}
// 		callee2.Decode(encoded)
// 	}
// 	t.Log("Time Spend to encode / Decode :", numCalls, time.Since(t0))
// }
