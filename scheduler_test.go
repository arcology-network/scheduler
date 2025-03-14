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
	"crypto/sha256"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"testing"
	"time"

	slice "github.com/arcology-network/common-lib/exp/slice"
	eucommon "github.com/arcology-network/common-lib/types"
	ethcommon "github.com/ethereum/go-ethereum/common"
	ethcore "github.com/ethereum/go-ethereum/core"
)

func TestSchedulerAddAndLoadConflicts(t *testing.T) {
	file := "./tmp/history"
	os.Remove(file) // Clean up the file if it exists

	// Create a new scheduler with default deferred flag being true
	sch, err := NewScheduler(file, true)
	if err != nil {
		t.Error(err)
	}

	alice := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	bob := []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	carol := []byte("cccccccccccccccccccccccccccccccccccccccc")
	david := []byte("dddddddddddddddddddddddddddddddddddddddd")

	// eva := []byte("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	// frank := []byte("ffffffffffffffffffffffffffffffffffffffff")

	// Add the conflict pairs to the scheduler
	sch.Add([20]byte(alice), [4]byte{1, 1, 1, 1}, [20]byte(bob), [4]byte{2, 2, 2, 2})
	sch.Add([20]byte(carol), [4]byte{3, 3, 3, 3}, [20]byte(david), [4]byte{4, 4, 4, 4})

	sch.Add([20]byte(alice), [4]byte{1, 1, 1, 1}, [20]byte(bob), [4]byte{2, 2, 2, 2})
	sch.Add([20]byte(carol), [4]byte{3, 3, 3, 3}, [20]byte(david), [4]byte{4, 4, 4, 4})

	if len(sch.callees) != 4 {
		t.Error("Failed to add contracts")
	}

	if err = SaveScheduler(sch, file); err != nil {
		t.Error(err)
	}

	sch, err = LoadScheduler(file)
	if err != nil {
		t.Error(err)
	}

	if len(sch.callees) != 4 {
		t.Error("Failed to add contracts")
	}

	if sch.Add([20]byte(alice), [4]byte{1, 1, 1, 1}, [20]byte(bob), [4]byte{2, 2, 2, 2}) {
		t.Error("Should not exist")
	}

	if !sch.Add([20]byte(alice), [4]byte{1, 2, 1, 1}, [20]byte(bob), [4]byte{2, 2, 2, 2}) {
		t.Error("Failed to add contracts")
	}
	os.Remove(file)
}

func TestSchedulerNoConflictWithDeferred(t *testing.T) {
	scheduler, _ := NewScheduler("", true) // No conflict db file.

	alice := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	aaddr := ethcommon.BytesToAddress(alice)

	callAlice0 := &eucommon.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{To: &aaddr, Data: []byte{5, 5, 5, 5, 0, 0, 0, 0}},
	}

	callAlice1 := &eucommon.StandardMessage{
		ID:     1,
		Native: &ethcore.Message{To: &aaddr, Data: []byte{5, 5, 5, 5, 1, 1, 1, 1}},
	}

	callAlice2 := &eucommon.StandardMessage{
		ID:     2,
		Native: &ethcore.Message{To: &aaddr, Data: []byte{5, 5, 5, 5, 2, 2, 2, 2}},
	}

	// Produce a new schedule for the given transactions based on the conflicts information.
	rawSch := scheduler.New([]*eucommon.StandardMessage{
		callAlice0,
		callAlice1,
		callAlice2,
	})

	if optimized := rawSch.Optimize(scheduler); len(optimized) != 2 || len(optimized[0]) != 2 || len(optimized[1]) != 1 {
		t.Error("Wrong generation size", optimized[0], optimized[1])
	}
}

func TestSchedulerNoConflictWithoutDeferred(t *testing.T) {
	scheduler, _ := NewScheduler("", false) // No conflict db file.

	alice := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	aaddr := ethcommon.BytesToAddress(alice)

	callAlice0 := &eucommon.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{To: &aaddr, Data: []byte{5, 5, 5, 5, 0, 0, 0, 0}},
	}

	callAlice1 := &eucommon.StandardMessage{
		ID:     1,
		Native: &ethcore.Message{To: &aaddr, Data: []byte{5, 5, 5, 5, 1, 1, 1, 1}},
	}

	callAlice2 := &eucommon.StandardMessage{
		ID:     2,
		Native: &ethcore.Message{To: &aaddr, Data: []byte{5, 5, 5, 5, 2, 2, 2, 2}},
	}

	// Produce a new schedule for the given transactions based on the conflicts information.
	rawSch := scheduler.New([]*eucommon.StandardMessage{
		callAlice0,
		callAlice1,
		callAlice2,
	})

	if optimized := rawSch.Optimize(scheduler); len(optimized) != 1 || len(optimized[0]) != 3 {
		t.Error("Wrong generation size", optimized[0])
	}
}

func TestSchedulerWithConflicInfo(t *testing.T) {
	scheduler, _ := NewScheduler("", true) // No conflict db file.

	alice := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	bob := []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	carol := []byte("cccccccccccccccccccccccccccccccccccccccc")
	david := []byte("dddddddddddddddddddddddddddddddddddddddd")

	scheduler.Add(
		[20]byte(alice), [4]byte{1, 1, 1, 1},
		[20]byte(bob), [4]byte{2, 2, 2, 2})

	scheduler.Add(
		[20]byte(carol), [4]byte{3, 3, 3, 3},
		[20]byte(david), [4]byte{4, 4, 4, 4})

	aaddr := ethcommon.BytesToAddress(alice)
	callAlice := &eucommon.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{To: &aaddr, Data: []byte{1, 1, 1, 1}},
	}

	baddr := ethcommon.BytesToAddress(bob)
	callBob := &eucommon.StandardMessage{
		ID:     1,
		Native: &ethcore.Message{To: &baddr, Data: []byte{2, 2, 2, 2}},
	}

	caddr := ethcommon.BytesToAddress(carol)
	callCarol := &eucommon.StandardMessage{
		ID:     2,
		Native: &ethcore.Message{To: &caddr, Data: []byte{3, 3, 3, 3}},
	}

	daddr := ethcommon.BytesToAddress(david)
	callDavid := &eucommon.StandardMessage{
		ID:     3,
		Native: &ethcore.Message{To: &daddr, Data: []byte{4, 4, 4, 4}},
	}

	// deploy := ethcommon.BytesToAddress([]byte{})
	deployment0 := &eucommon.StandardMessage{
		ID:     3,
		Native: &ethcore.Message{To: nil, Data: []byte{4, 4, 4, 4}},
	}

	transferAdd := ethcommon.BytesToAddress([]byte{})
	transfer := &eucommon.StandardMessage{
		ID:     3,
		Native: &ethcore.Message{To: &transferAdd, Value: big.NewInt(100), Data: []byte{}},
	}

	// Produce a new schedule for the given transactions based on the conflicts information.
	// There should be 3 generations in the schedule.
	// 1. [Transfer], [deployment]
	// 2. [Alice, Carol]
	// 3. [Bob, David]
	rawSch := scheduler.New([]*eucommon.StandardMessage{
		callAlice, // Conflict with callCarol
		callBob,
		callCarol, // Conflict with callAlice
		callDavid,
		deployment0,
		transfer,
	})

	if len(rawSch.Generations) != 2 || len(rawSch.Generations[0]) != 2 || len(rawSch.Generations[1]) != 2 {
		t.Error("Wrong generation size", len(rawSch.Generations))
	}

	if optimized := rawSch.Optimize(scheduler); len(optimized) != 3 || len(optimized[0]) != 2 || len(optimized[1]) != 2 {
		t.Error("Wrong optimized generation size", len(optimized))
	}

	// Check that the schedule is correct.
	msgs := make([]*eucommon.StandardMessage, 10)
	for i := range msgs {
		h := sha256.Sum256([]byte(strconv.Itoa(i)))
		addr := ethcommon.BytesToAddress(h[:])
		msgs[i] = &eucommon.StandardMessage{
			ID:     uint64(i),
			Native: &ethcore.Message{To: &addr, Data: addr[:4]},
		}
	}
	msgs = slice.Join(msgs, slice.New(1000000, callAlice))

	t0 := time.Now()
	scheduler.New(msgs)
	fmt.Println("Scheduler", len(msgs), time.Since(t0))
}
