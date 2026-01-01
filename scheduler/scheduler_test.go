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
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/arcology-network/common-lib/crdt/statecell"
	queue "github.com/arcology-network/common-lib/exp/queue"
	"github.com/arcology-network/common-lib/exp/slice"
	eucommon "github.com/arcology-network/common-lib/types"
	callee "github.com/arcology-network/scheduler/callee"
	profile "github.com/arcology-network/scheduler/callee"
	workload "github.com/arcology-network/scheduler/workload"
	stateengine "github.com/arcology-network/state-engine"
	statecommon "github.com/arcology-network/state-engine/common"
	statecommitter "github.com/arcology-network/state-engine/state/committer"
	proxy "github.com/arcology-network/state-engine/storage/proxy"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"

	// "github.com/ethereum/go-ethereum/common/hexutil"
	ethcore "github.com/ethereum/go-ethereum/core"
)

func CreateAccountInStore(accts ...[20]byte) (*stateengine.StateStore, error) {
	sstore := stateengine.NewStateStore(proxy.NewMemDBStoreProxy())
	writeCache := sstore.StateCache

	for _, sender := range accts {
		if _, err := statecommon.CreateDefaultPaths(1, hexutil.Encode(sender[:]), writeCache); err != nil { // NewAccount account structure {
			return nil, errors.New("Failed to create default paths for " + hexutil.Encode(sender[:]))
		}
	}

	raw := writeCache.Export(statecell.Sorter)
	acctTrans := statecell.StateCells(slice.Clone(raw)).To(statecell.InterProcTransition{})

	buffer := statecell.StateCells(acctTrans).Encode()
	statecell.StateCells{}.Decode(buffer)

	committer := statecommitter.NewStateCommitter(sstore, sstore.GetWriters())
	committer.Import(acctTrans)
	committer.Precommit([]uint64{1})
	committer.DebugCommit(10)
	return sstore, nil
}

func TestSchedulerNoConflictNoDeferred(t *testing.T) {
	alice := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	aaddr := ethcommon.BytesToAddress(alice)

	sender1 := [20]byte{0x01}
	// sender2 := [20]byte{0x02}
	// sender3 := [20]byte{0x03}

	callAlice0 := &eucommon.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: sender1, To: &aaddr, Data: []byte{5, 5, 5, 5, 0, 0, 0, 0}},
	}

	callAlice1 := &eucommon.StandardMessage{
		ID:     1,
		Native: &ethcore.Message{From: sender1, To: &aaddr, Data: []byte{5, 5, 5, 5, 1, 1, 1, 1}},
	}

	callAlice2 := &eucommon.StandardMessage{
		ID:     2,
		Native: &ethcore.Message{From: sender1, To: &aaddr, Data: []byte{5, 5, 5, 5, 2, 2, 2, 2}},
	}

	sstore, storeErr := CreateAccountInStore(sender1)
	if storeErr != nil {
		t.Error(storeErr)
	}

	// Produce a new schedule for the given transactions based on the conflicts information.
	mgr := callee.NewProfileManager(sstore, 1000000)
	scheduler, _ := NewScheduler(mgr) // No conflict db file.
	rawSch, err := scheduler.New([]*eucommon.StandardMessage{
		callAlice0,
		callAlice1,
		callAlice2,
	})

	if err != nil {
		t.Error("Failed to New schedule:", err)
	}

	if len(rawSch.Generations) != 1 || len(rawSch.Generations[0].JobSeqs) != 3 {
		t.Error("Wrong generation size")
	}
}

func TestSchedulerWithConflicInfo(t *testing.T) {
	alice := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	bob := []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	carol := []byte("cccccccccccccccccccccccccccccccccccccccc")
	david := []byte("dddddddddddddddddddddddddddddddddddddddd")

	aaddr := ethcommon.BytesToAddress(alice)
	callAlice := &eucommon.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: [20]byte(alice), To: &aaddr, Nonce: 0, Data: []byte{1, 1, 1, 1}},
	}

	baddr := ethcommon.BytesToAddress(bob)
	callBob := &eucommon.StandardMessage{
		ID:     1,
		Native: &ethcore.Message{From: [20]byte(bob), To: &baddr, Nonce: 0, Data: []byte{2, 2, 2, 2}},
	}

	caddr := ethcommon.BytesToAddress(carol)
	callCarol := &eucommon.StandardMessage{
		ID:     2,
		Native: &ethcore.Message{From: [20]byte(carol), Nonce: 0, To: &caddr, Data: []byte{3, 3, 3, 3}},
	}

	daddr := ethcommon.BytesToAddress(david)
	callDavid := &eucommon.StandardMessage{
		ID:     3,
		Native: &ethcore.Message{From: [20]byte(david), Nonce: 0, To: &daddr, Data: []byte{4, 4, 4, 4}},
	}

	// deploy := ethcommon.BytesToAddress([]byte{})
	deployment0 := &eucommon.StandardMessage{
		ID:     4,
		Native: &ethcore.Message{From: [20]byte(alice), To: nil, Nonce: 1, Data: []byte{4, 4, 4, 4}},
	}

	transferAdd := ethcommon.BytesToAddress([]byte{})
	transfer := &eucommon.StandardMessage{
		ID:     5,
		Native: &ethcore.Message{From: [20]byte(alice), To: &transferAdd, Nonce: 2, Value: big.NewInt(100), Data: []byte{}},
	}

	sstore, storeErr := CreateAccountInStore(
		[20]byte(alice),
		[20]byte(bob),
		[20]byte(carol),
		[20]byte(david),
	)
	if storeErr != nil {
		t.Error("Failed to initialize account in store:", storeErr)
	}

	mgr := callee.NewProfileManager(sstore, 1000000)
	scheduler, _ := NewScheduler(mgr) // No conflict db file.
	// registerion have problem
	scheduler.ProfileStore.RegisterNewConflict(
		profile.NewID([20]byte(alice), [4]byte{1, 1, 1, 1}),
		profile.NewID([20]byte(bob), [4]byte{2, 2, 2, 2}))

	scheduler.ProfileStore.RegisterNewConflict(
		profile.NewID([20]byte(carol), [4]byte{3, 3, 3, 3}),
		profile.NewID([20]byte(david), [4]byte{4, 4, 4, 4}))

	// Produce a new schedule for the given transactions based on the conflicts information.
	// There should be 3 generations in the schedule.
	// 1. [Transfer], [deployment], [Alice], [Carol]
	// 2. [Bob, David]
	rawSch, err := scheduler.New([]*eucommon.StandardMessage{
		callAlice, // Conflict with callCarol
		callBob,
		callCarol, // Conflict with callAlice
		callDavid,
		deployment0,
		transfer,
	})

	if err != nil {
		t.Error("Failed to create schedule:", err)
	}

	msgIDSet := rawSch.ExportMsgIDs(scheduler.ProfileStore.Backend())
	_0 := slice.Flatten(msgIDSet[0])
	_1 := slice.Flatten(msgIDSet[1])

	// concurrency issue here
	if len(msgIDSet) != 2 ||
		len(msgIDSet[0]) != 4 ||
		!slice.ContentEquivalent(_0, []uint64{0, 2, 4, 5}) ||
		len(msgIDSet[1]) != 2 ||
		!slice.ContentEquivalent(_1, []uint64{1, 3}) {
		t.Error("Wrong generation size",
			len(rawSch.Generations),
			len(rawSch.Generations[0].JobSeqs))

		fmt.Println(_0)
		fmt.Println(_1)
	}
}

func TestCategorizeNonceOrderingWithinEachQueue(t *testing.T) {
	alice := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	bob := []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	carol := []byte("cccccccccccccccccccccccccccccccccccccccc")
	david := []byte("dddddddddddddddddddddddddddddddddddddddd")

	sender1 := [20]byte{0x01}
	sender2 := [20]byte{0x02}
	sender3 := [20]byte{0x03}

	// s := &Scheduler{}

	aaddr := ethcommon.BytesToAddress(alice)
	callAlice := &eucommon.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: sender1, Nonce: 0, To: &aaddr, Data: []byte{1, 1, 1, 1}},
	}

	baddr := ethcommon.BytesToAddress(bob)
	callBob := &eucommon.StandardMessage{
		ID:     1,
		Native: &ethcore.Message{From: sender1, Nonce: 1, To: &baddr, Data: []byte{2, 2, 2, 2}},
	}

	caddr := ethcommon.BytesToAddress(carol)
	callCarol := &eucommon.StandardMessage{
		ID:     2,
		Native: &ethcore.Message{From: sender2, Nonce: 0, To: &caddr, Data: []byte{3, 3, 3, 3}},
	}

	daddr := ethcommon.BytesToAddress(david)
	callDavid := &eucommon.StandardMessage{
		ID:     3,
		Native: &ethcore.Message{From: sender3, Nonce: 0, To: &daddr, Data: []byte{4, 4, 4, 4}},
	}

	j1 := &workload.Job{
		ID:     0,
		StdMsg: callAlice,
	}

	j2 := &workload.Job{
		ID:     0,
		StdMsg: callBob,
	}

	j3 := &workload.Job{
		ID:     0,
		StdMsg: callCarol,
	}

	j4 := &workload.Job{
		ID:     0,
		StdMsg: callDavid,
	}

	pending := []*workload.Job{j1, j2, j3, j4}

	// s.Categorize(pending)

	_, groups := slice.GroupBy(pending,
		func(_ int, job *workload.Job) *ethcommon.Address {
			return &job.StdMsg.Native.From
		})

	isNonceSorted := func(q *queue.Queue[*workload.Job]) bool {
		for i := 1; i < len(*q); i++ {
			if (*q)[i-1].StdMsg.Native.Nonce >= (*q)[i].StdMsg.Native.Nonce {
				return false
			}
		}
		return true
	}

	for _, g := range groups {
		q := queue.NewSortedQueueFromSlice(g, func(a, b *workload.Job) bool {
			return a.StdMsg.Native.Nonce < b.StdMsg.Native.Nonce
		})

		if !isNonceSorted(q) {
			t.Fatalf("queue not sorted by nonce")
		}
	}
}

func TestOffsetingNoncesSimple(t *testing.T) {
	alice := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	bob := []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	aaddr := ethcommon.BytesToAddress(alice)
	callAlice := &eucommon.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: [20]byte{0x01}, Nonce: 0, To: &aaddr, Data: []byte{1, 1, 1, 1}},
	}

	baddr := ethcommon.BytesToAddress(bob)
	callBob := &eucommon.StandardMessage{
		ID:     1,
		Native: &ethcore.Message{From: [20]byte{0x01}, Nonce: 1, To: &baddr, Data: []byte{2, 2, 2, 2}},
	}

	sstore, storeErr := CreateAccountInStore([20]byte{0x01})
	if storeErr != nil {
		t.Error("Failed to create account in store:", storeErr)
	}

	mgr := callee.NewProfileManager(sstore, 1000000)
	scheduler, _ := NewScheduler(mgr) // No conflict db file.

	rawSch, err := scheduler.New([]*eucommon.StandardMessage{callAlice, callBob})
	if err != nil {
		t.Error("Failed to New schedule:", err)
	}

	if len(rawSch.Generations) != 1 ||
		len(rawSch.Generations[0].JobSeqs) != 2 ||
		len(rawSch.Generations[0].JobSeqs[0].PreStateTransitions) != 0 ||
		len(rawSch.Generations[0].JobSeqs[1].PreStateTransitions) != 1 {
		t.Error("Wrong PreState size", len(rawSch.Generations[0].JobSeqs[0].PreStateTransitions),
			len(rawSch.Generations[0].JobSeqs[1].PreStateTransitions))
	}
}

func TestOffsetingNoncesWithWriteStorage(t *testing.T) {
	alice := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	bob := []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	carol := []byte("cccccccccccccccccccccccccccccccccccccccc")
	david := []byte("dddddddddddddddddddddddddddddddddddddddd")

	sender1 := [20]byte{0x01}
	sender2 := [20]byte{0x02}
	sender3 := [20]byte{0x03}

	// s := &Scheduler{}

	aaddr := ethcommon.BytesToAddress(alice)
	callAlice := &eucommon.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: sender1, Nonce: 0, To: &aaddr, Data: []byte{1, 1, 1, 1}},
	}

	baddr := ethcommon.BytesToAddress(bob)
	callBob := &eucommon.StandardMessage{
		ID:     1,
		Native: &ethcore.Message{From: sender1, Nonce: 1, To: &baddr, Data: []byte{2, 2, 2, 2}},
	}

	caddr := ethcommon.BytesToAddress(carol)
	callCarol := &eucommon.StandardMessage{
		ID:     2,
		Native: &ethcore.Message{From: sender2, Nonce: 0, To: &caddr, Data: []byte{3, 3, 3, 3}},
	}

	daddr := ethcommon.BytesToAddress(david)
	callDavid := &eucommon.StandardMessage{
		ID:     3,
		Native: &ethcore.Message{From: sender3, Nonce: 0, To: &daddr, Data: []byte{4, 4, 4, 4}},
	}

	sstore, storeErr := CreateAccountInStore(sender1, sender2, sender3)
	if storeErr != nil {
		t.Error("Failed to initialize account in store:", storeErr)
	}

	mgr := callee.NewProfileManager(sstore, 1000000)
	scheduler, _ := NewScheduler(mgr) // No conflict db file.

	_, err := scheduler.New([]*eucommon.StandardMessage{callAlice, callBob, callCarol, callDavid})
	if err != nil {
		t.Error("Failed to New schedule:", err)
	}
}
