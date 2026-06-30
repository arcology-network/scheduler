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
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/arcology-network/common-lib/crdt/commutative"
	"github.com/arcology-network/common-lib/crdt/statecell"
	queue "github.com/arcology-network/common-lib/exp/queue"
	"github.com/arcology-network/common-lib/exp/slice"
	libcommontype "github.com/arcology-network/common-lib/types"
	callee "github.com/arcology-network/scheduler/callee"
	profile "github.com/arcology-network/scheduler/callee"
	"github.com/arcology-network/scheduler/conflictor"
	workload "github.com/arcology-network/scheduler/workload"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"

	statetestharness "github.com/arcology-network/state-engine/test/harness"

	// "github.com/ethereum/go-ethereum/common/hexutil"
	ethcore "github.com/ethereum/go-ethereum/core"
)

func TestSchedulerNoConflictNoDeferred(t *testing.T) {
	alice := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	aaddr := ethcommon.BytesToAddress(alice)

	sender1 := [20]byte{0x01}
	// sender2 := [20]byte{0x02}
	// sender3 := [20]byte{0x03}

	callAlice0 := &libcommontype.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: sender1, To: &aaddr, Data: []byte{5, 5, 5, 5, 0, 0, 0, 0}},
	}

	callAlice1 := &libcommontype.StandardMessage{
		ID:     1,
		Native: &ethcore.Message{From: sender1, To: &aaddr, Data: []byte{5, 5, 5, 5, 1, 1, 1, 1}},
	}

	callAlice2 := &libcommontype.StandardMessage{
		ID:     2,
		Native: &ethcore.Message{From: sender1, To: &aaddr, Data: []byte{5, 5, 5, 5, 2, 2, 2, 2}},
	}

	sstore, storeErr := statetestharness.CreateAccountInStore(sender1)
	if storeErr != nil {
		t.Error(storeErr)
	}

	// Produce a new schedule for the given transactions based on the conflicts information.
	pStore := callee.NewProfileStore(sstore.CommittedStore())
	scheduler, _ := NewScheduler(pStore) // No conflict db file.
	rawSch, err := scheduler.New([]*libcommontype.StandardMessage{
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

func TestSchedulerWithConflic(t *testing.T) {
	alice := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	bob := []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	carol := []byte("cccccccccccccccccccccccccccccccccccccccc")
	david := []byte("dddddddddddddddddddddddddddddddddddddddd")

	aaddr := ethcommon.BytesToAddress(alice)
	callAlice := &libcommontype.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: [20]byte(alice), To: &aaddr, Nonce: 0, Data: []byte{1, 1, 1, 1}},
	}

	baddr := ethcommon.BytesToAddress(bob)
	callBob := &libcommontype.StandardMessage{
		ID:     1,
		Native: &ethcore.Message{From: [20]byte(bob), To: &baddr, Nonce: 0, Data: []byte{2, 2, 2, 2}},
	}

	caddr := ethcommon.BytesToAddress(carol)
	callCarol := &libcommontype.StandardMessage{
		ID:     2,
		Native: &ethcore.Message{From: [20]byte(carol), Nonce: 0, To: &caddr, Data: []byte{3, 3, 3, 3}},
	}

	daddr := ethcommon.BytesToAddress(david)
	callDavid := &libcommontype.StandardMessage{
		ID:     3,
		Native: &ethcore.Message{From: [20]byte(david), Nonce: 0, To: &daddr, Data: []byte{4, 4, 4, 4}},
	}

	// deploy := ethcommon.BytesToAddress([]byte{})
	deployment0 := &libcommontype.StandardMessage{
		ID:     4,
		Native: &ethcore.Message{From: [20]byte(alice), To: nil, Nonce: 1, Data: []byte{4, 4, 4, 4}},
	}

	transferAdd := ethcommon.BytesToAddress([]byte{})
	transfer := &libcommontype.StandardMessage{
		ID:     5,
		Native: &ethcore.Message{From: [20]byte(alice), To: &transferAdd, Nonce: 2, Value: big.NewInt(100), Data: []byte{}},
	}

	sstore, storeErr := statetestharness.CreateAccountInStore(
		[20]byte(alice),
		[20]byte(bob),
		[20]byte(carol),
		[20]byte(david),
	)
	if storeErr != nil {
		t.Error("Failed to initialize account in store:", storeErr)
	}

	pStore := callee.NewProfileStore(sstore.CommittedStore())
	scheduler, _ := NewScheduler(pStore) // No conflict db file.
	// registerion have problem
	profile.DebugRegisterNewConflict(
		scheduler.ProfileStore,
		profile.NewID(0, [20]byte(alice), [4]byte{1, 1, 1, 1}),
		profile.NewID(0, [20]byte(bob), [4]byte{2, 2, 2, 2}))

	profile.DebugRegisterNewConflict(
		scheduler.ProfileStore,
		profile.NewID(1, [20]byte(carol), [4]byte{3, 3, 3, 3}),
		profile.NewID(1, [20]byte(david), [4]byte{4, 4, 4, 4}))

	// Produce a new schedule for the given transactions based on the conflicts information.
	// There should be 3 generations in the schedule.
	// 1. [Transfer], [deployment], [Alice], [Carol]
	// 2. [Bob, David]
	rawSch, err := scheduler.New([]*libcommontype.StandardMessage{
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

	if len(rawSch.Generations) != 1 ||
		len(rawSch.Generations[0].JobSeqs) != 4 ||
		rawSch.Generations[0].NumJobs() != 6 {
		t.Error("Wrong generation size", len(rawSch.Generations))
	}

	// msgIDSet := rawSch.ExportMsgIDs(scheduler.ProfileStore.StateStore())
	// _0 := slice.Flatten(msgIDSet[0])

	// _1 := slice.Flatten(msgIDSet[1])

	// concurrency issue here
	// if len(msgIDSet) != 2 ||
	// 	len(msgIDSet[0]) != 4 ||
	// 	!slice.ContentEquivalent(_0, []uint64{0, 2, 4, 5}) ||
	// 	len(msgIDSet[1]) != 2 ||
	// 	!slice.ContentEquivalent(_1, []uint64{1, 3}) {
	// 	t.Error("Wrong generation size",
	// 		len(rawSch.Generations),
	// 		len(rawSch.Generations[0].JobSeqs))

	// 	fmt.Println(_0)
	// 	fmt.Println(_1)
	// }
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
	callAlice := &libcommontype.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: sender1, Nonce: 0, To: &aaddr, Data: []byte{1, 1, 1, 1}},
	}

	baddr := ethcommon.BytesToAddress(bob)
	callBob := &libcommontype.StandardMessage{
		ID:     1,
		Native: &ethcore.Message{From: sender1, Nonce: 1, To: &baddr, Data: []byte{2, 2, 2, 2}},
	}

	caddr := ethcommon.BytesToAddress(carol)
	callCarol := &libcommontype.StandardMessage{
		ID:     2,
		Native: &ethcore.Message{From: sender2, Nonce: 0, To: &caddr, Data: []byte{3, 3, 3, 3}},
	}

	daddr := ethcommon.BytesToAddress(david)
	callDavid := &libcommontype.StandardMessage{
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
	callAlice := &libcommontype.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: [20]byte{0x01}, Nonce: 0, To: &aaddr, Data: []byte{1, 1, 1, 1}},
	}

	baddr := ethcommon.BytesToAddress(bob)
	callBob := &libcommontype.StandardMessage{
		ID:     1,
		Native: &ethcore.Message{From: [20]byte{0x01}, Nonce: 1, To: &baddr, Data: []byte{2, 2, 2, 2}},
	}

	sstore, storeErr := statetestharness.CreateAccountInStore([20]byte{0x01})
	if storeErr != nil {
		t.Error("Failed to create account in store:", storeErr)
	}

	pStore := callee.NewProfileStore(sstore.CommittedStore())
	scheduler, _ := NewScheduler(pStore) // No conflict db file.

	rawSch, err := scheduler.New([]*libcommontype.StandardMessage{callAlice, callBob})
	if err != nil {
		t.Error("Failed to New schedule:", err)
	}

	if len(rawSch.Generations) != 1 ||
		len(rawSch.Generations[0].JobSeqs) != 2 ||
		len(rawSch.Generations[0].JobSeqs[0].PreTransitions) != 0 ||
		len(rawSch.Generations[0].JobSeqs[1].PreTransitions) != 1 {
		t.Error("Wrong PreState size", len(rawSch.Generations[0].JobSeqs[0].PreTransitions),
			len(rawSch.Generations[0].JobSeqs[1].PreTransitions))
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
	callAlice := &libcommontype.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: sender1, Nonce: 0, To: &aaddr, Data: []byte{1, 1, 1, 1}},
	}

	baddr := ethcommon.BytesToAddress(bob)
	callBob := &libcommontype.StandardMessage{
		ID:     1,
		Native: &ethcore.Message{From: sender1, Nonce: 1, To: &baddr, Data: []byte{2, 2, 2, 2}},
	}

	caddr := ethcommon.BytesToAddress(carol)
	callCarol := &libcommontype.StandardMessage{
		ID:     2,
		Native: &ethcore.Message{From: sender2, Nonce: 0, To: &caddr, Data: []byte{3, 3, 3, 3}},
	}

	daddr := ethcommon.BytesToAddress(david)
	callDavid := &libcommontype.StandardMessage{
		ID:     3,
		Native: &ethcore.Message{From: sender3, Nonce: 0, To: &daddr, Data: []byte{4, 4, 4, 4}},
	}

	sstore, storeErr := statetestharness.CreateAccountInStore(sender1, sender2, sender3)
	if storeErr != nil {
		t.Error("Failed to initialize account in store:", storeErr)
	}

	pStore := callee.NewProfileStore(sstore.CommittedStore())
	scheduler, _ := NewScheduler(pStore) // No conflict db file.

	_, err := scheduler.New([]*libcommontype.StandardMessage{callAlice, callBob, callCarol, callDavid})
	if err != nil {
		t.Error("Failed to New schedule:", err)
	}
}

func TestMultiGenerationMergeToOneSequence(t *testing.T) {
	// alice := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	// bob := []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	// carol := []byte("cccccccccccccccccccccccccccccccccccccccc")
	// david := []byte("dddddddddddddddddddddddddddddddddddddddd")

	sender1 := [20]byte{0x01}
	sender2 := [20]byte{0x02}
	sender3 := [20]byte{0x03}

	// s := &Scheduler{}

	contract0 := ethcommon.BytesToAddress([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	mockFunc0 := [4]byte{1, 1, 1, 1}
	_0CallContractAdd0 := &libcommontype.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: sender1, Nonce: 0, To: &contract0, Data: mockFunc0[:]},
	}

	_1CallContractAdd0 := &libcommontype.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: sender2, Nonce: 0, To: &contract0, Data: mockFunc0[:]},
	}

	_2CallContractAdd0 := &libcommontype.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: sender3, Nonce: 0, To: &contract0, Data: mockFunc0[:]},
	}

	sstore, storeErr := statetestharness.CreateAccountInStore(sender1, sender2, sender3)
	if storeErr != nil {
		t.Error("Failed to initialize account in store:", storeErr)
	}

	pStore := callee.NewProfileStore(sstore.CommittedStore())
	scheduler, _ := NewScheduler(pStore) // No conflict db file.

	_, _, schErr := profile.DebugRegisterNewConflict(
		scheduler.ProfileStore,
		profile.NewID(0, contract0, mockFunc0),
		profile.NewID(0, contract0, mockFunc0),
	)

	if schErr != nil {
		t.Error("Failed to register new conflict:", schErr)
	}

	rawSch, err := scheduler.New([]*libcommontype.StandardMessage{
		_0CallContractAdd0, // Conflict with callCarol
		_1CallContractAdd0,
		_2CallContractAdd0,
	})

	if err != nil {
		t.Error("Failed to create schedule:", err)
	}

	if len(rawSch.Generations) != 1 {
		t.Error("Wrong generation size", len(rawSch.Generations))
	}

	if len(rawSch.Generations[0].JobSeqs) != 1 {
		t.Error("Wrong JobSeqs size", len(rawSch.Generations[0].JobSeqs))
	}

	if rawSch.TotalJobs() != 3 {
		t.Error("Wrong job size", len(rawSch.Generations[0].JobSeqs[0].Jobs))
	}
}

func TestMultiGenerationMerge(t *testing.T) {
	sender1 := [20]byte{0x01}
	sender2 := [20]byte{0x02}
	sender3 := [20]byte{0x03}

	contract0 := ethcommon.BytesToAddress([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	mockFunc0 := [4]byte{1, 1, 1, 1}

	contract1 := ethcommon.BytesToAddress([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"))
	mockFunc1 := [4]byte{2, 2, 2, 2}

	// Initialize the profile store with the conflict pairs.
	sstore, storeErr := statetestharness.CreateAccountInStore(sender1, sender2, sender3)
	if storeErr != nil {
		t.Error("Failed to initialize account in store:", storeErr)
	}
	pStore := callee.NewProfileStore(sstore.CommittedStore())
	scheduler, _ := NewScheduler(pStore) // No conflict db file.

	// Register the conflict pairs to the scheduler, so the TXs calling
	// these two functions are serialized into separate execution generations.
	p0, p1, schErr := profile.DebugRegisterNewConflict(
		scheduler.ProfileStore,
		profile.NewID(0, contract0, mockFunc0),
		profile.NewID(0, contract1, mockFunc1),
	)

	if schErr != nil {
		t.Error("Failed to register new conflict:", schErr)
	}

	// Enable deferred execution for the two functions.
	p0.SetPrepayment(100)
	p1.SetPrepayment(200)

	// Produce a new schedule for the given transactions based on the conflicts information.
	_0_0_CallContractAdd0 := &libcommontype.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: sender1, Nonce: 0, To: &contract0, Data: mockFunc0[:]},
	}

	_0_1_CallContractAdd0 := &libcommontype.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: sender2, Nonce: 0, To: &contract0, Data: mockFunc0[:]},
	}

	_0_2_CallContractAdd0 := &libcommontype.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: sender3, Nonce: 0, To: &contract0, Data: mockFunc0[:]},
	}

	_0_3_CallContractAdd0 := &libcommontype.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: sender3, Nonce: 1, To: &contract0, Data: mockFunc0[:]},
	}

	_1_0_CallContractAdd1 := &libcommontype.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: sender1, Nonce: 1, To: &contract1, Data: mockFunc1[:]},
	}

	_1_1_CallContractAdd1 := &libcommontype.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: sender2, Nonce: 1, To: &contract1, Data: mockFunc1[:]},
	}

	_1_2_CallContractAdd1 := &libcommontype.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: sender3, Nonce: 1, To: &contract1, Data: mockFunc1[:]},
	}

	_1_3_CallContractAdd1 := &libcommontype.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: sender1, Nonce: 2, To: &contract1, Data: mockFunc1[:]},
	}

	rawSch, err := scheduler.New([]*libcommontype.StandardMessage{
		_0_0_CallContractAdd0, // Conflict with callCarol
		_0_1_CallContractAdd0,
		_0_2_CallContractAdd0,
		_0_3_CallContractAdd0,

		_1_0_CallContractAdd1, // Conflict with callCarol
		_1_1_CallContractAdd1,
		_1_2_CallContractAdd1,
		_1_3_CallContractAdd1,
	})

	if err != nil {
		t.Error("Failed to create schedule:", err)
	}

	if len(rawSch.Generations) != 4 {
		t.Error("Wrong generation size", len(rawSch.Generations))
	}
}

func TestFullWorkflow(t *testing.T) {
	sender1 := [20]byte{0x01}
	sender2 := [20]byte{0x02}
	sender3 := [20]byte{0x03}
	sender4 := [20]byte{0x04}

	contract0 := ethcommon.BytesToAddress([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	mockFunc0 := [4]byte{1, 1, 1, 1}

	contract1 := ethcommon.BytesToAddress([]byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"))
	mockFunc1 := [4]byte{2, 2, 2, 2}

	callContract00 := &libcommontype.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{From: sender1, Nonce: 0, To: &contract0, Data: mockFunc0[:]},
		TxHash: [32]byte{0x1},
	}

	callContract01 := &libcommontype.StandardMessage{
		ID:     1,
		Native: &ethcore.Message{From: sender1, Nonce: 1, To: &contract0, Data: mockFunc0[:]},
		TxHash: [32]byte{0x2},
	}

	callContract10 := &libcommontype.StandardMessage{
		ID:     2,
		Native: &ethcore.Message{From: sender2, Nonce: 0, To: &contract1, Data: mockFunc1[:]},
		TxHash: [32]byte{0x3},
	}

	callContract11 := &libcommontype.StandardMessage{
		ID:     3,
		Native: &ethcore.Message{From: sender3, Nonce: 0, To: &contract1, Data: mockFunc1[:]},
		TxHash: [32]byte{0x4},
	}

	sstore, storeErr := statetestharness.CreateAccountInStore(
		sender1,
		sender2,
		sender3,
		sender4,
		contract0,
		contract1,
	)

	if storeErr != nil {
		t.Error("Failed to initialize account in store:", storeErr)
	}

	pStore := callee.NewProfileStore(sstore.CommittedStore())
	scheduler, _ := NewScheduler(pStore) // No conflict db file.

	_, err := scheduler.New([]*libcommontype.StandardMessage{
		callContract00,
		callContract01,
		callContract10,
		callContract11},
	)

	if err != nil {
		t.Error("Failed to New schedule:", err)
	}

	// Initialize the state cells for the transactions.
	v0 := commutative.NewBoundedU256FromU64(1, 100)
	v0.SetValue(*uint256.NewInt(10))

	v1 := commutative.NewBoundedU256FromU64(10, 50)
	v1.SetValue(*uint256.NewInt(20))

	_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/"+hex.EncodeToString(contract0[:]), 0, 1, 0, v0, nil)
	_0.Property.JobSequenceID = 0

	_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/"+hex.EncodeToString(contract0[:]), 1, 1, 0, v1, nil)
	_1.Property.JobSequenceID = 1

	_2 := statecell.NewStateCell(0, "blcc://eth1.0/account/"+hex.EncodeToString(contract1[:])+"/ctrn/[:]", 0, 2, 1, commutative.NewPath(), nil)
	_2.Property.JobSequenceID = 0

	_3 := statecell.NewStateCell(1, "blcc://eth1.0/account/"+hex.EncodeToString(contract1[:])+"/ctrn/[:]", 0, 2, 1, commutative.NewPath(), nil)
	_3.Property.JobSequenceID = 1

	// scheduler.New()
	collisionSummary, _, _ := conflictor.NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})
	collisionSummary.Print()

	// Initialize the profile store with the conflict pairs.
	scheduler.ImportCollisions(collisionSummary)
	if err := scheduler.WriteToExeStore(); err != nil {
		t.Error("Failed to Write conflict info to :", err)
	}

	trans := pStore.ExecStore().Export(statecell.Sorter)
	if len(trans) == 0 {
		t.Fatal("Failed to export collision info from state store")
	}
	scheduler.Clear()

	if err := profile.DebugCommit(trans, scheduler.ProfileStore); err != nil {
		t.Error("Failed to commit collision info to state store:", err)
	}

	// Try to plan the execution of the transactions again, now
	// that the conflict information is available.
	execPlan, err := scheduler.New([]*libcommontype.StandardMessage{
		callContract00,
		callContract01,
		callContract10,
		callContract11},
	)

	if execPlan.TotalJobs() != 4 {
		t.Error("Failed to generate the correct number of jobs")
	}

	if len(execPlan.Generations) != 1 {
		t.Error("Failed to generate any schedule")
	}

	if err != nil {
		t.Error("Failed to New schedule:", err)
	}

}
