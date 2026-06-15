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
	"testing"

	commutative "github.com/arcology-network/common-lib/crdt/commutative"
	"github.com/arcology-network/common-lib/exp/slice"
	statecommon "github.com/arcology-network/state-engine/common"
	cache "github.com/arcology-network/state-engine/state/cache"
	statetestharness "github.com/arcology-network/state-engine/test/harness"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

func AliceAccount() string {
	b := make([]byte, 20)
	slice.Fill(b, 10)
	return hexutil.Encode(b)
}

func BobAccount() string {
	b := make([]byte, 20)
	slice.Fill(b, 11)
	return hexutil.Encode(b)
}

func CreateConflictParentPaths(acct []byte, selector [4]byte, writeCache *cache.ExecutionStateStore) (string, error) {
	account := ethcommon.BytesToAddress(acct)
	path := "blcc://eth1.0/account/" + hexutil.Encode(account[:])
	if typedv, _, _ := writeCache.Read(1, path, commutative.NewPath()); typedv == nil {
		if _, err := writeCache.Write(1, path, commutative.NewPath()); err != nil {
			return "", err
		}
	}

	builder := statecommon.PathBuilder{
		Sender:   ethcommon.BytesToAddress(acct),
		Address:  ethcommon.BytesToAddress(acct),
		Selector: selector,
	}

	path = builder.ProfileField("")
	_, err := writeCache.Write(1, path, commutative.NewPath())
	return path, err
}

func TestCalleeManager(t *testing.T) {
	alice := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	bob := []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	carol := []byte("cccccccccccccccccccccccccccccccccccccccc")
	david := []byte("dddddddddddddddddddddddddddddddddddddddd")

	sstore, err := statetestharness.CreateAccountInStore(
		ethcommon.BytesToAddress(alice),
		ethcommon.BytesToAddress(bob),
		ethcommon.BytesToAddress(carol),
		ethcommon.BytesToAddress(david))
	if err != nil {
		t.Error("Failed to create accounts in store", err)
	}

	writeCache := sstore

	if _, err := CreateConflictParentPaths(alice, [4]byte{1, 1, 1, 1}, writeCache); err != nil {
		t.Error(err)
	}

	if _, err := CreateConflictParentPaths(bob, [4]byte{2, 2, 2, 2}, writeCache); err != nil {
		t.Error(err)
	}

	if _, err := CreateConflictParentPaths(carol, [4]byte{3, 3, 3, 3}, writeCache); err != nil {
		t.Error(err)
	}

	if _, err := CreateConflictParentPaths(david, [4]byte{4, 4, 4, 4}, writeCache); err != nil {
		t.Error(err)
	}

	mgr := NewProfileStore(sstore, 1000000)
	_, _, err = DebugRegisterNewConflict(
		mgr,
		NewID(0, [20]byte(alice), [4]byte{1, 1, 1, 1}),
		NewID(0, [20]byte(bob), [4]byte{2, 2, 2, 2}),
	)
	if err != nil {
		t.Error("Failed to register new conflict", err)
	}

	_, _, err = DebugRegisterNewConflict(
		mgr,
		NewID(1, [20]byte(carol), [4]byte{3, 3, 3, 3}),
		NewID(1, [20]byte(david), [4]byte{4, 4, 4, 4}),
	)
	if err != nil {
		t.Error("Failed to register new conflict", err)
	}

	// if len(mgr.cache) != 4 {
	// 	t.Error("Failed to add contracts", len(mgr.cache))
	// }

	err = mgr.Commit()
	if err != nil {
		t.Error("Failed to commit profiles", err)
	}
}

func TestCalleeManagerCacheLimit(t *testing.T) {
	alice := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	bob := []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	david := []byte("dddddddddddddddddddddddddddddddddddddddd")

	sstore, err := statetestharness.CreateAccountInStore(
		ethcommon.BytesToAddress(alice),
		ethcommon.BytesToAddress(bob),
		// ethcommon.BytesToAddress(carol),
		ethcommon.BytesToAddress(david))
	if err != nil {
		t.Error("Failed to create accounts in store", err)
	}
	writeCache := sstore

	alicePath, err := CreateConflictParentPaths(
		alice,
		[4]byte{1, 1, 1, 1},
		writeCache,
	)
	if err != nil {
		t.Error(err)
	}

	bobPath, err := CreateConflictParentPaths(
		bob, [4]byte{2, 2, 2, 2}, writeCache)
	if err != nil {
		t.Error(err)
	}

	davidPath, err := CreateConflictParentPaths(david, [4]byte{3, 3, 3, 3}, writeCache)
	if err != nil {
		t.Error(err)
	}

	davidPath2, err := CreateConflictParentPaths(david, [4]byte{4, 4, 4, 4}, writeCache)
	if err != nil {
		t.Error(err)
	}

	// sstore := stateengine.NewStateStore(proxy.NewMemDBStoreProxy())
	mgr := NewProfileStore(sstore, 6000)

	// DebugRegisterNewConflict the conflict pairs to the scheduler

	if _, _, err := DebugRegisterNewConflict(
		mgr,
		NewID(0, [20]byte(alice), [4]byte{1, 1, 1, 1}),
		NewID(0, [20]byte(bob), [4]byte{2, 2, 2, 2}),
	); err != nil {
		t.Error("Failed to register new conflict", err)
	}

	if _, _, err := DebugRegisterNewConflict(
		mgr,
		NewID(1, [20]byte(david), [4]byte{3, 3, 3, 3}),
		NewID(1, [20]byte(david), [4]byte{4, 4, 4, 4}),
	); err != nil {
		t.Error("Failed to register new conflict", err)
	}

	if (mgr.cache.Length()) != 4 {
		t.Error("Failed to add contracts", (mgr.cache.Length()))
	}

	if err := mgr.Commit(); err != nil {
		t.Error("Failed to save profiles", err)
	}
	// mgr.Clear()

	if len(mgr.dirties) != 0 {
		t.Error("Failed to clear dirties", len(mgr.dirties))
	}

	keys, _ := sstore.KVs()
	// if len(keys) != 4 || len(v) != 4 {
	// 	t.Error("Failed to write back to storage", len(k), len(v))
	// }

	if !slice.Contains(keys, alicePath, func(a, b string) bool { return a == b }) {
		t.Log("Not Found alice profile in storage", keys, alicePath)
	}

	if !slice.Contains(keys, bobPath, func(a, b string) bool { return a == b }) {
		t.Log("Not Found bob profile in storage", keys, bobPath)
	}

	if !slice.Contains(keys, davidPath, func(a, b string) bool { return a == b }) {
		t.Log("Not Found david profile in storage", keys, davidPath)
	}

	if !slice.Contains(keys, davidPath2, func(a, b string) bool { return a == b }) {
		t.Log("Not Found david profile in storage", keys, davidPath2)
	}
}
