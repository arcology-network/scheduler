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
	"testing"

	"github.com/arcology-network/common-lib/exp/slice"
	stateengine "github.com/arcology-network/state-engine"
	"github.com/arcology-network/state-engine/storage/proxy"
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

func TestCalleeManager(t *testing.T) {
	sstore := stateengine.NewStateStore(proxy.NewMemDBStoreProxy())
	mgr := NewProfileManager(sstore, 1000000)

	alice := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	bob := []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	carol := []byte("cccccccccccccccccccccccccccccccccccccccc")
	david := []byte("dddddddddddddddddddddddddddddddddddddddd")

	// RegisterNewConflict the conflict pairs to the scheduler
	mgr.RegisterNewConflict([20]byte(alice), [4]byte{1, 1, 1, 1}, [20]byte(bob), [4]byte{2, 2, 2, 2})
	mgr.RegisterNewConflict([20]byte(carol), [4]byte{3, 3, 3, 3}, [20]byte(david), [4]byte{4, 4, 4, 4})

	if len(mgr.profileCache) != 4 {
		t.Error("Failed to add contracts", len(mgr.profileCache))
	}

	mgr.Save()
	mgr.Clear()

	if len(mgr.profileCache) != 4 {
		t.Error("Failed to add contracts", len(mgr.profileCache))
	}
}

func TestCalleeManagerCacheLimit(t *testing.T) {
	sstore := stateengine.NewStateStore(proxy.NewMemDBStoreProxy())
	mgr := NewProfileManager(sstore, 2)

	alice := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	bob := []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	carol := []byte("cccccccccccccccccccccccccccccccccccccccc")
	david := []byte("dddddddddddddddddddddddddddddddddddddddd")

	// RegisterNewConflict the conflict pairs to the scheduler
	mgr.RegisterNewConflict([20]byte(alice), [4]byte{1, 1, 1, 1}, [20]byte(bob), [4]byte{2, 2, 2, 2})
	mgr.RegisterNewConflict([20]byte(carol), [4]byte{3, 3, 3, 3}, [20]byte(david), [4]byte{4, 4, 4, 4})

	if len(mgr.profileCache) != 4 {
		t.Error("Failed to add contracts", len(mgr.profileCache))
	}

	mgr.Save()
	mgr.Clear()

	if len(mgr.profileCache) != 0 {
		t.Error("Failed to add contracts", len(mgr.profileCache))
	}

	if k, v := sstore.StateCache.KVs(); len(k) != 4 || len(v) != 4 {
		t.Error("Failed to write back to storage", len(k), len(v))
	}
}
