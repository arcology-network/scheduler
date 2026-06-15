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
	"crypto/sha256"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/arcology-network/common-lib/exp/slice"
	libtypes "github.com/arcology-network/common-lib/types"
	profile "github.com/arcology-network/scheduler/callee"
	execstatestore "github.com/arcology-network/state-engine/state/cache"
	proxy "github.com/arcology-network/state-engine/storage/proxy"
	ethcommon "github.com/ethereum/go-ethereum/common"

	// "github.com/ethereum/go-ethereum/common/hexutil"
	ethcore "github.com/ethereum/go-ethereum/core"
)

func BenchmarkSchedulerWithConflictInfo(t *testing.B) {
	sstore := execstatestore.NewDefaultExecutionStateStore(proxy.NewMemDBStoreProxy())
	manager := profile.NewProfileStore(sstore, 1000000)
	scheduler, _ := NewScheduler(manager) // No conflict db file.

	alice := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	aaddr := ethcommon.BytesToAddress(alice)

	callAlice := &libtypes.StandardMessage{
		ID:     0,
		Native: &ethcore.Message{To: &aaddr, Data: []byte{1, 1, 1, 1}},
	}

	msgs := make([]*libtypes.StandardMessage, 10)
	for i := range msgs {
		h := sha256.Sum256([]byte(strconv.Itoa(i)))
		addr := ethcommon.BytesToAddress(h[:])
		msgs[i] = &libtypes.StandardMessage{
			ID:     uint64(i),
			Native: &ethcore.Message{To: &addr, Data: addr[:4]},
		}
	}
	msgs = slice.Join(msgs, slice.New(100000, callAlice))

	t0 := time.Now()
	scheduler.New(msgs)
	fmt.Println("Scheduler", len(msgs), time.Since(t0))
}
