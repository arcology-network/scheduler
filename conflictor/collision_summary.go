/*
 *   Copyright (c) 2026 Arcology Network

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

package conflictor

import (
	"encoding/json"
	"fmt"
	"sort"

	statecell "github.com/arcology-network/common-lib/crdt/statecell"
	mapi "github.com/arcology-network/common-lib/exp/map"
	"github.com/arcology-network/common-lib/exp/slice"
	"github.com/arcology-network/scheduler/workload"
)

// CollisionSummary is a collection of Collision pointers.
// Scheduler uses it to make execution plans based on the detected conflicts.
type CollisionSummary struct {
	Collisions       []*Collision      `json:"conflicts"`       // The list of detected conflicts.
	RevertTxLookup   map[uint64]error  `json:"revertTxLookup"`  // The transaction IDs that need to be reverted.
	RevertSeqLookup  map[uint64]uint64 `json:"revertSeqLookup"` // The job sequence IDs that contain the transactions to revert.
	CollisionFreeTxs []uint64          `json:"cleared"`         // The IDs of the transactions that are conflict-free.
}

func CreateNewCollisionSummary() *CollisionSummary {
	return &CollisionSummary{
		Collisions:       make([]*Collision, 0),
		RevertTxLookup:   make(map[uint64]error),
		RevertSeqLookup:  make(map[uint64]uint64),
		CollisionFreeTxs: make([]uint64, 0),
	}
}

func NewCollisionSummary(trans []*statecell.StateCell, conflicts []*Collision) *CollisionSummary {
	revertTxLookup := make(map[uint64]error) // Unique transaction IDs to revert.
	seqLookup := make(map[uint64]uint64)     // Unique job sequence IDs that contain the transactions to revert.
	for _, conflict := range conflicts {
		IDs := conflict.GetRevertTxIDs()

		// The IDs of all the conflicting transactions that share the same state cell and
		// affected by the conflict. They all need to be reverted.
		slice.Foreach(IDs, func(_ int, txId *uint64) {
			revertTxLookup[*txId] = conflict.Reason

			// Map to the job sequence IDs as well.
			for _, peer := range conflict.Peers {
				seqLookup[peer.JobSequenceID]++
			}
		})
	}

	// Get unique transaction IDs.
	uniquesTx := make(map[uint64]uint64)
	for _, tran := range trans {
		uniquesTx[tran.GetTx()]++
	}

	// Sort the transaction IDs and sequence IDs.
	txs := mapi.Keys(uniquesTx)
	sort.SliceStable(txs, func(i, j int) bool { return txs[i] < txs[j] }) // Sort the transaction IDs.

	return &CollisionSummary{
		Collisions:       conflicts,
		RevertTxLookup:   revertTxLookup,
		RevertSeqLookup:  seqLookup,
		CollisionFreeTxs: slice.Exclude(slice.Clone(mapi.Keys(uniquesTx)), mapi.Keys(revertTxLookup)),
	}
}

func (this *CollisionSummary) IsEmpty() bool {
	return len(this.Collisions) == 0
}

// Mark all job for rollback and return the number of sequences marked.
func (this *CollisionSummary) MarkRollbackJobs(gen *workload.Generation) {
	for _, collision := range this.Collisions {
		// Mark the self job for rollback.
		if job, found := gen.TxToJobLookup[collision.Self.GetTx()]; found {
			job.Result.Err = collision.Reason
		}
	}
}

// Get the transaction IDs that need to be reverted.
// func (this *CollisionSummary) GetRevertTxs() []uint64 {
// 	reverts := mapi.Keys(this.RevertTxLookup)
// 	sort.SliceStable(reverts, func(i, j int) bool { return reverts[i] < reverts[j] })
// 	return reverts
// }

// // Get conflicted free transaction IDs.
// func (this *CollisionSummary) GetClearedTxs() []uint64 {
// 	clearTxs := make([]uint64, 0, len(this.Collisions)-len(this.RevertTxLookup))
// 	for _, conflict := range this.Collisions {
// 		if _, ok := this.RevertTxLookup[conflict.Self.GetTx()]; !ok {
// 			clearTxs = append(clearTxs, conflict.Self.GetTx())
// 		}
// 	}
// 	return clearTxs
// }

func (this CollisionSummary) Print() {
	for _, v := range this.Collisions {
		v.Print()
		fmt.Println()
	}
}

func (this *CollisionSummary) MarshalJSON() ([]byte, error) {
	type conflictsAlias struct {
		Collisions       []*Collision      `json:"conflicts"`
		RevertTxLookup   map[uint64]string `json:"revertTxLookup"`
		RevertSeqLookup  map[uint64]uint64 `json:"revertSeqLookup"`
		CollisionFreeTxs []uint64          `json:"cleared"`
	}

	var txLookup map[uint64]string
	if len(this.RevertTxLookup) > 0 {
		txLookup = make(map[uint64]string, len(this.RevertTxLookup))
		for txID, reason := range this.RevertTxLookup {
			if reason != nil {
				txLookup[txID] = reason.Error()
			} else {
				txLookup[txID] = ""
			}
		}
	}

	alias := conflictsAlias{
		Collisions:       this.Collisions,
		RevertTxLookup:   txLookup,
		RevertSeqLookup:  this.RevertSeqLookup,
		CollisionFreeTxs: this.CollisionFreeTxs,
	}

	return json.Marshal(&alias)
}
