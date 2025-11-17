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
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	stgcommon "github.com/arcology-network/storage-committer/common"
)

//  "github.com/arcology-network/scheduler"

// The callee struct stores the information of a contract function that is called by the EOA initiated transactions.
// It is mainly used to optimize the execution of the transactions. A callee is uniquely identified by a
// combination of the contract's address and the function signature.
type Profile struct {
	UID      uint64   `json:"uid"`      // Unique identifier of the callee (derived from address + selector)
	Contract [20]byte `json:"contract"` // Contract address
	Selector [4]byte  `json:"selector"` // Function selector

	ParallelismDegree uint32   `json:"parallelismDegree"` // Execution parallelism, 1 for sequential, otherwise parallel.
	IsDeferrable      bool     `json:"deferredPayment"`   // Required prepayment amount for the deferrable functions
	ConflictWith      []uint64 `json:"conflictWith"`      // ConflictWith of the conflicting callee indices.

	// Stats for cache management
	Dirty      bool   `json:"Dirty"`      // Whether the conflicts in callee profile has been modified.
	UsageCount uint64 `json:"usageCount"` // Number of times this profile has been used.
}

func (this *Profile) SortConflicts() { slices.Sort(this.ConflictWith) } // Sort the callees by the indices in ascending order.

func (this *Profile) AddConflict(other *Profile) {
	this.ConflictWith = append(this.ConflictWith, other.UID)
	if len(this.ConflictWith) > stgcommon.MAX_NUM_CONFLICTS {
		this.ConflictWith = this.ConflictWith[:0]
		this.ParallelismDegree = 1 // Too many conflicts, mark as sequential
	}
	this.Dirty = true

	other.ConflictWith = append(other.ConflictWith, this.UID)
	if len(other.ConflictWith) > stgcommon.MAX_NUM_CONFLICTS {
		other.ConflictWith = this.ConflictWith[:0]
		other.ParallelismDegree = 1 // Too many conflicts, mark as sequential
	}
	other.Dirty = true
}

func (this *Profile) IfConflictExists(other *Profile) bool {
	lft := this.IsInConflict(other)
	rgt := other.IsInConflict(this)

	if lft != rgt {
		panic("Conflict list inconsistent" + this.PrintToString() + other.PrintToString())
	}
	return lft && rgt
}

// Determine whether this callee is in conflict with another callee.
func (this *Profile) IsInConflict(other *Profile) bool {
	return slices.IndexFunc(this.ConflictWith, func(i uint64) bool { return i == uint64(other.UID) }) != -1
}

func (this *Profile) NumConflicts() int {
	return len(this.ConflictWith)
}

func (c *Profile) PrintToString() string {
	var b strings.Builder

	// Contract and Selector as hex
	contract := hex.EncodeToString(c.Contract[:])
	selector := hex.EncodeToString(c.Selector[:])

	b.WriteString("Profile {\n")
	b.WriteString(fmt.Sprintf("  UID: %d\n", c.UID))
	b.WriteString(fmt.Sprintf("  Contract: 0x%s\n", contract))
	b.WriteString(fmt.Sprintf("  Selector: 0x%s\n", selector))
	b.WriteString(fmt.Sprintf("  ParallelismDegree: %d\n", c.ParallelismDegree))
	b.WriteString(fmt.Sprintf("  IsDeferrable: %t\n", c.IsDeferrable))
	b.WriteString(fmt.Sprintf("  ConflictWith: %v\n", c.ConflictWith))
	b.WriteString(fmt.Sprintf("  Dirty: %t\n", c.Dirty))
	b.WriteString("}")

	return b.String()
}
