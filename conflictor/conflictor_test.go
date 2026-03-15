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

package conflictor

import (
	"testing"

	"github.com/arcology-network/common-lib/crdt/commutative"
	noncommutative "github.com/arcology-network/common-lib/crdt/noncommutative"
	statecell "github.com/arcology-network/common-lib/crdt/statecell"
	"github.com/holiman/uint256"
)

func TestArbiOnCommutatives(t *testing.T) { // Delta writes only, should be no conflict
	t.Run("init / init", func(t *testing.T) {
		v0 := commutative.NewBoundedU256FromU64(1, 100)
		v0.SetValue(*uint256.NewInt(10))
		v1 := commutative.NewBoundedU256FromU64(1, 100)
		v1.SetValue(*uint256.NewInt(20))

		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v0, nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v1, nil)

		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})
		if len(collisionSummary.Collisions) != 0 {
			t.Error("Error: There should be NO conflict")
		}
	})

	t.Run("init / init", func(t *testing.T) {
		v0 := commutative.NewBoundedU256FromU64(1, 100)
		v0.SetValue(*uint256.NewInt(10))
		v1 := commutative.NewBoundedU256FromU64(1, 100)
		v1.SetValue(*uint256.NewInt(20))

		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v0, nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v1, nil)

		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})
		if len(collisionSummary.Collisions) != 0 {
			t.Error("Error: There should be NO conflict")
		}
	})

	t.Run("Nil init / Nil init", func(t *testing.T) {
		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 0, 1, 0, nil, nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 0, 1, 0, nil, nil)
		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})

		// Nil init never exists, so it should be treated as a conflict
		if len(collisionSummary.Collisions) != 1 {
			t.Error("Error: There should be ONE conflict", len(collisionSummary.Collisions))
		}
	})

	t.Run("Nil init / Delete", func(t *testing.T) {
		v0 := commutative.NewBoundedU256FromU64(1, 100)
		v0.SetValue(*uint256.NewInt(10))

		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v0, nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 0, 1, 0, nil, nil)
		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})
		if len(collisionSummary.Collisions) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("init + write/ init + write", func(t *testing.T) {
		v0 := commutative.NewBoundedU256FromU64(1, 100)
		v0.SetValue(*uint256.NewInt(10))
		v1 := commutative.NewBoundedU256FromU64(1, 100)
		v1.SetValue(*uint256.NewInt(20))

		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 0, 1, 1, v0, nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 0, 1, 1, v1, nil)

		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})

		if len(collisionSummary.Collisions) != 0 {
			t.Error("Error: There should be NO conflict")
		}
	})

	t.Run("deletion / deletion", func(t *testing.T) {
		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 0, 1, 0, nil, nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 0, 1, 0, nil, nil)

		_0.DebugSetIsDeleted(true)
		_1.DebugSetIsDeleted(true)

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})

		if len(collisionSummary.Collisions) != 0 {
			t.Error("Error: There should be NO conflict")
		}
	})

	t.Run("Cumulative write / Cumulative write", func(t *testing.T) {
		v0 := commutative.NewBoundedU256FromU64(1, 100)
		v0.SetValue(*uint256.NewInt(10))
		v1 := commutative.NewBoundedU256FromU64(1, 100)
		v1.SetValue(*uint256.NewInt(20))

		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v0, nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v1, nil)

		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})

		if len(collisionSummary.Collisions) != 0 {
			t.Error("Error: There should be NO conflict")
		}
	})

	t.Run("Read / Cumulative write", func(t *testing.T) {
		v0 := commutative.NewBoundedU256FromU64(1, 100)
		v0.SetValue(*uint256.NewInt(10))
		v1 := commutative.NewBoundedU256FromU64(1, 100)
		v1.SetValue(*uint256.NewInt(20))

		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 1, 1, 1, v0, nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v1, nil)

		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})

		if len(collisionSummary.Collisions) != 1 {
			t.Error("Error: There should be ONE conflict", len(collisionSummary.Collisions))
		}
	})

	t.Run("init + write / init + write", func(t *testing.T) {
		v0 := commutative.NewBoundedU256FromU64(1, 100)
		v0.SetValue(*uint256.NewInt(10))
		v1 := commutative.NewBoundedU256FromU64(1, 100)
		v1.SetValue(*uint256.NewInt(20))

		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 0, 1, 1, v0, nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v1, nil)

		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})

		if len(collisionSummary.Collisions) != 0 {
			t.Error("Error: There should be NO conflict")
		}
	})

	t.Run("Read+write / Init with nil", func(t *testing.T) {
		v0 := commutative.NewBoundedU256FromU64(1, 100)
		v0.SetValue(*uint256.NewInt(10))
		v1 := commutative.NewBoundedU256FromU64(1, 100)
		v1.SetValue(*uint256.NewInt(20))

		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 1, 1, 1, v0, nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 0, 1, 0, nil, nil)

		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})

		if len(collisionSummary.Collisions) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("Read / delta write", func(t *testing.T) {
		v0 := commutative.NewBoundedU256FromU64(1, 100)
		v0.SetValue(*uint256.NewInt(10))
		v1 := commutative.NewBoundedU256FromU64(1, 100)
		v1.SetValue(*uint256.NewInt(20))

		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 1, 0, 0, v0, nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 0, 0, 1, v1, nil)

		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})

		if len(collisionSummary.Collisions) != 1 {
			t.Error("Error: There should be ONE conflict, actual:", len(collisionSummary.Collisions))
		}
	})
}

func TestArbiCreateTwoAccountsNoConflict(t *testing.T) {
	t.Run("one entry", func(t *testing.T) { // Reads only, should be no conflict
		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 1, 0, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_0.JobSequenceID = 0

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0})

		if len(collisionSummary.Collisions) != 0 {
			t.Error("Error: There should be NO conflict")
		}
	})
	t.Run("noncommutative read & read", func(t *testing.T) { // Reads only, should be no conflict
		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 1, 0, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 1, 0, 0, noncommutative.NewBytes([]byte{2, 3}), nil)

		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})

		if len(collisionSummary.Collisions) != 0 {
			t.Error("Error: There should be NO conflict")
		}
	})

	t.Run("delta write & delta write", func(t *testing.T) { // Delta writes only, should be no conflict
		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 0, 0, 1, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 0, 0, 2, noncommutative.NewBytes([]byte{2, 3}), nil)

		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})

		if len(collisionSummary.Collisions) != 0 {
			t.Error("Error: There should be NO conflict")
		}
	})

	t.Run("write & write", func(t *testing.T) { // Read delta write, should be 1 conflict
		// Write only, should be 1 conflict
		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 0, 2, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 0, 2, 0, noncommutative.NewBytes([]byte{2, 3}), nil)
		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})

		if len(collisionSummary.Collisions) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("Read & Delta-write", func(t *testing.T) { // Read delta write, should be 1 conflict
		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 2, 0, 2, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 2, 0, 0, noncommutative.NewBytes([]byte{2, 3}), nil)
		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})
		if len(collisionSummary.Collisions) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("Read & Delta-write", func(t *testing.T) { // Read delta write, should be 1 conflict
		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 2, 0, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 2, 0, 2, noncommutative.NewBytes([]byte{2, 3}), nil)
		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})
		if len(collisionSummary.Collisions) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("Read & Delta-write", func(t *testing.T) { // Read delta write, should be 1 conflict
		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 0, 0, 2, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 2, 0, 2, noncommutative.NewBytes([]byte{2, 3}), nil)
		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})
		if len(collisionSummary.Collisions) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("read-Write & read-write write", func(t *testing.T) { // Read delta write, should be 1 conflict
		// Read write, should be 1 conflict
		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 2, 2, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 2, 2, 0, noncommutative.NewBytes([]byte{2, 3}), nil)
		_0.JobSequenceID = 0
		_1.JobSequenceID = 1
		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})

		if len(collisionSummary.Collisions) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("read-Write & read-write write", func(t *testing.T) { // Read delta write, should be 1 conflict
		// Read write, should be 1 conflict
		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 2, 0, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 2, 2, 0, noncommutative.NewBytes([]byte{2, 3}), nil)
		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})

		if len(collisionSummary.Collisions) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("read-Write & read-write write", func(t *testing.T) { // Read delta write, should be 1 conflict
		// Read write, should be 1 conflict
		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 2, 2, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 2, 0, 0, noncommutative.NewBytes([]byte{2, 3}), nil)
		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})

		if len(collisionSummary.Collisions) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("read-Write & read-write write", func(t *testing.T) { // write / delta wirte, should be 1 conflict
		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 2, 2, 1, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 2, 2, 0, noncommutative.NewBytes([]byte{2, 3}), nil)
		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})

		if len(collisionSummary.Collisions) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("read-Write & read-write write", func(t *testing.T) { // write / delta wirte, should be 1 conflict
		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/0x0000000", 2, 2, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := statecell.NewStateCell(1, "blcc://eth1.0/account/0x0000000", 2, 2, 1, noncommutative.NewBytes([]byte{2, 3}), nil)
		_0.JobSequenceID = 0
		_1.JobSequenceID = 1

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _1})

		if len(collisionSummary.Collisions) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("Clear Committed / New Byte write", func(t *testing.T) {
		// Read an element
		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/ctrn/0x0000000", 2, 2, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_0.Property.DebugSetPreexist(false)

		// Clear all committed entries under the path
		_2 := statecell.NewStateCell(2, "blcc://eth1.0/account/ctrn/[:]", 0, 2, 1, commutative.NewPath(), nil)
		_2.Value().(*commutative.Path).DeltaSet.Removed().CommittedDeleted = (true)

		_0.JobSequenceID = 0
		_2.JobSequenceID = 2

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _2})
		if len(collisionSummary.Collisions) != 0 {
			t.Error("Error: ", len(collisionSummary.Collisions))
		}
	})

	t.Run("Clear All / Byte write", func(t *testing.T) {
		// Read + write an existing element
		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/ctrn/0x0000000", 2, 2, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_0.Property.DebugSetPreexist(true)

		// Clear all  entries (committed + Added) under the path
		_2 := statecell.NewStateCell(2, "blcc://eth1.0/account/ctrn/*", 0, 2, 1, commutative.NewPath(), nil)
		_2.Value().(*commutative.Path).DeltaSet.Removed().CommittedDeleted = (true)
		_2.Value().(*commutative.Path).DeltaSet.Removed().StagedAddedDeleted = (true)

		_0.JobSequenceID = 0
		_2.JobSequenceID = 2

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _2})

		if len(collisionSummary.Collisions) != 1 {
			t.Error("Error: ", len(collisionSummary.Collisions))
		}
	})

	t.Run("Clear All / existing Byte write", func(t *testing.T) {
		// Read an element
		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/ctrn/0x0000000", 2, 2, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_0.Property.DebugSetPreexist(false)

		// Clear all committed entries under the path
		_2 := statecell.NewStateCell(2, "blcc://eth1.0/account/ctrn/*", 0, 2, 1, commutative.NewPath(), nil)
		_2.Value().(*commutative.Path).DeltaSet.Removed().CommittedDeleted = (true)
		_2.Value().(*commutative.Path).DeltaSet.Removed().StagedAddedDeleted = (true)

		_0.JobSequenceID = 0
		_2.JobSequenceID = 2

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _2})

		if len(collisionSummary.Collisions) != 1 {
			t.Error("Error: ", len(collisionSummary.Collisions))
		}
	})

	t.Run("Clear Committed / New Byte write", func(t *testing.T) {
		// Read an element
		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/ctrn/0x0000000", 2, 2, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_0.Property.DebugSetPreexist(false)
		// This one isn't pre-exists

		// Clear all committed entries under the path
		_2 := statecell.NewStateCell(2, "blcc://eth1.0/account/ctrn/[:]", 0, 2, 1, commutative.NewPath(), nil)
		_2.Value().(*commutative.Path).DeltaSet.Removed().CommittedDeleted = (true)

		_0.JobSequenceID = 0
		_2.JobSequenceID = 2

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _2})

		if len(collisionSummary.Collisions) != 0 {
			t.Error("Error: ", len(collisionSummary.Collisions))
		}
	})

	t.Run("Preexist Byte write /  Clear Committed", func(t *testing.T) {
		// Read an element
		_0 := statecell.NewStateCell(0, "blcc://eth1.0/account/ctrn/0x0000000", 2, 2, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_0.Property.DebugSetPreexist(true)
		// This one is pre-exists

		// Clear all committed entries under the path
		_2 := statecell.NewStateCell(2, "blcc://eth1.0/account/ctrn/[:]", 0, 2, 1, commutative.NewPath(), nil)
		_2.Value().(*commutative.Path).DeltaSet.Removed().CommittedDeleted = (true)

		_0.JobSequenceID = 0
		_2.JobSequenceID = 2

		collisionSummary, _, _ := NewConflictor().DebugInsertAndDetect([]*statecell.StateCell{_0, _2})
		if len(collisionSummary.Collisions) != 1 {
			t.Error("Error: ", len(collisionSummary.Collisions))
		}
	})

}
