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

package arbitrator

import (
	"testing"
	"time"

	"github.com/arcology-network/storage-committer/type/commutative"
	noncommutative "github.com/arcology-network/storage-committer/type/noncommutative"
	univalue "github.com/arcology-network/storage-committer/type/univalue"
	btree "github.com/google/btree"
	"github.com/holiman/uint256"
)

func TestArrayMap(t *testing.T) {
	m := make(map[uint64]string)
	t0 := time.Now()
	for i := 0; i < 1000000; i++ {
		m[uint64(i)] = ""
	}
	t1 := time.Now()
	t.Log("Array map time:", t1.Sub(t0))

	btree.New(2)
	bMap := make(map[uint64]*btree.BTree)
	t0 = time.Now()
	for i := 0; i < 1000000; i++ {
		bMap[uint64(i)] = btree.New(3)
	}
	t1 = time.Now()
	t.Log("Btree time:", t1.Sub(t0))
}

func TestArbiOnCommutatives(t *testing.T) { // Delta writes only, should be no conflict
	t.Run("init / init", func(t *testing.T) {
		v0 := commutative.NewBoundedU256FromU64(1, 100)
		v0.SetValue(*uint256.NewInt(10))
		v1 := commutative.NewBoundedU256FromU64(1, 100)
		v1.SetValue(*uint256.NewInt(20))

		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v0, nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v1, nil)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})

		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 0 {
			t.Error("Error: There should be NO conflict")
		}
	})

	t.Run("init / init", func(t *testing.T) {
		v0 := commutative.NewBoundedU256FromU64(1, 100)
		v0.SetValue(*uint256.NewInt(10))
		v1 := commutative.NewBoundedU256FromU64(1, 100)
		v1.SetValue(*uint256.NewInt(20))

		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v0, nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v1, nil)

		// arib := new(Arbitrator)
		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 0 {
			t.Error("Error: There should be NO conflict")
		}
	})

	t.Run("init + write / init + write", func(t *testing.T) {
		v0 := commutative.NewBoundedU256FromU64(1, 100)
		v0.SetValue(*uint256.NewInt(10))
		v1 := commutative.NewBoundedU256FromU64(1, 100)
		v1.SetValue(*uint256.NewInt(20))

		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 0, 1, 1, v0, nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v1, nil)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 0 {
			t.Error("Error: There should be NO conflict")
		}
	})

	t.Run("init path/ init path", func(t *testing.T) {
		v0 := commutative.NewPath()
		v1 := commutative.NewPath()

		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v0, nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v1, nil)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 0 {
			t.Error("Error: There should be NO conflict")
		}
	})

	t.Run("Nil init / Nil init", func(t *testing.T) {
		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 0, 1, 0, nil, nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 0, 1, 0, nil, nil)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		// Nil init never exists, so it should be treated as a conflict
		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("Nil init / Delete", func(t *testing.T) {
		v0 := commutative.NewBoundedU256FromU64(1, 100)
		v0.SetValue(*uint256.NewInt(10))

		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v0, nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 0, 1, 0, nil, nil)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("init + write/ init + write", func(t *testing.T) {
		v0 := commutative.NewBoundedU256FromU64(1, 100)
		v0.SetValue(*uint256.NewInt(10))
		v1 := commutative.NewBoundedU256FromU64(1, 100)
		v1.SetValue(*uint256.NewInt(20))

		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 0, 1, 1, v0, nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 0, 1, 1, v1, nil)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 0 {
			t.Error("Error: There should be NO conflict")
		}
	})

	t.Run("deletion / deletion", func(t *testing.T) {
		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 0, 1, 0, nil, nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 0, 1, 0, nil, nil)

		_0.SetIsDeleted(true)
		_1.SetIsDeleted(true)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 0 {
			t.Error("Error: There should be NO conflict")
		}
	})

	t.Run("Cumulative write / Cumulative write", func(t *testing.T) {
		v0 := commutative.NewBoundedU256FromU64(1, 100)
		v0.SetValue(*uint256.NewInt(10))
		v1 := commutative.NewBoundedU256FromU64(1, 100)
		v1.SetValue(*uint256.NewInt(20))

		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v0, nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v1, nil)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 0 {
			t.Error("Error: There should be NO conflict")
		}
	})

	t.Run("Read / Cumulative write", func(t *testing.T) {
		v0 := commutative.NewBoundedU256FromU64(1, 100)
		v0.SetValue(*uint256.NewInt(10))
		v1 := commutative.NewBoundedU256FromU64(1, 100)
		v1.SetValue(*uint256.NewInt(20))

		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 1, 1, 1, v0, nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 0, 1, 0, v1, nil)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("Read+write / Init with nil", func(t *testing.T) {
		v0 := commutative.NewBoundedU256FromU64(1, 100)
		v0.SetValue(*uint256.NewInt(10))
		v1 := commutative.NewBoundedU256FromU64(1, 100)
		v1.SetValue(*uint256.NewInt(20))

		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 1, 1, 1, v0, nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 0, 1, 0, nil, nil)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("Read / delta write", func(t *testing.T) {
		v0 := commutative.NewBoundedU256FromU64(1, 100)
		v0.SetValue(*uint256.NewInt(10))
		v1 := commutative.NewBoundedU256FromU64(1, 100)
		v1.SetValue(*uint256.NewInt(20))

		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 1, 0, 0, v0, nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 0, 0, 1, v1, nil)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 1 {
			t.Error("Error: There should be ONE conflict, actual:", len(conflictdict))
		}
	})
}

func TestArbiCreateTwoAccountsNoConflict(t *testing.T) {
	t.Run("one entry", func(t *testing.T) { // Reads only, should be no conflict
		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 1, 0, 0, noncommutative.NewBytes([]byte{1, 2}), nil)

		arib := NewArbitrator()
		arib.Insert([]uint64{0}, []*univalue.Univalue{_0})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 0 {
			t.Error("Error: There should be NO conflict")
		}
	})
	t.Run("noncommutative read & read", func(t *testing.T) { // Reads only, should be no conflict
		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 1, 0, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 1, 0, 0, noncommutative.NewBytes([]byte{2, 3}), nil)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 0 {
			t.Error("Error: There should be NO conflict")
		}
	})

	t.Run("delta write & delta write", func(t *testing.T) { // Delta writes only, should be no conflict
		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 0, 0, 1, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 0, 0, 2, noncommutative.NewBytes([]byte{2, 3}), nil)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 0 {
			t.Error("Error: There should be NO conflict")
		}
	})

	t.Run("write & write", func(t *testing.T) { // Read delta write, should be 1 conflict
		// Write only, should be 1 conflict
		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 0, 2, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 0, 2, 0, noncommutative.NewBytes([]byte{2, 3}), nil)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("Read & Delta-write", func(t *testing.T) { // Read delta write, should be 1 conflict
		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 2, 0, 2, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 2, 0, 0, noncommutative.NewBytes([]byte{2, 3}), nil)
		_0.Setsequence(0)
		_1.Setsequence(1)

		// sorted := univalue.Univalues([]*univalue.Univalue{_0, _1}).Sort()

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("Read & Delta-write", func(t *testing.T) { // Read delta write, should be 1 conflict
		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 2, 0, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 2, 0, 2, noncommutative.NewBytes([]byte{2, 3}), nil)
		_0.Setsequence(0)
		_1.Setsequence(1)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("Read & Delta-write", func(t *testing.T) { // Read delta write, should be 1 conflict
		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 0, 0, 2, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 2, 0, 2, noncommutative.NewBytes([]byte{2, 3}), nil)
		_0.Setsequence(0)
		_1.Setsequence(1)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("read-Write & read-write write", func(t *testing.T) { // Read delta write, should be 1 conflict
		// Read write, should be 1 conflict
		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 2, 2, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 2, 2, 0, noncommutative.NewBytes([]byte{2, 3}), nil)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("read-Write & read-write write", func(t *testing.T) { // Read delta write, should be 1 conflict
		// Read write, should be 1 conflict
		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 2, 0, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 2, 2, 0, noncommutative.NewBytes([]byte{2, 3}), nil)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("read-Write & read-write write", func(t *testing.T) { // Read delta write, should be 1 conflict
		// Read write, should be 1 conflict
		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 2, 2, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 2, 0, 0, noncommutative.NewBytes([]byte{2, 3}), nil)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("read-Write & read-write write", func(t *testing.T) { // write / delta wirte, should be 1 conflict
		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 2, 2, 1, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 2, 2, 0, noncommutative.NewBytes([]byte{2, 3}), nil)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})

	t.Run("read-Write & read-write write", func(t *testing.T) { // write / delta wirte, should be 1 conflict
		_0 := univalue.NewUnivalue(0, "blcc://eth1.0/account/0x0000000", 2, 2, 0, noncommutative.NewBytes([]byte{1, 2}), nil)
		_1 := univalue.NewUnivalue(1, "blcc://eth1.0/account/0x0000000", 2, 2, 1, noncommutative.NewBytes([]byte{2, 3}), nil)

		arib := NewArbitrator()
		arib.Insert([]uint64{0, 1}, []*univalue.Univalue{_0, _1})
		conflicts := arib.Detect()

		conflictdict, _, _ := Conflicts(conflicts).ToDict()
		if len(conflictdict) != 1 {
			t.Error("Error: There should be ONE conflict")
		}
	})
}
