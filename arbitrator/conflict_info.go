/*
 *   Copyright (c) 2023 Arcology Network

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
	"fmt"

	mapi "github.com/arcology-network/common-lib/exp/map"
	univalue "github.com/arcology-network/storage-committer/type/univalue"
)

type Conflict struct {
	key           string
	self          uint64
	groupID       []uint64
	txIDs         []uint64
	selfTran      *univalue.Univalue
	conflictTrans []*univalue.Univalue
	Err           error
}

func (this Conflict) ToPairs() [][2]uint64 {
	pairs := make([][2]uint64, 0, len(this.txIDs)*(len(this.txIDs)+1)/2-len(this.txIDs))
	for i := 0; i < len(this.txIDs); i++ {
		pairs = append(pairs, [2]uint64{this.self, this.txIDs[i]})
	}
	return pairs
}

func (this *Conflict) Print() {
	this.selfTran.Print()
	fmt.Println(" ----- conflict with ----- ")
	univalue.Univalues(this.conflictTrans).Print()
	fmt.Println("Season: ", this.Err)
}

type Conflicts []*Conflict

func (this Conflicts) ToDict() (map[uint64]uint64, map[uint64]uint64, [][2]uint64) {
	txDict := make(map[uint64]uint64)
	groupIDdict := make(map[uint64]uint64)
	for _, v := range this {
		for i := 0; i < len(v.txIDs); i++ {
			txDict[v.txIDs[i]] += 1
			groupIDdict[v.groupID[i]] += 1
		}
	}

	return txDict, groupIDdict, this.ToPairs()
}

func (this Conflicts) Keys() []string {
	keys := make([]string, 0, len(this))
	for _, v := range this {
		keys = append(keys, v.key)
	}
	return keys
}

func (this Conflicts) ToPairs() [][2]uint64 {
	dict := make(map[[2]uint64]int)
	for _, v := range this {
		pairs := v.ToPairs()
		for _, pair := range pairs {
			dict[pair]++
		}
	}
	return mapi.Keys(dict)
}

func (this Conflicts) Print() {
	for _, v := range this {
		v.Print()
		fmt.Println()
	}
}
