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
package arbitrator

import (
	"sort"
	"strings"

	"github.com/arcology-network/common-lib/exp/slice"
	univalue "github.com/arcology-network/storage-committer/type/univalue"
)

// Wildcard is a struct that holds the wildcards and provides methods to filter and substitute them.

type Wildcard struct {
	WildcardTrans []*univalue.Univalue
}

func NewWildcard(initWildcardTrans ...*univalue.Univalue) *Wildcard {
	return &Wildcard{
		WildcardTrans: initWildcardTrans,
	}
}

// Filter filters the wildcards from the transitions.
// It will also add the wildcards to the WildcardTrans field.
func (this *Wildcard) Filter(trans []*univalue.Univalue) []*univalue.Univalue {
	for _, tran := range trans {
		if strings.Contains(*tran.GetPath(), "*") || strings.Contains(*tran.GetPath(), "[:]") {
			// if isWildcard, _ := tran.IsCommittedDeleted(); isWildcard {
			this.WildcardTrans = append(this.WildcardTrans, tran) // Apply the wildcard to the transition
			// trans[i] = nil
			// }
		}
	}
	// slice.Remove(&trans, nil) // Remove the nil elements from
	return trans
}

// Expand in the transitions with the corresponding values.
// This function assumes that all the elements in the trans array have the same path.
func (this *Wildcard) Expand(trans *[]*univalue.Univalue) []*univalue.Univalue {
	if len(this.WildcardTrans) == 0 {
		return []*univalue.Univalue{}
	}

	sort.SliceStable(this.WildcardTrans, func(i, j int) bool {
		return len(*this.WildcardTrans[i].GetPath()) < len(*this.WildcardTrans[j].GetPath()) || *this.WildcardTrans[i].GetPath() < *this.WildcardTrans[j].GetPath()
	})

	// Remove the duplicates from the WildcardTrans.
	allExpanded := []*univalue.Univalue{}
	k := (*trans)[0].GetPath() // All the tarns have the same path. So we can use the first one.
	for _, wildcard := range this.WildcardTrans {
		wildCardPath := *wildcard.GetPath()

		// Has the prefix of the wildcard path but not the same path to prevent duplication.
		if len(wildCardPath) != len(*k) && strings.HasPrefix(*k, wildCardPath) {
			// User may still want to write to the cleared path again. If this happens
			// Only if a transition with the same tx id isn't found, we will insert one.
			// Otherwise itself is enough for conflict detection.
			if idx, _ := slice.FindFirstIf(*trans, func(_ int, v *univalue.Univalue) bool { return v.GetTx() == wildcard.GetTx() }); idx == -1 {
				// On expand it to preexist entries. otherwise there will be independent transitions
				// for them.
				if !((*trans)[0].Preexist()) {
					continue
				}

				newUnival := new(univalue.Univalue)
				newUnival.Property = wildcard.Property.Clone()
				newUnival.SetExpanded(true)
				newUnival.IncrementWrites(1) // Increment the writes by 1, so that it will be treated as a write operation.
				newUnival.SetPath(k)
				newUnival.SetValue(nil) // Set the path to the actual path.
				*trans = append(*trans, newUnival)
				allExpanded = append(allExpanded, newUnival) // Add the wildcard to the substituted ones.
			}
		}
	}
	return allExpanded
}
