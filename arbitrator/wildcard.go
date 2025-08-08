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
	for i, tran := range trans {
		if isWildcard, _ := tran.IsWildcard(); isWildcard {
			this.WildcardTrans = append(this.WildcardTrans, tran) // Apply the wildcard to the transition
			trans[i] = nil
		}
	}
	slice.Remove(&trans, nil) // Remove the nil elements from
	return trans
}

// func (this *Wildcard) Unique() *Wildcard {
// 	sort.SliceStable(this.WildcardTrans, func(i, j int) bool {
// 		return len(*this.WildcardTrans[i].GetPath()) < len(*this.WildcardTrans[j].GetPath()) || *this.WildcardTrans[i].GetPath() < *this.WildcardTrans[j].GetPath()
// 	})

// 	slice.UniqueIf(this.WildcardTrans, func(lhv, rhv *Univalue) bool {
// 		lstr := strings.TrimSuffix(*lhv.GetPath(), "*")
// 		rstr := strings.TrimSuffix(*rhv.GetPath(), "*")
// 		return lstr == rstr[:len(lstr)] // The left hand side is always shorter than the right hand side.
// 	})
// 	return this
// }

// SubstituteWildcards replaces WildcardTrans in the transitions with the corresponding values.
// This function assumes that all the elements in the trans array have the same path.
func (this *Wildcard) Substitute(trans *[]*univalue.Univalue) []*univalue.Univalue {
	if len(this.WildcardTrans) == 0 {
		return []*univalue.Univalue{}
	}

	sort.SliceStable(this.WildcardTrans, func(i, j int) bool {
		return len(*this.WildcardTrans[i].GetPath()) < len(*this.WildcardTrans[j].GetPath()) || *this.WildcardTrans[i].GetPath() < *this.WildcardTrans[j].GetPath()
	})

	// Remove the duplicates from the WildcardTrans.
	allSubstituted := []*univalue.Univalue{}
	k := (*trans)[0].GetPath() // All the tarns have the same path. So we can use the first one.
	for _, wildcard := range this.WildcardTrans {
		wildCardPath := *wildcard.GetPath()

		// Has the prefix of the wildcard path but not the same path to prevent duplication.
		if len(wildCardPath) != len(*k) && strings.HasPrefix(*k, wildCardPath) {

			// All the transitions in the write cache will be properly marked as the wildcard deletion happened
			// But user may still want to write to the same value again. This will result in there in having
			// multiple operation to the same path. For example a wildcard delete and then a write.
			idx, _ := slice.FindFirstIf(*trans, func(_ int, v *univalue.Univalue) bool {
				return v.GetTx() == wildcard.GetTx()
			})

			// Only if a transition with the same tx id as the wildcard is not found, we will insert one.
			if idx == -1 {
				newUnival := new(univalue.Univalue)
				newUnival.Property = wildcard.Property.Clone()
				newUnival.IncrementDeltaWrites(1) // Increment the delta writes to 1, so that it will be treated as a write operation.
				newUnival.SetPath(k)
				newUnival.SetValue(nil) // Set the path to the actual path.
				*trans = append(*trans, newUnival)
				allSubstituted = append(allSubstituted, newUnival) // Add the wildcard to the substituted ones.
			}
		}
	}
	return allSubstituted
}
