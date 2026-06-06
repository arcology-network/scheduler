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

package conflictor

import (
	"sort"

	crdtcommon "github.com/arcology-network/common-lib/crdt/common"
	statecell "github.com/arcology-network/common-lib/crdt/statecell"
	"github.com/arcology-network/common-lib/exp/slice"
	slices "github.com/arcology-network/common-lib/exp/slice"
	schedulercommon "github.com/arcology-network/scheduler/common"
)

// Accumualator is dedicatd to cumulative numeric variables. It check if the value is out of limits defined by
// the type.
//
// It sorts the transitions by the delta sign and type so the negative deltas are in the front of the
// delta sequence to make sure it has sufficient initial value for the negative deltas.
// The underflow is always checked first before the overflow.

type Accumulator struct{}

// Categorize the transitions into negative and positive deltas.
func (*Accumulator) partitionByDeltaSign(transitions []*statecell.StateCell) ([]*statecell.StateCell, []*statecell.StateCell) {
	sort.SliceStable(transitions, func(i, j int) bool {
		lhv := transitions[i].Value().(crdtcommon.CRDT)
		rhv := transitions[i].Value().(crdtcommon.CRDT)
		_, lhvSign := lhv.Delta()
		_, rhvSign := rhv.Delta()

		return lhvSign != rhvSign && !lhvSign
	})

	offset, _ := slice.FindFirstIf(transitions, func(_ int, v *statecell.StateCell) bool {
		_, sign := v.Value().(crdtcommon.CRDT)
		return sign
	})

	if offset < 0 {
		offset = len(transitions)
	}
	return transitions[:offset], transitions[offset:]
}

// check if the value is either underflowed or overflowed. It returns the conflict if it is out of bounds.
func (this *Accumulator) CheckMinMax(transitions []*statecell.StateCell) *Collision {
	if len(transitions) <= 1 ||
		(transitions)[0].Value() == nil ||
		!(transitions)[0].Value().(crdtcommon.CRDT).IsCommutative() ||
		!(transitions)[0].Value().(crdtcommon.CRDT).IsNumeric() {
		return nil
	}

	workingCopy := slices.Clone(transitions) // Clone the transitions to avoid modifying the original order.
	slice.RemoveIf(&workingCopy, func(_ int, v *statecell.StateCell) bool {
		return v.IsReadOnly()
	})

	if len(workingCopy) <= 1 {
		return nil
	}

	negatives, positives := this.partitionByDeltaSign(workingCopy)

	// check for underflow.
	if len(negatives) > 0 { // all negative deltas
		underflowed := this.isOutOfLimits(*(workingCopy)[0].GetPath(), negatives)
		if underflowed != nil {
			underflowed.Reason = schedulercommon.WARN_OUT_OF_LOWER_LIMIT
			return underflowed
		}
	}

	// check for overflow.
	if len(positives) > 0 {
		overflowed := this.isOutOfLimits(*(workingCopy)[0].GetPath(), positives)
		if overflowed != nil {
			overflowed.Reason = schedulercommon.WARN_OUT_OF_UPPER_LIMIT
			return overflowed
		}
	}
	return nil
}

// check if the value is out of limits defined by the user. It can be different from the type bounds.
// It returns the conflict if it is out of bounds.
func (this *Accumulator) isOutOfLimits(_ string, newTrans []*statecell.StateCell) *Collision {
	if len(newTrans) <= 1 {
		return nil
	}

	initialv := newTrans[0].Value().(crdtcommon.CRDT).Clone().(crdtcommon.CRDT)

	typedVals := slice.Transform(newTrans, func(_ int, v *statecell.StateCell) crdtcommon.CRDT {
		return v.Value().(crdtcommon.CRDT)
	})

	_, offset, err := initialv.ApplyDelta(typedVals[1:])
	if err == nil {
		return nil
	}

	return &Collision{
		Self:  newTrans[0],
		Peers: newTrans[offset+1:], // Increment to get the ORIGINAL index in the transition sequence.
	}
}
