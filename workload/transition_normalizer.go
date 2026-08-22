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

package workload

import (
	"encoding/hex"
	"strings"

	crdtcommon "github.com/arcology-network/common-lib/crdt/common"
	statecell "github.com/arcology-network/common-lib/crdt/statecell"
	"github.com/arcology-network/common-lib/exp/slice"
	commontypes "github.com/arcology-network/common-lib/types"
	statecommon "github.com/arcology-network/state-engine/common"
	"github.com/holiman/uint256"
)

// ExecutionKind tells the normalizer whether an EVM execution owns a real
// transaction envelope or is internal work spawned by another execution.
type ExecutionKind uint8

const (
	// IndependentExecution represents a blockchain transaction. Its sender
	// nonce increment belongs to the transaction and must always be committed.
	IndependentExecution ExecutionKind = iota

	// InternalSubworkExecution represents Multiprocess work. It behaves like a
	// nested EVM call, so it must not consume another transaction-envelope nonce.
	InternalSubworkExecution
)

// TransactionNormalizer extracts unconditional gas transitions and applies the
// sender nonce semantics selected by ExecutionKind. Independent transactions
// keep their envelope nonce; internal subwork removes its synthetic increment.
type TransactionNormalizer struct {
	gasUsed  uint64
	Coinbase [20]byte
	txView   *commontypes.TransactionView
}

func NewTransactionNormalizer(gasUsed uint64, coinbase [20]byte, txView *commontypes.TransactionView) *TransactionNormalizer {
	return &TransactionNormalizer{
		gasUsed:  gasUsed,
		Coinbase: coinbase,
		txView:   txView,
	}
}

// insertGasTransition isolates the gas component of a balance update. If the
// existing transition’s delta already equals the gas fee, it is marked as
// conflict-immune and reused. Otherwise, a new transition is cloned with its
// delta set to the exact gas amount. The returned transition always has
// SkipConflictCheck enabled so it commits unconditionally.
func (this *TransactionNormalizer) insertGasTransition(balanceTransition *statecell.StateCell, gasDelta *uint256.Int, isCredit bool) *statecell.StateCell {
	v, _ := balanceTransition.Value().(crdtcommon.CRDT).Delta()
	totalDelta := v.(uint256.Int)

	if totalDelta.Cmp(gasDelta) == 0 { // Balance change == gas fee paid.
		balanceTransition.Property.SkipConflictCheck(true) // Won't be affect by conflicts
		return balanceTransition
	}

	// Separate the gas fee from the balance change and generate a new transition for that.
	gasTransition := balanceTransition.Clone().(*statecell.StateCell)
	gasTransition.Value().(crdtcommon.CRDT).SetDelta(*gasDelta, isCredit) // Set the gas fee.
	// gasTransition.Value().(crdtcommon.CRDT).SetDeltaSign(isCredit) // Negative for the sender, positive for the coinbase.
	gasTransition.Property.SkipConflictCheck(true)
	return gasTransition
}

func (this *TransactionNormalizer) Normalize(
	PreTransitions, RawStateRecords []*statecell.StateCell,
	executionKind ExecutionKind,
) ([]*statecell.StateCell, []*statecell.StateCell) {
	if len(RawStateRecords) == 0 {
		return RawStateRecords, nil
	}

	// Get the sender address from the pre-transitions, whose nonce increments need to be
	// unapplied.
	if len(PreTransitions) > 0 {
		senders := slice.Transform(PreTransitions, func(i int, v *statecell.StateCell) string {
			return statecommon.ParseAddressSubString(*v.GetPath())
		})
		this.UnapplyNonceOffset(senders, RawStateRecords) // Remove the nonce offset first.
	}

	// Gas belongs to the enclosing transaction even when its execution fails.
	gasTransitions := this.SeparateGasTransitions(RawStateRecords) // Post-process gas transitions.

	// Only a real blockchain transaction owns an unconditional sender nonce
	// increment. Internal subwork removes the synthetic sender nonce transition
	// inserted by the transaction-transition machinery.
	nonceTransitions := []*statecell.StateCell{}
	if executionKind == InternalSubworkExecution {
		RawStateRecords = this.removeInternalSenderNonceTransition(RawStateRecords)
	} else {
		nonceTransitions = this.MarkNonceConflictImmune(RawStateRecords)
	}

	return RawStateRecords, append(gasTransitions, nonceTransitions...)
}

// SeparateGasTransitions extracts unconditional gas fee transfers(for execution) from from balance transitions.
func (this *TransactionNormalizer) SeparateGasTransitions(RawStateRecords []*statecell.StateCell) []*statecell.StateCell {
	if this.txView.From == this.Coinbase {
		return nil
	}

	gasTransitions := []*statecell.StateCell{}
	senderString := hex.EncodeToString(this.txView.From[:])
	_, senderBalance := slice.FindFirstIf(RawStateRecords, func(_ int, v *statecell.StateCell) bool { //It includes the gas fee and possible transfers.
		return v != nil &&
			strings.HasSuffix(*v.GetPath(), "/balance") &&
			strings.Contains(*v.GetPath(), senderString)
	})

	coinbaseString := hex.EncodeToString(this.Coinbase[:])
	_, coinbaseBalance := slice.FindFirstIf(RawStateRecords, func(_ int, v *statecell.StateCell) bool {
		return v != nil &&
			strings.HasSuffix(*v.GetPath(), "/balance") &&
			strings.Contains(*v.GetPath(), coinbaseString)
	})

	// Usually, neither the sender balance nor the coinbase balance can be nil unless the transaction
	// is a L1->L2 transaction derived from a transaction receipt and the network is in a L2 setup.
	if senderBalance != nil && coinbaseBalance != nil {
		// Separate the gas fee from the balance change and generate a new transition for that. It will be immune to the execution status.
		gasPrice := &uint256.Int{}
		gasPrice.SetFromBig(this.txView.GasPrice)
		gasUsedInWei := new(uint256.Int).Mul(uint256.NewInt(this.gasUsed), gasPrice)
		if debit := this.insertGasTransition(*senderBalance, gasUsedInWei, false); debit != nil {
			gasTransitions = append(gasTransitions, debit)
		}

		if credit := this.insertGasTransition(*coinbaseBalance, gasUsedInWei, true); credit != nil {
			gasTransitions = append(gasTransitions, credit)
		}
	}
	return gasTransitions
}

// MarkNonceConflictImmune locates the nonce update for the transaction sender and marks it as
// conflict-immune. A sender's nonce must always be incremented and committed regardless
// of whether the transaction succeeds or reverts.
//
// In Ethereum semantics, nonce incrementation is unconditional once a transaction enters
// the execution pipeline. To preserve this behavior under Arcology's optimistic
// concurrency control, the nonce transition is flagged with SkipConflictCheck = true so
// that it bypasses conflict validation and is always included in the final commit set.
//
// If the sender's nonce update is not present in RawStateRecords (e.g., non-standard
// system transactions or partial receipts), this function returns an empty slice.
func (this *TransactionNormalizer) MarkNonceConflictImmune(RawStateRecords []*statecell.StateCell) []*statecell.StateCell {
	nonceTransitions := []*statecell.StateCell{}
	_, senderNonce := this.findSenderNonceTransition(RawStateRecords)

	if senderNonce != nil {
		senderNonce.Property.SkipConflictCheck(true)             // Won't be affected by conflicts either.
		nonceTransitions = append(nonceTransitions, senderNonce) // Commit the transaction nonce even if execution is unsuccessful.
	}
	return nonceTransitions
}

// removeInternalSenderNonceTransition removes the EOA sender nonce record
// created by running internal subwork through the transaction pipeline.
// Contract creator nonce changes use the creator's account path and therefore
// remain separate records.
func (this *TransactionNormalizer) removeInternalSenderNonceTransition(RawStateRecords []*statecell.StateCell) []*statecell.StateCell {
	index, _ := this.findSenderNonceTransition(RawStateRecords)
	return append(RawStateRecords[:index], RawStateRecords[index+1:]...)
}

// findSenderNonceTransition is the shared sender-nonce lookup used by both
// policies. Keeping identity matching here prevents the independent and
// internal execution paths from drifting apart.
func (this *TransactionNormalizer) findSenderNonceTransition(RawStateRecords []*statecell.StateCell) (int, *statecell.StateCell) {
	sender := hex.EncodeToString(this.txView.From[:])
	for index, record := range RawStateRecords {
		if record == nil {
			continue
		}

		path := *record.GetPath()
		if strings.HasSuffix(path, statecommon.PATH_NONCE) && strings.Contains(path, sender) {
			return index, record
		}
	}

	return -1, nil
}

// When processing multiple transactions from the same sender in a single generation,all the parallel transactions
// are executed based on the same initial state, so they all see the same nonce value for the sender. To prevent
// nonce conflicts, we need to add an offset to the nonce for each transaction based on its order in the batch.
//
// The offsets need to be removed before committing the transitions to the state store, otherwise the nonce values
// will be incorrect.
func (*TransactionNormalizer) UnapplyNonceOffset(senders []string, RawStateRecords []*statecell.StateCell) {
	if len(senders) == 0 {
		return
	}

	for _, sender := range senders {
		for _, record := range RawStateRecords {
			if !strings.HasSuffix(*record.GetPath(), statecommon.PATH_NONCE) ||
				statecommon.ParseAddressSubString(*record.GetPath()) != sender {
				continue // Skip non-nonce transitions or transitions that don't belong to the sender.
			}

			nonceDelta, _ := record.Value().(crdtcommon.CRDT).Delta() // Get the total nonce delta
			if nonceDelta.(uint64) > 1 {
				// Nonce only increases by 1 for each transaction, so if the delta is greater than the reads, it means
				// there is an offset applied. We can reverse the offset by subtracting the offset from the delta.
				negativeOffset := nonceDelta.(uint64) - 1
				record.Value().(crdtcommon.CRDT).SetDelta(negativeOffset, false) // Remove the offset by applying a negative delta.
			}
		}
	}
}
