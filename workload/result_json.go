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

package workload

import (
	// "github.com/arcology-network/common-lib/codec"

	"encoding/json"

	"github.com/arcology-network/common-lib/crdt/statecell"
	commontype "github.com/arcology-network/common-lib/types"
	evmcore "github.com/ethereum/go-ethereum/core"
	ethcoretypes "github.com/ethereum/go-ethereum/core/types"
)

// It is mainly for debugging and logging purposes from EXTERNAL tools. The system itself
// does NOT rely on this encoding for ANY functionality.

func (this *Result) MarshalJSON() ([]byte, error) {
	type resultAlias struct {
		GenerationID  uint64 `json:"generationId"`
		JobSequenceID uint64 `json:"jobSequenceId"`
		JobID         uint64 `json:"jobId"`

		TxInfo          *commontype.TransactionView `json:"stdMsg"`
		RawStateRecords []encodedStateMeta          `json:"rawStateRecords"`
		Immuned         []encodedStateMeta          `json:"immuned"`
		Receipt         *ethcoretypes.Receipt       `json:"receipt"`
		EvmResult       *evmcore.ExecutionResult    `json:"evmResult"`

		Err string `json:"err"`
	}

	alias := resultAlias{
		GenerationID:    this.GenerationID,
		JobSequenceID:   this.JobSequenceID,
		JobID:           this.JobID,
		RawStateRecords: encodeStateCells(this.RawStateRecords),
		Immuned:         encodeStateCells(this.Immuned),
		Receipt:         this.Receipt,
		EvmResult:       this.EvmResult,
	}

	if this.Err != nil {
		alias.Err = this.Err.Error()
	}

	return json.Marshal(&alias)
}

// encodedStateMeta is a JSON-serializable representation of a StateCell for Result encoding.
// No actual state data is included, only metadata for tracking purposes.
type encodedStateMeta struct {
	GenerationID  uint64 `json:"generationId"`
	JobSequenceID uint64 `json:"jobSequenceId"`
	JobID         uint64 `json:"jobId"`

	Tx          uint64 `json:"tx"`
	Sequence    uint64 `json:"sequence"`
	Path        string `json:"path"`
	Reads       uint32 `json:"reads"`
	Writes      uint32 `json:"writes"`
	DeltaWrites uint32 `json:"deltaWrites"`
}

func encodeStateCells(cells []*statecell.StateCell) []encodedStateMeta {
	encoded := make([]encodedStateMeta, 0, len(cells))
	for _, cell := range cells {
		if cell == nil {
			continue
		}

		path := ""
		if p := cell.GetPath(); p != nil {
			path = *p
		}

		encoded = append(encoded, encodedStateMeta{
			Tx:            cell.GetTx(),
			GenerationID:  cell.GenerationID,
			JobSequenceID: cell.JobSequenceID,
			JobID:         cell.JobID,
			Sequence:      cell.JobSequenceID,
			Path:          path,
			Reads:         cell.Reads(),
			Writes:        cell.Writes(),
			DeltaWrites:   cell.DeltaWrites(),
		})
	}

	if len(encoded) == 0 {
		return []encodedStateMeta{}
	}

	return encoded
}
