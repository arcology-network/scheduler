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

package scheduler

import (
	"encoding/json"
	"log"
)

type Contract struct {
	index        uint32   `json:"index"`        // Index of the contract in the contract list
	address      [8]byte  `json:"address"`      // Short address
	signature    [4]byte  `json:"signature"`    // Function signature
	Indices      []uint32 `json:"indices"`      // Indices of the conflict contract indices.
	IsSequential bool     `json:"isSequential"` // A sequential only contract
}

func (this *Contract) Encode() ([]byte, error) {
	encoded, err := json.Marshal(this)
	if err != nil {
		log.Println("Failed to encode contract:", err)
		return nil, err
	}
	return encoded, nil
}

func (this *Contract) Decode(data []byte) *Contract {
	if err := json.Unmarshal(data, this); err != nil {
		log.Println("Failed to decode contract:", err)
		return this
	}
	return nil
}
