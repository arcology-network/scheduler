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
	"os"

	mapi "github.com/arcology-network/common-lib/exp/map"
	profile "github.com/arcology-network/scheduler/profiles"
)

func LoadFromFile(filepath string) (*Scheduler, error) {
	buffer, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}

	sch, err := NewScheduler(filepath)
	if err != nil {
		return nil, err
	}

	callees := profile.CalleeProfiles{}.Decode(buffer).(profile.CalleeProfiles)
	sch.LoadIn(callees)
	return sch, nil
}

func SaveToFile(this *Scheduler, filepath string) error {
	vals := mapi.Values(this.ProfileDict)
	buffer := profile.CalleeProfiles(vals).Encode()
	return os.WriteFile(filepath, buffer, 0644)
}

// func (this *Scheduler) LoadFromStorage() error {

// }

// 							(path)
// [address]/funprofs/[selector/callee]/sequential (boolean)
//   								   /conflictWith(bytes)
