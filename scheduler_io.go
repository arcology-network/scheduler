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
	profile "github.com/arcology-network/scheduler/profile"
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

func (this *Scheduler) LoadFromStorage(contractAddr [20]byte, selector [4]byte) *profile.Callee {
	uid := profile.DeriveUID(contractAddr[:], selector[:])

	stgcommon.PathBuilder{contractAddr, selector, stgcommon.ETH_PATH}.UnderCalleeProfile
		path.BuildProfilePath(contractAddr, selector)
	this.store.Retrive()

}

// 							(path)
// [address]/funprofs/[selector/callee]/sequential (boolean)
//   								   /conflictWith(bytes)
