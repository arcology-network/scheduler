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

package profile

import (
	"errors"

	stgcommon "github.com/arcology-network/storage-committer/common"
	"github.com/arcology-network/storage-committer/storage/cache"
)

type SchedulerStorage struct {
	*cache.StateCache
	LocalCache  map[uint64]*Callee
	Updated     []*Callee
	PathBuilder *stgcommon.PathBuilder
}

func NewSchedulerStorage(stateCache *cache.StateCache, pathBuilder *stgcommon.PathBuilder) *SchedulerStorage {
	return &SchedulerStorage{
		StateCache:  stateCache,
		LocalCache:  make(map[uint64]*Callee),
		Updated:     make([]*Callee, 0),
		PathBuilder: pathBuilder,
	}
}

func (this *SchedulerStorage) ReadByPath(path string) (*Callee, error) {
	pathBuiler, err := stgcommon.NewPathBuilderFromPath(path)
	if err != nil {
		return nil, errors.New("Failed to parse the path")
	}

	UID := DeriveUID(pathBuiler.Address[:], pathBuiler.Selector[:])

	// If exists in local cache, return directly
	if callee, exists := this.LocalCache[UID]; exists {
		return callee, nil // Return from local cache
	}

	// Otherwise, read from storage
	callee := NewCalleeFromStorage(pathBuiler, this)
	return callee, nil
}

func (this *SchedulerStorage) Read(pathBuiler *stgcommon.PathBuilder) (*Callee, error) {
	// pathBuiler := &stgcommon.PathBuilder{
	// 	Address:  addr,
	// 	Selector: selector,
	// 	Platform: stgcommon.ETH_PATH,
	// }

	UID := DeriveUID(pathBuiler.Address[:], pathBuiler.Selector[:])

	// If exists in local cache, return directly
	if callee, exists := this.LocalCache[UID]; exists {
		return callee, nil // Return from local cache
	}

	// Otherwise, read from storage
	callee := NewCalleeFromStorage(pathBuiler, this)
	return callee, nil
}

// Write the callee profile to the storage.
// The results need to be exported and committed to the storage later.
func (this *SchedulerStorage) Write(path string, v any) error {
	_, err := this.StateCache.Write(0, path, v)
	return err
}
