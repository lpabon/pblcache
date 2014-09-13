//
// Copyright (c) 2014 The pblcache Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
package cache

import (
	"github.com/lpabon/godbc"
	"sync"
)

type AddressMapKey struct {
	objid, lba uint64
}

type AddressMap struct {
	addressmap map[AddressMapKey]uint64
	rwlock     sync.RWMutex
}

func NewAddressMap() *AddressMap {
	a := AddressMap{}
	a.addressmap = make(map[AddressMapKey]uint64)

	godbc.Ensure(a.addressmap != nil)

	return &a

}

func (a *AddressMap) SetAddressMapKey(key AddressMapKey, index uint64) {
	a.Set(key.objid, key.lba, index)
}
func (a *AddressMap) GetAddressMapKey(key AddressMapKey) (uint64, bool) {
	return a.Get(key.objid, key.lba)
}
func (a *AddressMap) DeleteAddressMapKey(key AddressMapKey) {
	a.Delete(key.objid, key.lba)
}
func (a *AddressMap) HasAddressMapKey(key AddressMapKey) bool {
	return a.Has(key.objid, key.lba)
}

func (a *AddressMap) Set(objid, lba uint64, index uint64) {
	a.rwlock.Lock()
	defer a.rwlock.Unlock()

	a.addressmap[AddressMapKey{objid, lba}] = index
}

func (a *AddressMap) Get(objid, lba uint64) (index uint64, found bool) {
	a.rwlock.RLock()
	defer a.rwlock.RUnlock()

	index, found = a.addressmap[AddressMapKey{objid, lba}]
	return
}

func (a *AddressMap) Delete(objid, lba uint64) {
	a.rwlock.Lock()
	defer a.rwlock.Unlock()

	delete(a.addressmap, AddressMapKey{objid, lba})
}

func (a *AddressMap) Has(objid, lba uint64) bool {
	_, ok := a.Get(objid, lba)

	return ok
}
