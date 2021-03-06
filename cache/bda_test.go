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
	"github.com/pblcache/pblcache/tests"
	"testing"
)

func TestInsert(t *testing.T) {
	bda := NewBlockDescriptorArray(2)

	id := uint64(123)
	index, evictkey, evict := bda.Insert(id)
	tests.Assert(t, bda.bds[0].key == id)
	tests.Assert(t, bda.bds[0].clock_set == false)
	tests.Assert(t, bda.bds[0].used == true)
	tests.Assert(t, index == 0)
	tests.Assert(t, evictkey == INVALID_KEY)
	tests.Assert(t, evict == false)
}

func TestUsing(t *testing.T) {
	bda := NewBlockDescriptorArray(2)

	id := uint64(123)
	index, evictkey, evict := bda.Insert(id)
	tests.Assert(t, bda.bds[0].key == id)
	tests.Assert(t, bda.bds[0].clock_set == false)
	tests.Assert(t, bda.bds[0].used == true)
	tests.Assert(t, index == 0)
	tests.Assert(t, evictkey == INVALID_KEY)
	tests.Assert(t, evict == false)

	bda.Using(index)
	tests.Assert(t, bda.bds[0].key == id)
	tests.Assert(t, bda.bds[0].clock_set == true)
	tests.Assert(t, bda.bds[0].used == true)
}

func TestFree(t *testing.T) {
	bda := NewBlockDescriptorArray(2)

	id := uint64(123)
	index, evictkey, evict := bda.Insert(id)
	tests.Assert(t, bda.bds[0].key == id)
	tests.Assert(t, bda.bds[0].clock_set == false)
	tests.Assert(t, bda.bds[0].used == true)
	tests.Assert(t, index == 0)
	tests.Assert(t, evictkey == INVALID_KEY)
	tests.Assert(t, evict == false)

	bda.Free(index)
	tests.Assert(t, bda.bds[0].clock_set == false)
	tests.Assert(t, bda.bds[0].used == false)
}

func TestEvictions(t *testing.T) {
	bda := NewBlockDescriptorArray(2)

	id1 := uint64(123)
	id2 := uint64(456)
	id3 := uint64(678)

	index, evictkey, evict := bda.Insert(id1)
	tests.Assert(t, bda.bds[0].key == id1)
	tests.Assert(t, bda.bds[0].clock_set == false)
	tests.Assert(t, bda.bds[0].used == true)
	tests.Assert(t, index == 0)
	tests.Assert(t, evictkey == INVALID_KEY)
	tests.Assert(t, evict == false)

	index, evictkey, evict = bda.Insert(id2)
	tests.Assert(t, bda.bds[0].key == id1)
	tests.Assert(t, bda.bds[0].clock_set == false)
	tests.Assert(t, bda.bds[0].used == true)
	tests.Assert(t, bda.bds[1].key == id2)
	tests.Assert(t, bda.bds[1].clock_set == false)
	tests.Assert(t, bda.bds[1].used == true)
	tests.Assert(t, index == 1)
	tests.Assert(t, evictkey == INVALID_KEY)
	tests.Assert(t, evict == false)

	bda.Using(0)
	tests.Assert(t, bda.bds[0].key == id1)
	tests.Assert(t, bda.bds[0].clock_set == true)
	tests.Assert(t, bda.bds[0].used == true)

	index, evictkey, evict = bda.Insert(id3)
	tests.Assert(t, bda.bds[0].key == id1)
	tests.Assert(t, bda.bds[0].clock_set == false)
	tests.Assert(t, bda.bds[0].used == true)
	tests.Assert(t, bda.bds[1].key == id3)
	tests.Assert(t, bda.bds[1].clock_set == false)
	tests.Assert(t, bda.bds[1].used == true)
	tests.Assert(t, index == 1)
	tests.Assert(t, evictkey == id2)
	tests.Assert(t, evict == true)

	bda.Free(1)
	tests.Assert(t, bda.bds[1].clock_set == false)
	tests.Assert(t, bda.bds[1].used == false)

	index, evictkey, evict = bda.Insert(id2)
	tests.Assert(t, bda.bds[0].key == id2)
	tests.Assert(t, bda.bds[0].clock_set == false)
	tests.Assert(t, bda.bds[0].used == true)
	tests.Assert(t, bda.bds[1].clock_set == false)
	tests.Assert(t, bda.bds[1].used == false)
	tests.Assert(t, index == 0)
	tests.Assert(t, evictkey == id1)
	tests.Assert(t, evict == true)
}
