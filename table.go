/* SPDX-License-Identifier: Apache-2.0
 *
 * Copyright 2023 Damian Peckett <damian@peckett>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package qcow2

import (
	"encoding/binary"
	"fmt"

	"github.com/goburrow/cache"
)

type tableKey struct {
	imageOffset int64
	n           int
}

func (i *Image) tableLoader(key cache.Key) (cache.Value, error) {
	imageOffset := key.(tableKey).imageOffset
	n := key.(tableKey).n

	buf := make([]byte, 8*n)
	if _, err := i.f.ReadAt(buf, imageOffset); err != nil {
		return nil, fmt.Errorf("failed to read table: %w", err)
	}

	t := make([]uint64, n)
	for i := range t {
		t[i] = binary.BigEndian.Uint64(buf[i*8 : (i+1)*8])
	}

	return t, nil
}

func (i *Image) readTable(imageOffset int64, n int) ([]uint64, error) {
	t, err := i.tableCache.Get(tableKey{imageOffset: imageOffset, n: n})
	if err != nil {
		return nil, fmt.Errorf("failed to read table: %w", err)
	}

	return t.([]uint64), nil
}

func (i *Image) writeTable(imageOffset int64, t []uint64) error {
	buf := make([]byte, 8*len(t))
	for i, v := range t {
		binary.BigEndian.PutUint64(buf[i*8:(i+1)*8], v)
	}

	_, err := i.f.WriteAt(buf, imageOffset)
	if err != nil {
		return fmt.Errorf("failed to write table: %w", err)
	}

	// TODO: In the future when we support growing the table, we will need to
	// come up with a smarter way to invalidate the cache to avoid leaking
	// memory. But given we are using LRU it'll be evicted pretty quickly
	// anyway.
	i.tableCache.Invalidate(&tableKey{imageOffset: imageOffset, n: len(t)})

	return nil
}
