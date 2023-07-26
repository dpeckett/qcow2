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
	"fmt"
	"os"
)

func (i *Image) getRefcount(diskOffset int64) (uint64, error) {
	refcountOffset, err := i.diskToRefcountOffset(diskOffset)
	if err != nil {
		return 0, err
	}

	refcountBits := int64(1 << i.hdr.RefcountOrder)
	return readBits(i.f, refcountOffset, refcountBits)
}

func (i *Image) setRefcount(diskOffset int64, refcount uint64) error {
	refcountOffset, err := i.diskToRefcountOffset(diskOffset)
	if err != nil {
		return err
	}

	refcountBits := int64(1 << i.hdr.RefcountOrder)
	return writeBits(i.f, refcountOffset, refcountBits, refcount)
}

func (i *Image) incrementRefcounts(l1TableOffset int64, l1Size int) error {
	l2EntriesPerTable := i.clusterSize / 8

	// 1. Go through each L1 entry.
	l1Table, err := i.readTable(l1TableOffset, l1Size)
	if err != nil {
		return err
	}

	for l1Index, l1EntryRaw := range l1Table {
		l1Entry := L1TableEntry(l1EntryRaw)

		// 2. Go through each L2 entry.
		l2Table, err := i.readTable(l1Entry.Offset(), int(l2EntriesPerTable))
		if err != nil {
			return err
		}

		for l2Index, l2EntryRaw := range l2Table {
			l2Entry := L2TableEntry(l2EntryRaw)

			if l2Entry.Unallocated() {
				continue
			}

			// 3. Calculate the disk offset for the L2 entry.
			diskOffset := int64(l1Index)*l2EntriesPerTable*i.clusterSize + int64(l2Index)*i.clusterSize

			// 4. Get the refcount for the disk offset.
			refcount, err := i.getRefcount(diskOffset)
			if err != nil {
				return err
			}

			// 5. Increment the refcount for the disk offset.
			err = i.setRefcount(diskOffset, refcount+1)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (i *Image) diskToRefcountOffset(diskOffset int64) (int64, error) {
	refcountBits := int64(1 << i.hdr.RefcountOrder)

	refcountBlockEntries := i.clusterSize * 8 / refcountBits

	refcountBlockIndex := (diskOffset / i.clusterSize) % refcountBlockEntries
	refcountTableIndex := (diskOffset / i.clusterSize) / refcountBlockEntries

	refCountTableEntries := (int64(i.hdr.RefcountTableClusters) * i.clusterSize) / 8
	refCountTable, err := i.readTable(int64(i.hdr.RefcountTableOffset), int(refCountTableEntries))
	if err != nil {
		return 0, err
	}

	refcountBlockOffset := int64(refCountTable[refcountTableIndex] &^ ((1 << 9) - 1))

	return refcountBlockOffset + refcountBlockIndex*refcountBits, nil
}

func readBits(f *os.File, imageOffset int64, nBits int64) (uint64, error) {
	nBytes := (nBits + 7) / 8
	buf := make([]byte, nBytes)

	if _, err := f.ReadAt(buf, imageOffset); err != nil {
		return 0, fmt.Errorf("failed to read bits: %w", err)
	}

	var bits uint64
	for bitIdx := 0; bitIdx < int(nBits); bitIdx++ {
		bits <<= 1
		byteIdx := bitIdx / 8
		bitPosition := 7 - (bitIdx % 8)
		if buf[byteIdx]&(1<<bitPosition) != 0 {
			bits |= 1
		}
	}

	return bits, nil
}

func writeBits(f *os.File, imageOffset int64, nBits int64, value uint64) error {
	nBytes := (nBits + 7) / 8
	buf := make([]byte, nBytes)

	for bitIdx := 0; bitIdx < int(nBits); bitIdx++ {
		bitMask := uint64(1) << (uint64(nBits) - 1 - uint64(bitIdx))
		if value&bitMask != 0 {
			byteIdx := bitIdx / 8
			buf[byteIdx] |= 1 << (7 - (bitIdx % 8))
		}
	}

	if _, err := f.WriteAt(buf, imageOffset); err != nil {
		return fmt.Errorf("failed to write bits: %w", err)
	}

	return nil
}
