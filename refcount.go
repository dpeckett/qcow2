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

func getRefcount(f *os.File, hdr *HeaderAndAdditionalFields, diskOffset int64) (uint64, error) {
	refcountOffset, err := diskToRefcountOffset(f, hdr, diskOffset)
	if err != nil {
		return 0, err
	}

	refcountBits := int64(1 << hdr.RefcountOrder)
	return readBits(f, refcountOffset, refcountBits)
}

func setRefcount(f *os.File, hdr *HeaderAndAdditionalFields, diskOffset int64, refcount uint64) error {
	refcountOffset, err := diskToRefcountOffset(f, hdr, diskOffset)
	if err != nil {
		return err
	}

	refcountBits := int64(1 << hdr.RefcountOrder)
	return writeBits(f, refcountOffset, refcountBits, refcount)
}

func diskToRefcountOffset(f *os.File, hdr *HeaderAndAdditionalFields, diskOffset int64) (int64, error) {
	refcountBits := int64(1 << hdr.RefcountOrder)
	clusterSize := int64(1 << hdr.ClusterBits)

	refcountBlockEntries := clusterSize * 8 / refcountBits

	refcountBlockIndex := (diskOffset / clusterSize) % refcountBlockEntries
	refcountTableIndex := (diskOffset / clusterSize) / refcountBlockEntries

	refCountTableEntries := (int64(hdr.RefcountTableClusters) * clusterSize) / 8
	refCountTable, err := readTable(f, int64(hdr.RefcountTableOffset), int(refCountTableEntries))
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
