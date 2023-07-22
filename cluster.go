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
	"compress/flate"
	"fmt"
	"io"
	"os"
)

func clusterReader(f *os.File, hdr *HeaderAndAdditionalFields, diskOffset int64) (io.Reader, error) {
	clusterSize := int64(1 << hdr.ClusterBits)
	bytesRemainingInCluster := clusterSize - (diskOffset % clusterSize)

	l2Entries := clusterSize / 8
	l2Index := (diskOffset / clusterSize) % l2Entries
	l1Index := (diskOffset / clusterSize) / l2Entries

	l1Table, err := readTable(f, int64(hdr.L1TableOffset), int(hdr.L1Size))
	if err != nil {
		return nil, err
	}

	l1Entry := L1TableEntry(l1Table[l1Index])

	l2TableOffset := l1Entry.Offset()

	l2Table, err := readTable(f, l2TableOffset, int(l2Entries))
	if err != nil {
		return nil, err
	}

	l2Entry := L2TableEntry(l2Table[l2Index])

	// Is it a hole?
	if l2Entry == 0 || (!l2Entry.Compressed() && l2Entry&0x1 == 1) {
		return io.LimitReader(zeroReader{}, int64(bytesRemainingInCluster)), nil
	}

	// Is it a compressed cluster?
	if l2Entry.Compressed() {
		imageOffset := l2Entry.Offset(hdr)

		fr := flate.NewReader(io.LimitReader(newOffsetReader(f, imageOffset), l2Entry.CompressedSize(hdr)))

		if _, err := io.CopyN(io.Discard, fr, diskOffset%clusterSize); err != nil {
			return nil, err
		}

		return io.LimitReader(fr, int64(bytesRemainingInCluster)), nil
	}

	imageOffset := l2Entry.Offset(hdr) + (diskOffset % clusterSize)

	return io.LimitReader(newOffsetReader(f, imageOffset), int64(bytesRemainingInCluster)), nil
}

func clusterWriter(f *os.File, hdr *HeaderAndAdditionalFields, diskOffset int64) (io.Writer, error) {
	refcount, err := getRefcount(f, hdr, diskOffset)
	if err != nil {
		return nil, err
	}

	var imageOffset int64
	if refcount == 0 {
		imageOffset, err = allocateCluster(f, hdr)
		if err != nil {
			return nil, fmt.Errorf("failed to allocate cluster: %w", err)
		}

		if err := setRefcount(f, hdr, diskOffset, 1); err != nil {
			return nil, fmt.Errorf("failed to update refcount: %w", err)
		}

		if err := storeImageToDiskOffset(f, hdr, imageOffset, diskOffset); err != nil {
			return nil, fmt.Errorf("failed to update L2 table: %w", err)
		}
	} else if refcount == 1 {
		// Perform an in-place write.
		imageOffset, err = diskToImageOffset(f, hdr, diskOffset)
		if err != nil {
			return nil, fmt.Errorf("failed to get image offset: %w", err)
		}
	} else {
		// Copy the cluster and perform an in-place write.
		imageOffset, err = copyCluster(f, hdr, diskOffset)
		if err != nil {
			return nil, fmt.Errorf("failed to copy cluster: %w", err)
		}

		if err := setRefcount(f, hdr, diskOffset, 1); err != nil {
			return nil, fmt.Errorf("failed to update refcount: %w", err)
		}

		if err := storeImageToDiskOffset(f, hdr, imageOffset, diskOffset); err != nil {
			return nil, fmt.Errorf("failed to update L2 table: %w", err)
		}
	}

	return newOffsetWriter(f, imageOffset), nil
}

func allocateCluster(f *os.File, hdr *HeaderAndAdditionalFields) (int64, error) {
	imageOffset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	clusterSize := int64(1 << hdr.ClusterBits)
	if _, err := io.CopyN(newOffsetWriter(f, imageOffset), zeroReader{}, int64(clusterSize)); err != nil {
		return 0, err
	}

	return imageOffset, nil
}

func copyCluster(f *os.File, hdr *HeaderAndAdditionalFields, diskOffset int64) (int64, error) {
	imageOffset, err := diskToImageOffset(f, hdr, diskOffset)
	if err != nil {
		return 0, err
	}

	newImageOffset, err := allocateCluster(f, hdr)
	if err != nil {
		return 0, err
	}

	clusterSize := int64(1 << hdr.ClusterBits)
	if _, err := io.CopyN(newOffsetWriter(f, newImageOffset), newOffsetReader(
		f, alignToClusterBoundary(hdr, imageOffset)), int64(clusterSize)); err != nil {
		return 0, err
	}

	return newImageOffset + (imageOffset % clusterSize), nil
}

func storeImageToDiskOffset(f *os.File, hdr *HeaderAndAdditionalFields, imageOffset, diskOffset int64) error {
	clusterSize := int64(1 << hdr.ClusterBits)

	l2Entries := clusterSize / 8
	l2Index := (diskOffset / clusterSize) % l2Entries
	l1Index := (diskOffset / clusterSize) / l2Entries

	l1Table, err := readTable(f, int64(hdr.L1TableOffset), int(hdr.L1Size))
	if err != nil {
		return err
	}

	l1Entry := L1TableEntry(l1Table[l1Index])

	l2Table, err := readTable(f, l1Entry.Offset(), int(l2Entries))
	if err != nil {
		return err
	}

	l2Table[l2Index] = uint64(NewL2TableEntry(hdr, imageOffset, false, 0))

	if err := writeTable(f, l1Entry.Offset(), l2Table); err != nil {
		return err
	}

	return nil
}

func diskToImageOffset(f *os.File, hdr *HeaderAndAdditionalFields, diskOffset int64) (int64, error) {
	clusterSize := int64(1 << hdr.ClusterBits)

	l2Entries := clusterSize / 8
	l2Index := (diskOffset / clusterSize) % l2Entries
	l1Index := (diskOffset / clusterSize) / l2Entries

	l1Table, err := readTable(f, int64(hdr.L1TableOffset), int(hdr.L1Size))
	if err != nil {
		return 0, err
	}

	l1Entry := L1TableEntry(l1Table[l1Index])

	l2Table, err := readTable(f, l1Entry.Offset(), int(l2Entries))
	if err != nil {
		return 0, err
	}

	l2Entry := L2TableEntry(l2Table[l2Index])

	return l2Entry.Offset(hdr) + (diskOffset % clusterSize), nil
}

func alignToClusterBoundary(hdr *HeaderAndAdditionalFields, imageOffset int64) int64 {
	clusterSize := int64(1 << hdr.ClusterBits)
	return clusterSize - (imageOffset % clusterSize)
}
