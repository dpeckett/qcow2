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
)

func (i *Image) clusterReader(diskOffset int64) (io.Reader, error) {
	bytesRemainingInCluster := i.clusterSize - (diskOffset % i.clusterSize)

	l2Entries := i.clusterSize / 8
	l2Index := (diskOffset / i.clusterSize) % l2Entries
	l1Index := (diskOffset / i.clusterSize) / l2Entries

	l1Table, err := readTable(i.f, int64(i.hdr.L1TableOffset), int(i.hdr.L1Size))
	if err != nil {
		return nil, err
	}

	l1Entry := L1TableEntry(l1Table[l1Index])

	l2TableOffset := l1Entry.Offset()

	l2Table, err := readTable(i.f, l2TableOffset, int(l2Entries))
	if err != nil {
		return nil, err
	}

	l2Entry := L2TableEntry(l2Table[l2Index])

	// Is it a hole?
	if l2Entry.Unallocated() {
		return io.LimitReader(zeroReader{}, int64(bytesRemainingInCluster)), nil
	}

	// Is it a compressed cluster?
	if l2Entry.Compressed() {
		imageOffset := l2Entry.Offset(i.hdr)

		fr := flate.NewReader(io.LimitReader(newOffsetReader(i.f, imageOffset), l2Entry.CompressedSize(i.hdr)))

		if _, err := io.CopyN(io.Discard, fr, diskOffset%i.clusterSize); err != nil {
			return nil, err
		}

		return io.LimitReader(fr, int64(bytesRemainingInCluster)), nil
	}

	imageOffset := l2Entry.Offset(i.hdr) + (diskOffset % i.clusterSize)

	return io.LimitReader(newOffsetReader(i.f, imageOffset), int64(bytesRemainingInCluster)), nil
}

func (i *Image) clusterWriter(diskOffset int64) (io.Writer, error) {
	imageOffset, l2Entry, err := i.diskToImageOffset(diskOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to get image offset: %w", err)
	}

	var refcount uint64
	if !l2Entry.Unallocated() {
		refcount, err = i.getRefcount(diskOffset)
		if err != nil {
			return nil, err
		}
	}

	if refcount == 0 {
		imageOffsetClusterBase, err := i.allocateCluster()
		if err != nil {
			return nil, fmt.Errorf("failed to allocate cluster: %w", err)
		}

		if err := i.updateL2Table(imageOffsetClusterBase, i.alignToClusterBoundary(diskOffset)); err != nil {
			return nil, fmt.Errorf("failed to update L2 table: %w", err)
		}

		if err := i.setRefcount(diskOffset, 1); err != nil {
			return nil, fmt.Errorf("failed to update refcount: %w", err)
		}

		imageOffset = imageOffsetClusterBase + (diskOffset % i.clusterSize)
	} else if refcount > 1 {
		// Copy the cluster and perform an in-place write.
		imageOffsetClusterBase, err := i.copyCluster(i.alignToClusterBoundary(diskOffset))
		if err != nil {
			return nil, fmt.Errorf("failed to copy cluster: %w", err)
		}

		if err := i.updateL2Table(imageOffsetClusterBase, i.alignToClusterBoundary(diskOffset)); err != nil {
			return nil, fmt.Errorf("failed to update L2 table: %w", err)
		}

		if err := i.setRefcount(diskOffset, 1); err != nil {
			return nil, fmt.Errorf("failed to update refcount: %w", err)
		}

		imageOffset = imageOffsetClusterBase + (diskOffset % i.clusterSize)
	}

	return newLimitWriter(newOffsetWriter(i.f, imageOffset), int64(i.clusterSize-(diskOffset%i.clusterSize))), nil
}

func (i *Image) allocateCluster() (int64, error) {
	imageOffset, err := i.f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	clusterSize := int64(1 << i.hdr.ClusterBits)
	if _, err := io.CopyN(newOffsetWriter(i.f, imageOffset), zeroReader{}, int64(clusterSize)); err != nil {
		return 0, err
	}

	return imageOffset, nil
}

func (i *Image) copyCluster(diskOffset int64) (int64, error) {
	newImageOffset, err := i.allocateCluster()
	if err != nil {
		return 0, err
	}

	imageOffset, _, err := i.diskToImageOffset(diskOffset)
	if err != nil {
		return 0, err
	}

	if _, err := io.CopyN(newOffsetWriter(i.f, newImageOffset),
		newOffsetReader(i.f, imageOffset), int64(i.clusterSize)); err != nil {
		return 0, err
	}

	return newImageOffset, nil
}

func (i *Image) updateL2Table(imageOffset, diskOffset int64) error {
	l2Entries := i.clusterSize / 8
	l2Index := (diskOffset / i.clusterSize) % l2Entries
	l1Index := (diskOffset / i.clusterSize) / l2Entries

	l1Table, err := readTable(i.f, int64(i.hdr.L1TableOffset), int(i.hdr.L1Size))
	if err != nil {
		return err
	}

	l1Entry := L1TableEntry(l1Table[l1Index])

	l2Table, err := readTable(i.f, l1Entry.Offset(), int(l2Entries))
	if err != nil {
		return err
	}

	l2Table[l2Index] = uint64(NewL2TableEntry(i.hdr, imageOffset, false, 0))

	if err := writeTable(i.f, l1Entry.Offset(), l2Table); err != nil {
		return err
	}

	return nil
}

func (i *Image) diskToImageOffset(diskOffset int64) (int64, L2TableEntry, error) {
	clusterSize := int64(1 << i.hdr.ClusterBits)

	l2Entries := clusterSize / 8
	l2Index := (diskOffset / clusterSize) % l2Entries
	l1Index := (diskOffset / clusterSize) / l2Entries

	l1Table, err := readTable(i.f, int64(i.hdr.L1TableOffset), int(i.hdr.L1Size))
	if err != nil {
		return 0, 0, err
	}

	l1Entry := L1TableEntry(l1Table[l1Index])

	l2Table, err := readTable(i.f, l1Entry.Offset(), int(l2Entries))
	if err != nil {
		return 0, 0, err
	}

	l2Entry := L2TableEntry(l2Table[l2Index])

	return l2Entry.Offset(i.hdr) + (diskOffset % clusterSize), l2Entry, nil
}

func (i *Image) alignToClusterBoundary(offset int64) int64 {
	return i.clusterSize * (offset / i.clusterSize)
}
