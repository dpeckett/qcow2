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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"unsafe"
)

func readHeader(f *os.File) (*HeaderAndAdditionalFields, error) {
	var hdr Header
	if err := binary.Read(f, binary.BigEndian, &hdr); err != nil {
		return nil, fmt.Errorf("failed to read image header: %w", err)
	}

	if hdr.Magic != Magic {
		return nil, fmt.Errorf("invalid magic bytes")
	}

	if hdr.Version != Version3 {
		return nil, fmt.Errorf("only version 3 is supported")
	}

	if hdr.BackingFileOffset != 0 {
		return nil, fmt.Errorf("backing files are not supported")
	}

	if hdr.CryptMethod != NoEncryption {
		return nil, fmt.Errorf("encryption is not supported")
	}

	if hdr.IncompatibleFeatures != 0 {
		return nil, fmt.Errorf("incompatible features are not supported")
	}

	var additionalFields *HeaderAdditionalFields
	if hdr.HeaderLength > uint32(unsafe.Sizeof(hdr)) {
		additionalFields = &HeaderAdditionalFields{}
		if err := binary.Read(f, binary.BigEndian, additionalFields); err != nil {
			return nil, fmt.Errorf("failed to read additional header fields: %w", err)
		}
	}

	if additionalFields != nil && additionalFields.CompressionType != CompressionTypeDeflate {
		return nil, fmt.Errorf("unsupported compression type")
	}

	var extensions []HeaderExtension
	for {
		var headerExtension HeaderExtension
		if err := binary.Read(f, binary.BigEndian, &headerExtension.HeaderExtensionMetadata); err != nil {
			return nil, fmt.Errorf("failed to read header extension type and length: %w", err)
		}

		if headerExtension.Type == EndOfHeaderExtensionArea {
			break
		}

		if headerExtension.Type == BackingFileFormatName ||
			headerExtension.Type == ExternalDataFileName ||
			headerExtension.Type == FullDiskEncryptionHeader {
			return nil, fmt.Errorf("unsupported header extension")
		}

		headerExtension.Data = make([]byte, headerExtension.Length)
		if _, err := io.ReadFull(f, headerExtension.Data); err != nil {
			return nil, fmt.Errorf("failed to read header extension data: %w", err)
		}

		extensions = append(extensions, headerExtension)
	}

	return &HeaderAndAdditionalFields{
		Header:           hdr,
		AdditionalFields: additionalFields,
		Extensions:       extensions,
	}, nil
}

func writeHeader(f *os.File, size int64) (*HeaderAndAdditionalFields, error) {
	hdr := Header{
		Magic:         Magic,
		Version:       Version3,
		ClusterBits:   16,
		Size:          uint64(size),
		CryptMethod:   NoEncryption,
		RefcountOrder: 4,
		HeaderLength:  uint32(unsafe.Sizeof(Header{})),
	}

	clusterSize := uint64(1 << hdr.ClusterBits)
	l2Entries := clusterSize / 8

	totalClusters := 1 + uint64(size)/clusterSize
	l2TableClusters := 1 + totalClusters/l2Entries

	hdr.L1Size = uint32(l2TableClusters)

	refcountBits := uint64(1 << hdr.RefcountOrder)
	refcountBlockEntries := clusterSize / refcountBits
	totalRefcountBlocks := 1 + totalClusters/refcountBlockEntries

	hdr.RefcountTableClusters = uint32(1 + totalRefcountBlocks/(clusterSize/8))

	/*
	 * Layout is (numbered by cluster):
	 * 1. Header
	 * 2. L1 table
	 * 3. L2 table/s
	 * 4. Refcount table/s
	 * 5. Refcount block/s
	 */

	imageOffset := int64(clusterSize)

	// write the L1 table
	l1Table := make([]uint64, clusterSize/8)

	for i := int64(0); i < int64(l2TableClusters); i++ {
		l1Table[i] = uint64(NewL1TableEntry(imageOffset + (i+1)*int64(clusterSize)))
	}

	if err := writeTable(f, imageOffset, l1Table); err != nil {
		return nil, fmt.Errorf("failed to write L1 table: %w", err)
	}
	hdr.L1TableOffset = uint64(imageOffset)
	imageOffset += int64(clusterSize)

	// write the L2 table/s
	for i := int64(0); i < int64(l2TableClusters); i++ {
		l2Table := make([]uint64, clusterSize/8)

		if err := writeTable(f, imageOffset, l2Table); err != nil {
			return nil, fmt.Errorf("failed to write L2 table: %w", err)
		}

		imageOffset += int64(clusterSize)
	}

	// write the refcount table
	refcountTable := make([]uint64, (uint64(hdr.RefcountTableClusters)*clusterSize)/8)

	for i := int64(0); i < int64(totalRefcountBlocks); i++ {
		refcountTable[i] = uint64(imageOffset+(i+int64(hdr.RefcountTableClusters))*int64(clusterSize)) &^ ((1 << 9) - 1)
	}

	if err := writeTable(f, imageOffset, refcountTable); err != nil {
		return nil, fmt.Errorf("failed to write refcount table: %w", err)
	}

	hdr.RefcountTableOffset = uint64(imageOffset)
	imageOffset += int64(hdr.RefcountTableClusters) * int64(clusterSize)

	// write the refcount block/s
	for i := int64(0); i < int64(totalRefcountBlocks); i++ {
		if _, err := io.CopyN(newOffsetWriter(f, imageOffset), zeroReader{}, int64(clusterSize)); err != nil {
			return nil, fmt.Errorf("failed to write refcount block: %w", err)
		}

		imageOffset += int64(clusterSize)
	}

	var encodedHdr bytes.Buffer
	if err := binary.Write(&encodedHdr, binary.BigEndian, hdr); err != nil {
		return nil, fmt.Errorf("failed to write image header: %w", err)
	}

	extension := HeaderExtensionMetadata{
		Type:   EndOfHeaderExtensionArea,
		Length: 0,
	}

	if err := binary.Write(&encodedHdr, binary.BigEndian, extension); err != nil {
		return nil, fmt.Errorf("failed to write end of header extension area: %w", err)
	}

	// finally write the header
	if _, err := io.CopyN(newOffsetWriter(f, 0), io.MultiReader(&encodedHdr, zeroReader{}), int64(clusterSize)); err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	return &HeaderAndAdditionalFields{
		Header: hdr,
	}, nil
}
