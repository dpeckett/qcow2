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

const (
	// Magic bytes for QCOW2 file format.
	Magic = 0x514649FB
)

// Version is the QCOW version number.
type Version uint32

const (
	// Version3 is the QCOW version 3.
	// Only version 3 is supported by this library.
	Version3 Version = 3
)

// EncryptionMethod is the disk encryption method.
type EncryptionMethod uint32

const (
	NoEncryption  EncryptionMethod = 0
	AesEncryption EncryptionMethod = 1
)

// RefcountOrder is the descriptor for refcount width:
// 4 implies 16 bits, 5 implies 32 bits, 6 implies 64 bits.
type RefcountOrder uint32

const (
	RefcountOrder16 RefcountOrder = 4
	RefcountOrder32 RefcountOrder = 5
	RefcountOrder64 RefcountOrder = 6
)

// IncompatibleFeatures is a bitmask of incompatible features.
type IncompatibleFeatures uint64

const (
	// IncompatibleDirty is the dirty bit. If this bit is set then refcounts may be
	// inconsistent, make sure to scan L1/L2 tables to repair refcounts before
	// accessing the image.
	IncompatibleDirty IncompatibleFeatures = 1 << 0
	// IncompatibleCorrupt is the corrupt bit. If this bit is set then any data
	// structure may be corrupt and the image must not be written to (unless for
	// regaining consistency).
	IncompatibleCorrupt IncompatibleFeatures = 1 << 1
	// IncompatibleExternalData is the external data file bit. If this bit is set, an
	// external data file is used. Guest clusters are then stored in the external
	// data file. For such images, clusters in the external data file are not
	// refcounted.
	IncompatibleExternalData IncompatibleFeatures = 1 << 2
	// IncompatibleExtendedL2 is the extended L2 entries bit. If this bit is set then
	// L2 table entries use an extended format that allows subcluster-based
	// allocation.
	IncompatibleExtendedL2 IncompatibleFeatures = 1 << 3
)

// CompatibleFeatures is a bitmask of compatible features.
type CompatibleFeatures uint64

const (
	// CompatibleLazyRefcounts is the lazy refcounts bit. If this bit is set then
	// lazy refcount updates can be used. This means marking the image file dirty
	// and postponing refcount metadata updates.
	CompatibleLazyRefcounts CompatibleFeatures = 1 << 0
)

// AutoclearFeatures is a bitmask of auto-clear features.
type AutoclearFeatures uint64

const (
	// AutoclearBitmaps is the bitmaps extension bit. This bit indicates
	// consistency for the bitmaps extension data.
	AutoclearBitmaps AutoclearFeatures = 1 << 0
	// AutoclearRaw is the raw external data bit. If this bit is set,
	// the external data file can be read as a consistent standalone raw image
	// without looking at the qcow2 metadata.
	AutoclearRaw AutoclearFeatures = 1 << 1
)

// CompressionType is the compression method used for compressed clusters.
type CompressionType uint8

const (
	// CompressionTypeDeflate is the deflate compression type.
	CompressionTypeDeflate CompressionType = 0
	// CompressionTypeZstd is the zstd compression type.
	CompressionTypeZstd CompressionType = 1
)

// Header is the QCOW image header.
type Header struct {
	// Magic is the QCOW magic bytes: 'Q', 'F', 'I', 0xfb.
	Magic uint32
	// Version is the QCOW version number.
	Version Version
	// BackingFileOffset is the offset into the image file at which the backing file name is stored (or 0 if no backing file).
	BackingFileOffset uint64
	// BackingFileSize is the length of the backing file name in bytes.
	BackingFileSize uint32
	// ClusterBits is the number of bits that are used for addressing an offset within a cluster.
	ClusterBits uint32
	// Size is the size of the disk image (in bytes).
	Size uint64
	// CryptMethod is the encryption method.
	CryptMethod EncryptionMethod
	// L1Size is the number of entries in the active L1 table.
	L1Size uint32
	// L1TableOffset is the offset into the image file at which the active L1 table starts.
	L1TableOffset uint64
	// RefcountTableOffset is the offset into the image file at which the refcount table starts.
	RefcountTableOffset uint64
	// RefcountTableClusters is the number of clusters that the refcount table occupies.
	RefcountTableClusters uint32
	// NbSnapshots is the number of snapshots contained in the image.
	NbSnapshots uint32
	// SnapshotsOffset is the offset into the image file at which the snapshot table starts.
	SnapshotsOffset uint64
	// IncompatibleFeatures is a bitmask of incompatible features.
	IncompatibleFeatures IncompatibleFeatures
	// CompatibleFeatures is a bitmask of compatible features.
	CompatibleFeatures CompatibleFeatures
	// AutoclearFeatures is a bitmask of auto-clear features.
	AutoclearFeatures AutoclearFeatures
	// RefcountOrder is the descriptor for refcount width: 4 implies 16 bits, 5 implies 32 bits, 6 implies 64 bits.
	RefcountOrder RefcountOrder
	// HeaderLength is the size of the header structure in bytes.
	HeaderLength uint32
}

// HeaderAdditionalFields is the additional header fields for version 3.
type HeaderAdditionalFields struct {
	// CompressionType is the compression method used for compressed clusters.
	// All compressed clusters in an image use the same compression type.
	CompressionType CompressionType
	Padding         [7]byte
}

// HeaderExtensionType is the header extension type.
type HeaderExtensionType uint32

const (
	// EndOfHeaderExtensionArea is the end of the header extension area.
	EndOfHeaderExtensionArea HeaderExtensionType = 0x00000000
	// BackingFileFormatName is the backing file format name string.
	BackingFileFormatName HeaderExtensionType = 0xe2792aca
	// FeatureNameTable is the feature name table.
	FeatureNameTable HeaderExtensionType = 0x6803f857
	// BitmapsExtension is the bitmaps extension.
	BitmapsExtension HeaderExtensionType = 0x23852875
	// FullDiskEncryptionHeader is the full disk encryption header pointer.
	FullDiskEncryptionHeader HeaderExtensionType = 0x0537be77
	// ExternalDataFileName is the external data file name string.
	ExternalDataFileName HeaderExtensionType = 0x44415441
)

type HeaderExtensionMetadata struct {
	// Type is the header extension type.
	Type HeaderExtensionType
	// Length is the length of the header extension data.
	Length uint32
}

// HeaderExtension is a header extension.
type HeaderExtension struct {
	HeaderExtensionMetadata
	// Data is the header extension data.
	Data []byte
}

type HeaderAndAdditionalFields struct {
	Header
	AdditionalFields *HeaderAdditionalFields
	Extensions       []HeaderExtension
}

type L1TableEntry uint64

func NewL1TableEntry(offset int64) L1TableEntry {
	return L1TableEntry(1<<63) | L1TableEntry(offset&((1<<48-1)<<9))
}

func (e L1TableEntry) Used() bool {
	return e&(1<<63) != 0
}

func (e L1TableEntry) Offset() int64 {
	return int64(e & ((1<<48 - 1) << 9))
}

type L2TableEntry uint64

func NewL2TableEntry(hdr *HeaderAndAdditionalFields, offset int64, compressed bool, compressedSize int64) L2TableEntry {
	e := L2TableEntry(1 << 63)
	if compressed {
		hostClusterBits := 62 - (hdr.ClusterBits - 8)
		additionalSectors := compressedSize / 512
		e |= L2TableEntry(1<<62) | (L2TableEntry(additionalSectors) << hostClusterBits) | L2TableEntry(offset)&((1<<hostClusterBits)-1)
	} else {
		e |= L2TableEntry(offset & ((1<<48 - 1) << 9))
	}
	return e
}

func (e L2TableEntry) Used() bool {
	return e&(1<<63) != 0
}

func (e L2TableEntry) Compressed() bool {
	return e&(1<<62) != 0
}

func (e L2TableEntry) Offset(hdr *HeaderAndAdditionalFields) int64 {
	if e.Compressed() {
		hostClusterBits := 62 - (hdr.ClusterBits - 8)
		return int64(e & ((1 << hostClusterBits) - 1))
	} else {
		return int64(e & ((1<<48 - 1) << 9))
	}
}

func (e L2TableEntry) CompressedSize(hdr *HeaderAndAdditionalFields) int64 {
	hostClusterBits := 62 - (hdr.ClusterBits - 8)
	additionalSectors := int64((e >> hostClusterBits) & ((1 << (61 - hostClusterBits + 1)) - 1))
	return (additionalSectors + 1) * 512
}
