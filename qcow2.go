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
	"io"
	"os"
)

type Image struct {
	f           *os.File
	hdr         *HeaderAndAdditionalFields
	clusterSize int64
	cursor      int64
}

func Create(path string, size int64) (*Image, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	hdr, err := writeHeader(f, size)
	if err != nil {
		return nil, err
	}

	return &Image{
		f:           f,
		hdr:         hdr,
		clusterSize: int64(1 << hdr.ClusterBits),
	}, nil
}

func Open(path string, readOnly bool) (*Image, error) {
	var f *os.File
	var err error

	if readOnly {
		f, err = os.OpenFile(path, os.O_RDONLY, 0o444)
	} else {
		f, err = os.OpenFile(path, os.O_RDWR, 0o644)
	}

	if err != nil {
		return nil, err
	}

	hdr, err := readHeader(f)
	if err != nil {
		return nil, err
	}

	return &Image{
		f:           f,
		hdr:         hdr,
		clusterSize: int64(1 << hdr.ClusterBits),
	}, nil
}

func (i *Image) Close() error {
	return i.f.Close()
}

func (i *Image) Size() (int64, error) {
	return int64(i.hdr.Size), nil
}

func (i *Image) Sync() error {
	return i.f.Sync()
}

func (i *Image) Read(p []byte) (n int, err error) {
	n, err = i.ReadAt(p, i.cursor)
	i.cursor += int64(n)
	return
}

func (i *Image) ReadAt(p []byte, diskOffset int64) (n int, err error) {
	n = len(p)
	if n == 0 {
		return
	}

	if diskOffset+int64(n) > int64(i.hdr.Size) {
		n = int(int64(i.hdr.Size) - diskOffset)
		p = p[:n]
		err = io.EOF
	}

	remaining := n
	for remaining > 0 {
		r, err := clusterReader(i.f, i.hdr, diskOffset)
		if err != nil {
			return n - remaining, err
		}

		bytesInCluster, err := r.Read(p[:min(int64(i.clusterSize), int64(remaining))])
		if err != nil && err != io.EOF {
			return n - remaining, err
		}

		// advance to the next cluster
		diskOffset += int64(bytesInCluster)
		p = p[bytesInCluster:]
		remaining -= bytesInCluster
	}

	return
}

func (i *Image) Write(p []byte) (n int, err error) {
	n, err = i.WriteAt(p, i.cursor)
	i.cursor += int64(n)
	return
}

func (i *Image) WriteAt(p []byte, diskOffset int64) (n int, err error) {
	n = len(p)
	if n == 0 {
		return
	}

	if diskOffset+int64(n) > int64(i.hdr.Size) {
		err = io.ErrUnexpectedEOF
		return
	}

	remaining := n
	for remaining > 0 {
		w, err := clusterWriter(i.f, i.hdr, diskOffset)
		if err != nil {
			return n - remaining, err
		}

		bytesInCluster, err := w.Write(p[:min(int64(i.clusterSize), int64(remaining))])
		if err != nil && err != io.EOF {
			return n - remaining, err
		}

		// advance to the next cluster
		diskOffset += int64(bytesInCluster)
		p = p[bytesInCluster:]
		remaining -= bytesInCluster
	}

	return
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
