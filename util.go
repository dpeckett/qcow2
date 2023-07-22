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
	"os"
)

// zeroReader is a reader that reads zeros.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// offsetReader is a reader that reads from a given offset in a file.
type offsetReader struct {
	f      *os.File
	offset int64
}

func newOffsetReader(f *os.File, offset int64) *offsetReader {
	return &offsetReader{f: f, offset: offset}
}

func (r *offsetReader) Read(p []byte) (int, error) {
	n, err := r.f.ReadAt(p, r.offset)
	r.offset += int64(n)

	return n, err
}

// offsetWriter is a writer that writes to a given offset in a file.
type offsetWriter struct {
	f      *os.File
	offset int64
}

func newOffsetWriter(f *os.File, offset int64) *offsetWriter {
	return &offsetWriter{f: f, offset: offset}
}

func (w *offsetWriter) Write(p []byte) (int, error) {
	n, err := w.f.WriteAt(p, w.offset)
	w.offset += int64(n)

	return n, err
}
