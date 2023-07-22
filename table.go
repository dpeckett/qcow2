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
	"os"
)

func readTable(f *os.File, imageOffset int64, n int) ([]uint64, error) {
	buf := make([]byte, 8*n)
	if _, err := f.ReadAt(buf, imageOffset); err != nil {
		return nil, fmt.Errorf("failed to read table: %w", err)
	}

	t := make([]uint64, n)
	if err := binary.Read(bytes.NewReader(buf), binary.BigEndian, &t); err != nil {
		return nil, fmt.Errorf("failed to read table: %w", err)
	}

	return t, nil
}

func writeTable(f *os.File, imageOffset int64, t []uint64) error {
	if err := binary.Write(newOffsetWriter(f, imageOffset), binary.BigEndian, &t); err != nil {
		return fmt.Errorf("failed to write table: %w", err)
	}

	return nil
}
