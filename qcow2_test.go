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

package qcow2_test

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/gpu-ninja/qcow2"
	"github.com/silverisntgold/randshiro"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testImage    = "testdata/cirros-0.5.1-x86_64-disk.img"
	testImageURL = "https://download.cirros-cloud.net/0.5.1/cirros-0.5.1-x86_64-disk.img"
)

type block struct {
	offset int64
	size   int
}

func TestImageEndToEnd(t *testing.T) {
	if _, err := os.Stat(testImage); os.IsNotExist(err) {
		err = os.MkdirAll("testdata", 0o755)
		require.NoError(t, err)

		t.Log("Downloading test image...")
		err := downloadFile(testImage, testImageURL)
		require.NoError(t, err)
	}

	input, err := qcow2.Open(testImage, true)
	require.NoError(t, err)
	defer input.Close()

	size, err := input.Size()
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, int64(117440512), size)

	outputPath := filepath.Join(t.TempDir(), "output.qcow2")
	output, err := qcow2.Create(outputPath, size)
	require.NoError(t, err)
	defer output.Close()

	_, err = io.Copy(output, input)
	require.NoError(t, err)

	// Shell out to qemu-img to verify the correctness of the output.
	rawPath := filepath.Join(t.TempDir(), "output.raw")
	cmd := exec.Command("qemu-img", "convert", "-f", "qcow2", "-O", "raw", outputPath, rawPath)

	err = cmd.Run()
	require.NoError(t, err)

	sum, err := hashFile(rawPath)
	require.NoError(t, err)

	expectedSum := "f8d297a47fd2017a776a2975919c90ba27131e2083fbf38ca434ba26a8b0dd6e"

	assert.Equal(t, expectedSum, sum)
}

// Fuzz the image reader/writer a bit.
func TestImageRandomReadsAndWrites(t *testing.T) {
	image, err := qcow2.Create(filepath.Join(t.TempDir(), "test.qcow2"), 1<<30)
	require.NoError(t, err)

	imageSize, err := image.Size()
	require.NoError(t, err)

	var blocks []block

	rng := randshiro.New128pp()
	randReader := &randshiroReader{rng: rng}

	for i := 0; i < 20; i++ {
		blockSize := int(rng.Uint64()>>(64-20)) + 1
		offset := int64(rng.Uint64() % (uint64(imageSize) - uint64(blockSize) + 1))

		newBlock := block{
			offset: offset,
			size:   blockSize,
		}

		if err := checkBlockOverlap(newBlock, blocks); err != nil {
			continue
		}

		blocks = append(blocks, newBlock)

		data := make([]byte, blockSize)
		_, err = randReader.Read(data)
		require.NoError(t, err)

		n, err := image.WriteAt(data, offset)
		require.NoError(t, err)
		require.Equal(t, n, len(data))

		readData := make([]byte, blockSize)
		n, err = image.ReadAt(readData, offset)
		require.NoError(t, err)
		require.Equal(t, n, len(data))

		require.Equal(t, data, readData)
	}

	err = image.Sync()
	require.NoError(t, err)

	// Not a full snapshot but will increment the refcount.
	err = image.Snapshot()
	require.NoError(t, err)

	// Now we'll update the blocks in random order.
	randshiro.Shuffle(rng, blocks)
	for _, b := range blocks {
		data := make([]byte, b.size)
		_, err = randReader.Read(data)
		require.NoError(t, err)

		n, err := image.WriteAt(data, b.offset)
		require.NoError(t, err)
		require.Equal(t, n, len(data))

		readData := make([]byte, b.size)
		n, err = image.ReadAt(readData, b.offset)
		require.NoError(t, err)
		require.Equal(t, n, len(data))

		require.Equal(t, data, readData)
	}
}

func downloadFile(path string, url string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.Copy(f, resp.Body)
	if err != nil {
		return err
	}

	return nil
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

type randshiroReader struct {
	rng *randshiro.Gen
}

func (r *randshiroReader) Read(p []byte) (int, error) {
	n := 0
	for len(p[n:]) >= 8 {
		binary.LittleEndian.PutUint64(p[n:], r.rng.Uint64())
		n += 8
	}
	if n < len(p) {
		remainingBytes := r.rng.Uint64()
		for i := n; i < len(p); i++ {
			p[i] = byte(remainingBytes)
			remainingBytes >>= 8
		}
		n = len(p)
	}
	return n, nil
}

func checkBlockOverlap(newBlock block, blocks []block) error {
	for _, b := range blocks {
		if overlap(newBlock.offset, int64(newBlock.size), b.offset, int64(b.size)) {
			return fmt.Errorf("block overlap detected")
		}
	}
	return nil
}

func overlap(a, asize, b, bsize int64) bool {
	return a < b+bsize && b < a+asize
}
