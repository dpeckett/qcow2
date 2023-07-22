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
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/gpu-ninja/qcow2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testImage    = "testdata/cirros-0.5.1-x86_64-disk.img"
	testImageURL = "https://download.cirros-cloud.net/0.5.1/cirros-0.5.1-x86_64-disk.img"
)

func TestQCOW2ImageEndToEnd(t *testing.T) {
	if _, err := os.Stat(testImage); os.IsNotExist(err) {
		t.Log("Downloading test image...")
		err := downloadFile(testImage, testImageURL)
		require.NoError(t, err)
	}

	input, err := qcow2.Open(testImage)
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
