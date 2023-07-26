package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gpu-ninja/qcow2"
	"github.com/silverisntgold/randshiro"
)

const blockSize = 4096    // 4KB block size.
const totalBlocks = 10000 // Total number of blocks to write/read.
const queueDepth = 20     // Concurrent users or operations.

type operation struct {
	isWrite bool
	*block
}

type block struct {
	offset int64
	crc    uint32
}

func main() {
	rng := randshiro.New128pp()
	randReader := &randshiroReader{rng: rng}

	tempDir, err := os.MkdirTemp("", "qcow2-benchmark")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	image, err := qcow2.Create(filepath.Join(tempDir, "test.qcow2"), 1<<30)
	if err != nil {
		log.Fatal(err)
	}

	imageSize, err := image.Size()
	if err != nil {
		log.Fatal(err)
	}

	var blocks []block
	for i := 0; i < totalBlocks; i++ {
		for {
			offset := int64(rng.Uint64() % (uint64(imageSize) - uint64(blockSize) + 1))
			newBlock := block{offset: offset}

			if err := checkBlockOverlap(newBlock, blocks); err != nil {
				continue
			}

			blocks = append(blocks, newBlock)
			break
		}
	}

	var writeOperations []operation
	for i := range blocks {
		writeOperations = append(writeOperations, operation{
			isWrite: true,
			block:   &blocks[i],
		})
	}

	var readOperations []operation
	for i := range blocks {
		readOperations = append(readOperations, operation{
			isWrite: false,
			block:   &blocks[i],
		})
	}

	var wg sync.WaitGroup
	jobCh := make(chan operation)

	for i := 0; i < queueDepth; i++ {
		go worker(&wg, jobCh, randReader, image)
	}

	// Start benchmark.
	start := time.Now()

	for _, op := range writeOperations {
		wg.Add(1)
		jobCh <- op
	}

	// Wait for all write operations to complete.
	wg.Wait()

	for _, op := range readOperations {
		wg.Add(1)
		jobCh <- op
	}

	close(jobCh)

	// wait for all read operations to complete.
	wg.Wait()

	// Stop benchmark.
	elapsed := time.Since(start)

	iops := float64(len(writeOperations)+len(readOperations)) / elapsed.Seconds()
	throughput := iops * float64(blockSize) / (1024 * 1024) // MB/s

	log.Printf("IOPS: %.2f, Throughput: %.2f MB/s\n", iops, throughput)
}

func worker(jobCompleted *sync.WaitGroup, jobCh <-chan operation, randReader io.Reader, image *qcow2.Image) {
	for op := range jobCh {
		data := make([]byte, blockSize)
		if op.isWrite {
			if _, err := randReader.Read(data); err != nil {
				log.Fatal(err)
			}

			if _, err := image.WriteAt(data, op.offset); err != nil {
				log.Fatal(err)
			}

			op.crc = crc32.ChecksumIEEE(data)
		} else {
			if _, err := image.ReadAt(data, op.offset); err != nil {
				log.Fatal(err)
			}

			// Compare written and read CRCs (to check for data corruption).
			if crc := crc32.ChecksumIEEE(data); crc != op.crc {
				log.Fatalf("CRC mismatch: %x != %x\n", crc, op.crc)
			}
		}
		jobCompleted.Done()
	}
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
		if overlap(newBlock.offset, blockSize, b.offset, blockSize) {
			return fmt.Errorf("block overlap detected")
		}
	}
	return nil
}

func overlap(a, asize, b, bsize int64) bool {
	return a < b+bsize && b < a+asize
}
