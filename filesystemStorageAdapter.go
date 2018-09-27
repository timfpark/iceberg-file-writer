package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)

type FilesystemStorageAdapter struct {
	BasePath string

	Input chan *Block
}

func (fsa *FilesystemStorageAdapter) processBlocks() {
	go func() {
		for {
			block, more := <-fsa.Input

			if !more {
				break
			}

			startingKeyAsBase32 := block.GetStartingKeyAsBase32()
			endingKeyAsBase32 := block.GetEndingKeyAsBase32()

			blockPath := fmt.Sprintf("%s/%s", fsa.BasePath, block.PartitionKey)
			blockFilePath := fmt.Sprintf("%s/%s-%s", blockPath, startingKeyAsBase32, endingKeyAsBase32)

			blockAsBinary := block.GetRowsAsBinary()

			os.MkdirAll(blockPath, os.ModePerm)
			ioutil.WriteFile(blockFilePath, blockAsBinary, 0644)
		}
	}()
}

func (fsa *FilesystemStorageAdapter) Start() (err error) {
	fsa.processBlocks()

	return nil
}

func (fsa *FilesystemStorageAdapter) Stop() (err error) {
	close(fsa.Input)

	// wait for completion
	ttl := 100
	for len(fsa.Input) > 0 && ttl > 0 {
		ttl--
		time.Sleep(200 * time.Millisecond)
	}

	if len(fsa.Input) > 0 {
		errorText := fmt.Sprintf("FilesystemStorageAdapter: input did not finish, still has %d blocks remaining", len(fsa.Input))
		return errors.New(errorText)
	} else {
		return nil
	}

}
