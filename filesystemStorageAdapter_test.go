package main

import (
	"fmt"
	"testing"
	"time"

	goavro "gopkg.in/linkedin/goavro.v2"
)

func TestFilesystemStorageAdapter(t *testing.T) {
	fmt.Println("Starting TestFilesystemStorageAdapter")

	input := make(chan *Block)

	filesystemStorageAdapter := &FilesystemStorageAdapter{
		BasePath: "./test/data",
		Input:    input,
	}

	err := filesystemStorageAdapter.Start()
	if err != nil {
		t.Errorf("filesystemStorageAdapter failed to start: %s", err)
	}

	codec, err := goavro.NewCodec(`
	{
		"type": "record",
		"name": "Test",
		"fields": [
			{ "name": "userId", "type": "string" },
			{ "name": "timestamp", "type": "long" }
		]
	}`)

	blockManager := &BlockManager{
		ID: "userId-timestamp",

		MaxAge:  60 * 1000, // milliseconds
		MaxSize: 1024,      // rows

		PartitionColumn: "userId",
		KeyColumn:       "timestamp",
		KeyType:         "uint32",

		Input:  nil,
		Output: nil,
		Codec:  codec,
	}

	block := NewBlock(blockManager, "userid1")

	textual := []byte(`{"userId":"userid1","timestamp":23432423}`)

	// Convert textual Avro data (in Avro JSON format) to native Go form
	native, _, err := codec.NativeFromTextual(textual)
	if err != nil {
		t.Errorf("NativeFromTextual failed: %s", err)
	}

	block.Write(native)

	input <- block

	time.Sleep(1 * time.Second)

	fmt.Println("Finishing TestFilesystemStorageAdapter")
}
