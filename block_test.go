package main

import (
	"fmt"
	"testing"

	goavro "gopkg.in/linkedin/goavro.v2"
)

func TestBlock(t *testing.T) {
	fmt.Println("Starting TestBlock")

	codec, err := goavro.NewCodec(`
{
	"type": "record",
	"name": "Test",
	"fields": [
		{ "name": "userId", "type": "string" },
		{ "name": "timestamp", "type": "long" }
	]
}`)

	if err != nil {
		t.Errorf("NewCodec failed: %s", err)
	}

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

	if block.Length() != 1 {
		t.Errorf("Row length after write incorrect: %d", block.Length())
	}

	binary := block.GetRowsAsBinary()

	if len(binary) != 12 {
		t.Errorf("Binary length after write incorrect: %d", len(binary))
	}

	fmt.Println("Finished TestBlock")
}
