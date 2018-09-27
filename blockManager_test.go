package main

import (
	"fmt"
	"testing"
	"time"

	goavro "gopkg.in/linkedin/goavro.v2"
)

func TestBlockManager(t *testing.T) {
	fmt.Println("Starting TestBlockManager")

	codec, err := goavro.NewCodec(`
	{
		"type": "record",
		"name": "Test",
		"fields": [
			{ "name": "userId", "type": "string" },
			{ "name": "timestamp", "type": "long" }
		]
	}`)

	input := make(chan interface{})
	output := make(chan *Block)

	textual := []byte(`{"userId":"userid1","timestamp":23432423}`)

	native, _, err := codec.NativeFromTextual(textual)
	if err != nil {
		t.Errorf("NativeFromTextual failed: %s", err)
	}

	blockManager := &BlockManager{
		MaxAge:  1000, // milliseconds
		MaxSize: 1024, // rows

		PartitionColumn: "userId",
		KeyColumn:       "timestamp",
		KeyType:         "int",

		Input:  input,
		Output: output,
		Codec:  codec,
	}

	err = blockManager.Start()
	if err != nil {
		t.Errorf("Block Manager start failed with error: %s", err)
	}

	start := time.Now()

	input <- native
	input <- native
	block := <-output

	ageMillis := uint32(time.Now().Sub(start).Seconds() * 1000.0)

	if ageMillis < blockManager.MaxAge {
		t.Errorf("Block was committed too early: %d vs. %d", ageMillis, blockManager.MaxAge)
	}

	if block.Length() != 2 {
		t.Errorf("Block is the wrong size: %d vs. 2", block.Length())
	}

	fmt.Println("Finished TestBlockManager")
}
