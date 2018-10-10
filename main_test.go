package main

import (
	"log"
	"testing"
	"time"

	core "github.com/timfpark/iceberg-core"
	goavro "gopkg.in/linkedin/goavro.v2"
)

func TestMain(t *testing.T) {
	log.Println("Starting TestMain")

	codec, err := goavro.NewCodec(`{
		"type": "record",
		"name": "Location",
		"fields": [
			{ "name": "accuracy", "type": ["null", "double"], "default": null },
			{ "name": "altitude", "type": ["null", "double"], "default": null },
			{ "name": "altitudeAccuracy", "type": ["null", "double"], "default": null },
			{ "name": "course", "type": ["null", "double"], "default": null },
			{
				"name": "features",
				"type": {
					"type": "array",
					"items": { "name": "id", "type": "string" }
				}
			},
			{ "name": "latitude", "type": "double" },
			{ "name": "longitude", "type": "double" },
			{ "name": "speed", "type": ["null", "double"], "default": null },
			{ "name": "source", "type": "string", "default": "device" },
			{ "name": "timestamp", "type": "long" },
			{ "name": "user_id", "type": "string" }
		]
	}`)

	if err != nil {
		panic(err)
	}

	storageAdapter := &core.FilesystemStorageAdapter{
		BasePath:        "./test/data",
		Codec:           GetCodecFixture(),
		PartitionColumn: "user_id",
		KeyColumn:       "timestamp",
		CompressionName: "snappy",
		Input:           make(chan *core.Block, 128),
	}

	if err = storageAdapter.Start(); err != nil {
		t.Errorf("storageAdapter.Start failed with: %s", err)
	}

	streamAdapter := &core.FileStreamAdapter{
		FilePath: "./test/fixtures/test.avro",
		Codec:    GetCodecFixture(),
		Output:   make(chan interface{}, 1024),
	}

	if err = streamAdapter.Start(); err != nil {
		t.Errorf("streamAdapter.Start failed with: %s", err)
	}

	blockManager := &BlockManager{
		ID: "user_id-timestamp",

		MaxAge:  60 * 1000, // milliseconds
		MaxSize: 4096,      // rows

		PartitionColumn: "user_id",
		KeyColumn:       "timestamp",

		Input:  streamAdapter.Output,
		Output: storageAdapter.Input,
		Codec:  codec,
	}

	if err := blockManager.Start(); err != nil {
		t.Errorf("Start failed with %s\n", err)
	}

	time.Sleep(time.Second * 10)

	if err := blockManager.CommitBlocks(true); err != nil {
		t.Errorf("CommitBlocks failed with %s\n", err)
	}

	log.Println("Finished TestMain")
}
