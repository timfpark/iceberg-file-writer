package main

import (
	"fmt"
	"log"
	"os"

	core "github.com/timfpark/iceberg-core"
	goavro "gopkg.in/linkedin/goavro.v2"
)

var (
	storageAdapter *core.FilesystemStorageAdapter
	blockManager   *core.BlockManager
	streamAdapter  *core.FileStreamAdapter
)

func start(inputFile string, outputPath string) (err error) {
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
		return err
	}

	storageAdapter = &core.FilesystemStorageAdapter{
		BasePath:        outputPath,
		Codec:           codec,
		PartitionColumn: "user_id",
		KeyColumn:       "timestamp",
		CompressionName: "snappy",
		Input:           make(chan *core.Block, 128),
	}

	if err = storageAdapter.Start(); err != nil {
		log.Printf("storageAdapter.Start failed with: %s", err)
		return err
	}

	streamAdapter = &core.FileStreamAdapter{
		FilePath: inputFile,
		Codec:    codec,
		Output:   make(chan interface{}, 1024),
	}

	if err = streamAdapter.Start(); err != nil {
		log.Printf("streamAdapter.Start failed with: %s", err)
		return err
	}

	blockManager = &core.BlockManager{
		ID: "user_id-timestamp",

		MaxAge:  60 * 1000, // milliseconds
		MaxSize: 4096,      // rows

		PartitionColumn: "user_id",
		KeyColumn:       "timestamp",

		Input:    streamAdapter.Output,
		Output:   storageAdapter.Input,
		Finished: make(chan bool),
		Codec:    codec,
	}

	if err := blockManager.Start(); err != nil {
		log.Printf("Start failed with %s\n", err)
		return err
	}

	return nil
}

func stop() (err error) {
	// first such that the streamAdapter can stop message flow
	if err := streamAdapter.Stop(); err != nil {
		log.Printf("error stream storage adapter: %s\n", err)
		return err
	}

	// finish processing messages in channel and flush all in mem partitions to the storage adapter
	if err := blockManager.Stop(); err != nil {
		log.Printf("error stopping storage adapter: %s\n", err)
		return err
	}

	// finish any in progress writes and return
	if err := storageAdapter.Stop(); err != nil {
		log.Printf("error stopping storage adapter: %s\n", err)
		return err
	}

	return nil
}

func main() {
	fmt.Printf("%d\n", len(os.Args))

	inputFile := os.Args[1]
	fmt.Printf("input: %s\n", inputFile)

	outputPath := os.Args[2]
	fmt.Printf("output: %s\n", outputPath)

	if err := start(inputFile, outputPath); err != nil {
		panic(err)
	}

	<-blockManager.Finished

	/*
		if err := blockManager.CommitBlocks(true); err != nil {
			panic(err)
		}
	*/

	if err := stop(); err != nil {
		panic(err)
	}
}
