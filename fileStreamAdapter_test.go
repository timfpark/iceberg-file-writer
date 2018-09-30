package main

import (
	"log"
	"testing"
)

func TestFileStreamAdapter(t *testing.T) {
	log.Println("Starting TestFileStreamAdapter")

	output := make(chan interface{})

	fileStreamAdapter := &FileStreamAdapter{
		FilePath: "./test/data/userid1/timestamp/GEYDAMBQGA======-GEYDAMBQGA======.avro",
		Codec:    GetCodecFixture(),
		Output:   output,
	}

	err := fileStreamAdapter.Start()
	if err != nil {
		t.Errorf("fileStreamAdapter failed to start: %s", err)
	}

	rowCount := 0
	for {
		_, more := <-output
		if more {
			rowCount++
		} else {
			break
		}
	}

	if rowCount != 1 {
		t.Errorf("row count was not correct: %d vs. 1", rowCount)
	}

	fileStreamAdapter.Stop()

	log.Println("Finishing TestFileStreamAdapter")
}
