package main

import (
	"fmt"
	"testing"
)

func TestFileStreamAdapter(t *testing.T) {
	fmt.Println("Starting TestFileStreamAdapter")

	output := make(chan interface{})

	fileStreamAdapter := &FileStreamAdapter{
		FilePath: "/Users/tim/Downloads/part-00000-tid-6228055414261845306-6bc31bde-04c3-4d2a-ba88-86707d242532-0-c000.avro",
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

	if rowCount != 1171839 {
		t.Errorf("row count was not correct: %d", rowCount)
	}

	fmt.Println("Finishing TestFileStreamAdapter")
}
