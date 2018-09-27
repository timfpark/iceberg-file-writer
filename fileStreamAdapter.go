package main

import (
	"bufio"
	"fmt"
	"os"

	goavro "gopkg.in/linkedin/goavro.v2"
)

type FileStreamAdapter struct {
	FilePath string

	Output chan interface{}
}

func (fsa *FileStreamAdapter) Start() (err error) {
	file, err := os.Open(fsa.FilePath)
	if err != nil {
		return err
	}

	reader := bufio.NewReader(file)
	ocf, err := goavro.NewOCFReader(reader)
	if err != nil {
		return err
	}

	go func(ocf *goavro.OCFReader) {
		for ocf.Scan() {
			native, err := ocf.Read()
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				continue
			}

			fsa.Output <- native
		}

		close(fsa.Output)
	}(ocf)

	return nil
}

func (fsa *FileStreamAdapter) Stop() (err error) {
	return nil
}
