package main

import (
	"log"
	"time"

	goavro "gopkg.in/linkedin/goavro.v2"
)

type BlockManager struct {
	ID string

	PartitionColumn string
	KeyColumn       string

	MaxAge  uint32 // in milliseconds
	MaxSize int    // in rows

	Input  chan interface{}
	Output chan *Block
	Codec  *goavro.Codec

	blocks map[string]*Block // partitionKey -> block
}

func (bm *BlockManager) processRows() {
	go func() {
		for {
			row, more := <-bm.Input

			if !more {
				break
			}

			rowMap := row.(map[string]interface{})

			var partitionKey string
			switch t := rowMap[bm.PartitionColumn].(type) {
			case map[string]interface{}:
				columnMap := rowMap[bm.PartitionColumn].(map[string]interface{})
				for _, value := range columnMap {
					partitionKey = value.(string)
				}
			case string:
				partitionKey = rowMap[bm.PartitionColumn].(string)
			default:
				log.Printf("processRows unknown type: %T", t)
			}

			block, exists := bm.blocks[partitionKey]
			if !exists {
				block = NewBlock(partitionKey, bm.KeyColumn, bm.Codec)
				bm.blocks[partitionKey] = block
			}

			block.Write(row)

			if block.Length() >= bm.MaxSize {
				bm.commitBlock(block)
			}
		}
	}()
}

func (bm *BlockManager) commitBlock(block *Block) (err error) {
	log.Printf("Committing block PartitionKey: %+v StartingKey: %+v EndingKey: %+v with %d rows\n", block.PartitionKey, block.StartingKey, block.EndingKey, len(block.Rows))

	bm.Output <- block

	delete(bm.blocks, block.PartitionKey)
	return nil
}

func (bm *BlockManager) commitBlocks(commitAll bool) (err error) {
	for _, block := range bm.blocks {
		blockAgeMillis := uint32(time.Now().Sub(block.CreationTime).Seconds() * 1000.0)
		if commitAll || blockAgeMillis > bm.MaxAge {
			bm.commitBlock(block)
		}
	}

	return nil
}

func (bm *BlockManager) checkBlockAges() {
	checkRate := time.Duration(bm.MaxAge / 4)
	checkTicker := time.NewTicker(checkRate)

	go func() {
		for _ = range checkTicker.C {
			bm.commitBlocks(false)
		}
	}()
}

func (bm *BlockManager) Start() (err error) {
	bm.blocks = make(map[string]*Block)

	bm.processRows()
	bm.checkBlockAges()

	return nil
}

func (bm *BlockManager) Stop() (err error) {
	close(bm.Input)
	log.Println("stopping BlockManager")

	// wait for completion
	ttl := 100
	for len(bm.Input) > 0 && ttl > 0 {
		log.Printf("waiting for BlockManager to stop:  %d remaining Blocks, %d TTL\n", len(bm.Input), ttl)

		ttl--
		time.Sleep(200 * time.Millisecond)
	}

	bm.commitBlocks(true)

	return nil
}
