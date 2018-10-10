package main

import (
	"log"
	"sync"
	"time"

	core "github.com/timfpark/iceberg-core"
	goavro "gopkg.in/linkedin/goavro.v2"
)

type BlockManager struct {
	ID string

	PartitionColumn string
	KeyColumn       string

	MaxAge  uint32 // in milliseconds
	MaxSize int    // in rows

	Input  chan interface{}
	Output chan *core.Block
	Codec  *goavro.Codec

	blocks       map[string]*core.Block // partitionKey -> block
	managerMutex sync.Mutex
}

func (bm *BlockManager) processRows() {
	go func() {
		for {
			row, more := <-bm.Input

			if !more {
				break
			}

			bm.managerMutex.Lock()

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
				block = core.NewBlock(partitionKey, bm.KeyColumn, bm.Codec)
				bm.blocks[partitionKey] = block
				log.Printf("creating block for partition key: %s uncommitted block count: %d\n", partitionKey, len(bm.blocks))
			}

			block.Write(row)

			if block.Length() >= bm.MaxSize {
				bm.commitBlock(block)
			}

			bm.managerMutex.Unlock()
		}
	}()
}

func (bm *BlockManager) commitBlock(block *core.Block) (err error) {
	log.Printf("Committing block PartitionKey: %+v StartingKey: %+v EndingKey: %+v with %d rows\n", block.PartitionKey, block.StartingKey, block.EndingKey, len(block.Rows))

	bm.Output <- block

	delete(bm.blocks, block.PartitionKey)

	log.Printf("uncommitted blocks remaining: %d\n", len(bm.blocks))

	return nil
}

func (bm *BlockManager) CommitBlocks(commitAll bool) (err error) {
	bm.managerMutex.Lock()

	log.Printf("Committing blocks, all: %+v", commitAll)
	blocksToCommit := []*core.Block{}

	for _, block := range bm.blocks {
		blockAgeMillis := uint32(time.Now().Sub(block.CreationTime).Seconds() * 1000.0)
		if commitAll || blockAgeMillis > bm.MaxAge {
			blocksToCommit = append(blocksToCommit, block)
		}
	}

	for _, block := range blocksToCommit {
		bm.commitBlock(block)
	}

	bm.managerMutex.Unlock()

	return nil
}

func (bm *BlockManager) checkBlockAges() {
	checkTicker := time.NewTicker(1 * time.Second)

	go func() {
		for _ = range checkTicker.C {
			bm.CommitBlocks(false)
		}
	}()
}

func (bm *BlockManager) Start() (err error) {
	bm.blocks = make(map[string]*core.Block)
	bm.managerMutex = sync.Mutex{}

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

	bm.CommitBlocks(true)

	return nil
}
