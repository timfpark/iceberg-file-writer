package main

import (
	"encoding/base32"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"
)

type Block struct {
	CreationTime time.Time
	Rows         []interface{}
	PartitionKey string

	BlockManager *BlockManager

	StartingKey interface{}
	EndingKey   interface{}
}

func NewBlock(blockManager *BlockManager, partitionKey string) (block *Block) {
	return &Block{
		BlockManager: blockManager,
		CreationTime: time.Now(),
		PartitionKey: partitionKey,
		Rows:         []interface{}{},
	}
}

func (b *Block) updateInt64KeyRange(keyValue int64) {
	if b.StartingKey == nil || b.StartingKey.(int64) > keyValue {
		b.StartingKey = keyValue
	}

	if b.EndingKey == nil || b.EndingKey.(int64) > keyValue {
		b.EndingKey = keyValue
	}
}

func (b *Block) updateKeyRange(row interface{}) (err error) {
	rowMap := row.(map[string]interface{})

	switch t := rowMap[b.BlockManager.KeyColumn].(type) {
	case int64:
		b.updateInt64KeyRange(rowMap[b.BlockManager.KeyColumn].(int64))
	default:
		errorText := fmt.Sprintf("updateKeyRange: unsupported type %T", t)
		return errors.New(errorText)
	}

	return nil
}

func base32Encode(key interface{}) string {
	var s string

	switch t := key.(type) {
	case string:
		s = key.(string)
	case int64:
		s = strconv.FormatInt(key.(int64), 10)
	default:
		log.Printf("block: type %T not supported", t)
		return ""
	}

	return base32.StdEncoding.EncodeToString([]byte(s))
}

func (b *Block) GetStartingKeyAsBase32() string {
	return base32Encode(b.StartingKey)
}

func (b *Block) GetEndingKeyAsBase32() string {
	return base32Encode(b.EndingKey)
}

func (b *Block) Write(row interface{}) (err error) {
	b.updateKeyRange(row)

	b.Rows = append(b.Rows, row)

	return nil
}

func (b *Block) Length() int {
	return len(b.Rows)
}

func (b *Block) GetRowsAsBinary() (blockBinary []byte) {
	blockBinary = []byte{}
	for _, row := range b.Rows {
		rowBinary, err := b.BlockManager.Codec.BinaryFromNative(nil, row)
		if err != nil {
			fmt.Println(err)
		}

		blockBinary = append(blockBinary, rowBinary...)
	}

	return blockBinary
}
