package main

import (
	"encoding/base32"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	goavro "gopkg.in/linkedin/goavro.v2"
)

type Block struct {
	Codec        *goavro.Codec
	CreationTime time.Time
	Rows         []interface{}
	PartitionKey string
	KeyColumn    string
	StartingKey  interface{}
	EndingKey    interface{}
}

func NewBlock(partitionKey string, keyColumn string, codec *goavro.Codec) (block *Block) {
	return &Block{
		Codec:        codec,
		CreationTime: time.Now(),
		PartitionKey: partitionKey,
		KeyColumn:    keyColumn,
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

	switch t := rowMap[b.KeyColumn].(type) {
	case int64:
		b.updateInt64KeyRange(rowMap[b.KeyColumn].(int64))
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

func (b *Block) Write(row interface{}) {
	b.updateKeyRange(row)

	b.Rows = append(b.Rows, row)
}

func (b *Block) Length() int {
	return len(b.Rows)
}

func (b *Block) GetRowsAsBinary() (blockBinary []byte) {
	blockBinary = []byte{}
	for _, row := range b.Rows {
		fmt.Printf("%+v\n", row)
		rowBinary, err := b.Codec.BinaryFromNative(nil, row)
		if err != nil {
			fmt.Printf("GetRowsAsBinary: %s\n", err)
		}

		blockBinary = append(blockBinary, rowBinary...)
	}

	return blockBinary
}

func (b *Block) GetFilename() string {
	startingKeyAsBase32 := b.GetStartingKeyAsBase32()
	endingKeyAsBase32 := b.GetEndingKeyAsBase32()

	return fmt.Sprintf("%s-%s", startingKeyAsBase32, endingKeyAsBase32)
}

func (b *Block) RowsForKeyRange(startKey interface{}, endKey interface{}) (rowsInRange []interface{}) {
	rowsInRange = make([]interface{}, 0)

	for _, row := range b.Rows {
		rowMap := row.(map[string]interface{})
		key := rowMap[b.KeyColumn]

		var intersects bool
		switch t := key.(type) {
		case int64:
			intersects = (key.(int64) >= startKey.(int64) && key.(int64) <= endKey.(int64))
		case string:
			intersects = (key.(string) >= startKey.(string) && key.(string) <= endKey.(string))
		default:
			fmt.Printf("blockFilenameIntersectsKeyRange: Type %T not yet supported", t)
			intersects = false
		}

		if intersects {
			rowsInRange = append(rowsInRange, row)
		}
	}

	return
}

func filenameIntersectsKeyRange(blockFilename string, startKey interface{}, endKey interface{}) (intersects bool) {
	var blockStartKeyString string
	var blockEndKeyString string
	_, err := fmt.Sscanf(blockFilename, "%s-%s", &blockStartKeyString, &blockEndKeyString)
	if err != nil {
		return false
	}

	blockStartKey, err := convertBlockKeyToType(startKey, blockStartKeyString)
	if err != nil {
		return false
	}

	blockEndKey, err := convertBlockKeyToType(endKey, blockEndKeyString)
	if err != nil {
		return false
	}

	switch t := startKey.(type) {
	case int64:
		intersects = !(startKey.(int64) > blockEndKey.(int64) || endKey.(int64) < blockStartKey.(int64))
	case string:
		intersects = !(startKey.(string) > blockEndKey.(string) || endKey.(string) < blockStartKey.(string))
	default:
		fmt.Printf("blockFilenameIntersectsKeyRange: Type %T not yet supported", t)
		intersects = false
	}

	return
}

func IntersectingBlockFilenames(blockFilenames []string, startKey interface{}, endKey interface{}) (intersectingBlockFilenames []string) {
	intersectingBlockFilenames = make([]string, len(blockFilenames))

	for _, blockFilename := range blockFilenames {
		if filenameIntersectsKeyRange(blockFilename, startKey, endKey) {
			intersectingBlockFilenames = append(intersectingBlockFilenames, blockFilename)
		}
	}

	return
}
