package broker

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
)

var (
	ErrDuplicateKey = errors.New("duplicate key")
)

type PartitionManager struct {
	topicKey        string
	numOfPartitions int
	keys            map[uint32]map[string]int
	partitions      map[uint32][][]byte
	dataMtx         *sync.Mutex
}

func NewPartitionManager(numberOfPartitions int, topicKey string) *PartitionManager {

	partitions := make(map[uint32][][]byte)
	for idx := range numberOfPartitions {
		partitions[uint32(idx)] = make([][]byte, 0, 1024)
	}

	keys := make(map[uint32]map[string]int)
	for idx := range numberOfPartitions {
		keys[uint32(idx)] = make(map[string]int)
	}
	return &PartitionManager{
		partitions:      partitions,
		keys:            keys,
		dataMtx:         &sync.Mutex{},
		topicKey:        topicKey,
		numOfPartitions: numberOfPartitions,
	}
}
func (b *PartitionManager) getPartitionId(key string, partitions int) uint32 {
	hash := md5.Sum([]byte(key))
	return binary.BigEndian.Uint32(hash[:4]) % uint32(partitions)
}

func (p *PartitionManager) AddData(key string, val []byte) error {
	p.dataMtx.Lock()
	defer p.dataMtx.Unlock()

	partitionId := p.getPartitionId(key, p.numOfPartitions)

	partition := p.partitions[partitionId]

	if _, ok := p.keys[partitionId][key]; ok {
		return ErrDuplicateKey
	}

	p.partitions[partitionId] = append(p.partitions[partitionId], val)
	p.keys[partitionId][key] = len(partition) - 1

	f, err := os.OpenFile(fmt.Sprintf("data/%s_%d/%d", p.topicKey, p.numOfPartitions, partitionId), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
	}

	_, err = f.Write([]byte(fmt.Sprintf("%s-%s\n", key, string(val))))
	if err != nil {
		fmt.Println(err)
	}

	return nil
}

func (p *PartitionManager) AddDataFromLog(key string, val []byte) error {
	p.dataMtx.Lock()
	defer p.dataMtx.Unlock()

	partitionId := p.getPartitionId(key, p.numOfPartitions)

	partition := p.partitions[partitionId]

	if _, ok := p.keys[partitionId][key]; ok {
		return ErrDuplicateKey
	}

	partition = append(partition, val)
	p.keys[partitionId][key] = len(partition) - 1
	return nil
}

func (p *PartitionManager) ReadData(partitionId, offset, max int) ([][]byte, error) {
	data, ok := p.partitions[uint32(partitionId)]
	if !ok {
		return nil, ErrNoPartition
	}
	if offset >= len(data) {
		return nil, io.EOF
	}
	end := math.Min(float64(offset+max), float64(offset+len(data)))
	return data[offset:int(end)], nil
}
