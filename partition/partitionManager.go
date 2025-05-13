package partition

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"
)

var (
	ErrDuplicateKey = errors.New("duplicate key")
	ErrNoPartition  = errors.New("partition not found")
)

type PartitionManager struct {
	topicKey        string
	NumOfPartitions int
	partitions      []*Partition
}

func NewPartitionManager(numberOfPartitions int, topicKey string) *PartitionManager {

	partitions := make([]*Partition, numberOfPartitions)
	for idx := range numberOfPartitions {
		partitions[idx] = NewPartition()
	}
	return &PartitionManager{
		partitions:      partitions,
		topicKey:        topicKey,
		NumOfPartitions: numberOfPartitions,
	}
}
func (b *PartitionManager) getPartitionId(key string, partitions int) uint32 {
	hash := md5.Sum([]byte(key))
	return binary.BigEndian.Uint32(hash[:4]) % uint32(partitions)
}

func (p *Partition) addData(key string, val []byte) error {
	p.dataMtx.Lock()
	defer p.dataMtx.Unlock()

	if _, ok := p.keys[key]; ok {
		return ErrDuplicateKey
	}

	p.data = append(p.data, val)
	p.keys[key] = len(p.data) - 1
	p.timestamps = append(p.timestamps, time.Now())
	return nil
}
func (p *PartitionManager) AddData(key string, val []byte) error {

	partitionId := p.getPartitionId(key, p.NumOfPartitions)

	partition := p.partitions[partitionId]

	if err := partition.addData(key, val); err != nil {
		return err
	}
	f, err := os.OpenFile(fmt.Sprintf("data/%s_%d/%d", p.topicKey, p.NumOfPartitions, partitionId), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
	}
	defer f.Close()

	_, err = f.Write([]byte(fmt.Sprintf("%s-%s\n", key, string(val))))
	if err != nil {
		return err
	}

	return nil
}

func (p *PartitionManager) AddDataFromLog(key string, val []byte) error {
	partitionId := p.getPartitionId(key, p.NumOfPartitions)
	partition := p.partitions[partitionId]
	return partition.addData(key, val)
}

func (p *PartitionManager) ReadData(partitionId, offset, max int) ([][]byte, error) {
	if partitionId < 0 || partitionId >= p.NumOfPartitions {
		return nil, ErrNoPartition
	}
	partition := p.partitions[partitionId]
	return partition.ReadData(offset, max)
}

func (p *PartitionManager) Cleanup() []int {
	deleted := make([]int, len(p.partitions))

	for idx := range p.partitions {
		go func(idx int) {
			lastDeleted := p.partitions[idx].cleanup(1)
			deleted[idx] = lastDeleted
		}(idx)
	}
	return deleted
}
