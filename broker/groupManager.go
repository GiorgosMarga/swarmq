package broker

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrNoPartition = errors.New("partition not found")
	ErrNoConsumer  = errors.New("consumer not found")
)

type GroupManager struct {
	partitions           int
	partitionToOffset    map[int]int
	memberToPartitions   map[int][]int
	consumersIdToLocalId map[int]int
	mtx                  *sync.Mutex
}

func NewGroupManager(partitions int) *GroupManager {
	partitionToOffset := make(map[int]int, partitions)
	for idx := range partitions {
		partitionToOffset[idx] = 0
	}
	return &GroupManager{
		partitionToOffset:    partitionToOffset,
		memberToPartitions:   make(map[int][]int),
		partitions:           partitions,
		mtx:                  &sync.Mutex{},
		consumersIdToLocalId: make(map[int]int),
	}
}

func (gm *GroupManager) getOffsetFromPartition(partitionId int) (int32, error) {
	gm.mtx.Lock()
	defer gm.mtx.Unlock()
	offset, ok := gm.partitionToOffset[partitionId]
	if !ok {
		return -1, ErrNoPartition
	}
	return int32(offset), nil
}
func (gm *GroupManager) updateOffset(partitionId int, offset int) {
	gm.mtx.Lock()
	defer gm.mtx.Unlock()

	gm.partitionToOffset[partitionId] += offset
}
func (gm *GroupManager) getPartitionsFromConsumerId(consumerId int) ([]int, error) {
	gm.mtx.Lock()
	defer gm.mtx.Unlock()
	localId, ok := gm.consumersIdToLocalId[consumerId]
	if !ok {
		return nil, ErrNoConsumer
	}
	partitions, ok := gm.memberToPartitions[localId]
	if !ok {
		return nil, ErrNoConsumer
	}
	return partitions, nil
}

func (gm *GroupManager) addConsumer(consumerId int) {
	gm.mtx.Lock()
	defer gm.mtx.Unlock()

	newConsumerId := len(gm.memberToPartitions) + 1
	totalConsumers := len(gm.memberToPartitions) + 1
	gm.consumersIdToLocalId[consumerId] = newConsumerId

	gm.memberToPartitions[newConsumerId] = make([]int, 0)

	gm.memberToPartitions = make(map[int][]int)
	for i := range gm.partitions {
		gm.memberToPartitions[i%totalConsumers+1] = append(gm.memberToPartitions[i%totalConsumers+1], i)
	}

	for k, v := range gm.memberToPartitions {
		fmt.Println(k, v)
	}

}

// func (gm *GroupManager) readData(consumerId int) {
// 	gm.mtx.Lock()
// 	defer gm.mtx.Unlock()

// 	totalConsumers := len(gm.memberToPartitions) + 1
// 	gm.consumersIdToLocalId[consumerId] = totalConsumers

// 	gm.memberToPartitions[totalConsumers] = make([]int, 0)

// 	for i := range gm.partitions {
// 		gm.memberToPartitions[i%totalConsumers] = append(gm.memberToPartitions[i%totalConsumers], i)
// 	}

// }
