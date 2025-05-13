package groupmanager

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrNoConsumer  = errors.New("consumer not found")
	ErrNoPartition = errors.New("partition not found")
)

type GroupManager struct {
	partitions         int
	partitionToOffset  map[int]int
	memberToPartitions map[int][]int
	mtx                *sync.Mutex
}

func NewGroupManager(partitions int) *GroupManager {
	partitionToOffset := make(map[int]int, partitions)
	for idx := range partitions {
		partitionToOffset[idx] = 0
	}
	return &GroupManager{
		partitionToOffset:  partitionToOffset,
		memberToPartitions: make(map[int][]int),
		partitions:         partitions,
		mtx:                &sync.Mutex{},
	}
}

func (gm *GroupManager) GetOffsetFromPartition(partitionId int) (int32, error) {
	gm.mtx.Lock()
	defer gm.mtx.Unlock()
	offset, ok := gm.partitionToOffset[partitionId]
	if !ok {
		return -1, ErrNoPartition
	}
	return int32(offset), nil
}
func (gm *GroupManager) UpdateOffset(partitionId int, offset int, add bool) {
	gm.mtx.Lock()
	defer gm.mtx.Unlock()
	if add {

		gm.partitionToOffset[partitionId] += offset
	} else {
		gm.partitionToOffset[partitionId] -= offset
	}

}
func (gm *GroupManager) GetPartitionsFromConsumerId(consumerId int) ([]int, error) {
	gm.mtx.Lock()
	defer gm.mtx.Unlock()
	partitions, ok := gm.memberToPartitions[consumerId]
	if !ok {
		return nil, ErrNoConsumer
	}
	return partitions, nil
}

func (gm *GroupManager) RemoveConsumer(consumerId int) {
	gm.mtx.Lock()
	defer gm.mtx.Unlock()

	delete(gm.memberToPartitions, consumerId)

	totalConsumers := len(gm.memberToPartitions)
	if totalConsumers == 0 {
		fmt.Println("No consumers left")
		return
	}
	gm.splitPartitions(totalConsumers)
	for k, v := range gm.memberToPartitions {
		fmt.Println(k, v)
	}

}
func (gm *GroupManager) splitPartitions(members int) {

	partitionPerMember := gm.partitions / members
	fmt.Println(partitionPerMember)
	remainder := gm.partitions % members

	idx := 0
	for memberId := range gm.memberToPartitions {
		partitions := make([]int, partitionPerMember)
		for j := range partitionPerMember {
			partitions[j] = idx*partitionPerMember + j
		}
		idx++
		gm.memberToPartitions[memberId] = partitions
	}

	for idx := range remainder {
		gm.memberToPartitions[idx] = append(gm.memberToPartitions[idx], gm.partitions-idx-1)
	}

}
func (gm *GroupManager) AddConsumer(consumerId int) {
	gm.mtx.Lock()
	defer gm.mtx.Unlock()

	totalConsumers := len(gm.memberToPartitions) + 1

	gm.memberToPartitions[consumerId] = make([]int, 0)

	gm.splitPartitions(totalConsumers)
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
