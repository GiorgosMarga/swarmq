package broker

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/GiorgosMarga/swarmq/messages"
)

var (
	ErrTopicNotFound = errors.New("topic could not be found")
)

type Broker struct {
	topics    map[string]*PartitionManager
	Transport *Transport
	groups    map[string]*GroupManager
}

func NewBroker(brokerAddr string) *Broker {
	return &Broker{
		topics:    make(map[string]*PartitionManager),
		Transport: NewTransport(brokerAddr),

		// map[groupid]map[topicId][offset1(par1),offset2(par2),offset3(par3)]
		groups: make(map[string]*GroupManager),
	}
}
func (b *Broker) splitName(n string) (string, int) {
	for idx := len(n) - 1; idx >= 0; idx-- {
		if n[idx] == '_' {
			partitionId, _ := strconv.Atoi(n[idx+1:])
			return n[:idx], partitionId
		}
	}
	return "", -1
}
func (b *Broker) retrieveState() error {
	entries, err := os.ReadDir("data")
	if err != nil {
		return err
	}

	for _, entry := range entries {

		topicKey, numOfPartitions := b.splitName(entry.Name())
		if topicKey == "" && numOfPartitions == -1 {
			fmt.Println("Error with folder format")
			continue
		}
		b.addTopic(topicKey, numOfPartitions)

		topicEntries, err := os.ReadDir(fmt.Sprintf("data/%s", entry.Name()))
		if err != nil {
			fmt.Println(err)
			continue
		}

		for _, topicEntry := range topicEntries {
			if !topicEntry.IsDir() {
				f, err := os.ReadFile(filepath.Join("data", entry.Name(), topicEntry.Name()))
				if err != nil {
					return err
				}

				lines := strings.Split(string(f), "\n")

				for _, line := range lines {
					if len(line) == 0 {
						continue
					}
					splitted := strings.Split(line, "-")
					if len(splitted) != 2 {
						log.Fatal("invalid formatting", splitted)
					}
					key, val := splitted[0], splitted[1]
					b.addValueFromLog(topicKey, key, []byte(val))
				}

			}
		}
	}
	return nil
}

func (b *Broker) addTopic(topicKey string, numOfPartitions int) {

	if _, ok := b.topics[topicKey]; ok {
		return
	}

	b.topics[topicKey] = NewPartitionManager(numOfPartitions, topicKey)
	fmt.Printf("Added (%s) with (%d) partitions.\n", topicKey, numOfPartitions)

	dirName := fmt.Sprintf("data/%s_%d", topicKey, numOfPartitions)
	_, err := os.Stat(dirName)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.Mkdir(dirName, 0755); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal(err)
		}
	}

}

func (b *Broker) addValue(topicKey string, key string, val []byte) error {
	partitionManager, ok := b.topics[topicKey]
	if !ok {
		return ErrTopicNotFound
	}

	if err := partitionManager.AddData(key, val); err != nil {
		return err
	}
	fmt.Printf("Added (%s) to topic (%s) val: %+v\n", key, topicKey, string(val))
	return nil
}

func (b *Broker) addValueFromLog(topicKey string, key string, val []byte) error {
	partitionManager, ok := b.topics[topicKey]
	if !ok {
		return ErrTopicNotFound
	}

	if err := partitionManager.AddData(key, val); err != nil {
		return err
	}
	fmt.Printf("Added (%s) to topic (%s) val: %+v\n", key, topicKey, string(val))
	return nil
}
func (b *Broker) Start() {
	go b.Transport.Start()
	if err := b.retrieveState(); err != nil {
		fmt.Println(err)
	}

	time.Sleep(100 * time.Millisecond)

	for {
		msg := b.Transport.Consume()

		buf := bytes.NewBuffer(msg)
		var msgType uint16
		binary.Read(buf, binary.BigEndian, &msgType)
		switch msgType {
		case messages.CreateTopic:
			go b.handleCreateTopicMessage(buf)
		case messages.Pub:
			go b.handlePubMessage(buf)
		case messages.Join:
			go b.handleJoinMessage(buf)
		case messages.Sub:
			go b.handleReadMessage(buf)
		default:
			fmt.Println("Unknown")
		}

	}
}

func (b *Broker) handleJoinMessage(buf *bytes.Buffer) error {
	fmt.Println("Handling join message")

	var (
		consumerId  uint16
		groupIdLen  uint16
		numOfTopics uint16
		topicKeyLen uint16
	)

	// get consumers id
	binary.Read(buf, binary.BigEndian, &consumerId)

	// get group id

	binary.Read(buf, binary.BigEndian, &groupIdLen)

	groupId := make([]byte, groupIdLen)
	binary.Read(buf, binary.BigEndian, &groupId)

	// get topics

	binary.Read(buf, binary.BigEndian, &numOfTopics)

	for range numOfTopics {
		binary.Read(buf, binary.BigEndian, &topicKeyLen)
		topicKey := make([]byte, topicKeyLen)
		binary.Read(buf, binary.BigEndian, &topicKey)
		if err := b.joinTopic(int(consumerId), string(topicKey), string(groupId)); err != nil {
			return err
		}
	}
	return nil
}
func (b *Broker) joinTopic(consumerId int, topicKey, groupId string) error {
	partitionManager, ok := b.topics[topicKey]
	if !ok {
		return ErrTopicNotFound
	}

	topicGroupId := fmt.Sprintf("%s_%s", topicKey, groupId)

	if _, ok := b.groups[topicGroupId]; !ok {
		b.groups[topicGroupId] = NewGroupManager(partitionManager.numOfPartitions)
	}

	b.groups[topicGroupId].addConsumer(consumerId)
	return nil
}
func (b *Broker) handleCreateTopicMessage(buf *bytes.Buffer) error {
	var topicKeyLen uint16
	binary.Read(buf, binary.BigEndian, &topicKeyLen)

	topicKey := make([]byte, topicKeyLen)
	binary.Read(buf, binary.BigEndian, &topicKey)

	var numOfPartitions uint16
	binary.Read(buf, binary.BigEndian, &numOfPartitions)

	b.addTopic(string(topicKey), int(numOfPartitions))
	return nil
}

func (b *Broker) handlePubMessage(buf *bytes.Buffer) error {
	var topicKeyLen uint16
	binary.Read(buf, binary.BigEndian, &topicKeyLen)

	topicKey := make([]byte, topicKeyLen)
	binary.Read(buf, binary.BigEndian, &topicKey)

	var keyLen uint16
	binary.Read(buf, binary.BigEndian, &keyLen)

	key := make([]byte, keyLen)
	binary.Read(buf, binary.BigEndian, &key)

	var valLen uint16
	binary.Read(buf, binary.BigEndian, &valLen)

	val := make([]byte, valLen)
	binary.Read(buf, binary.BigEndian, &val)

	return b.addValue(string(topicKey), string(key), val)
}

func (b *Broker) handleReadMessage(buf *bytes.Buffer) error {
	var (
		partition  int32
		offset     int32
		consumerId uint16
		groupIdLen uint16
		topicLen   uint16
		max        uint16
		err        error
	)

	binary.Read(buf, binary.BigEndian, &consumerId)

	binary.Read(buf, binary.BigEndian, &groupIdLen)
	groupId := make([]byte, groupIdLen)
	binary.Read(buf, binary.BigEndian, groupId)

	binary.Read(buf, binary.BigEndian, &topicLen)
	topic := make([]byte, topicLen)
	binary.Read(buf, binary.BigEndian, topic)

	binary.Read(buf, binary.BigEndian, &partition)
	binary.Read(buf, binary.BigEndian, &offset)
	binary.Read(buf, binary.BigEndian, &max)

	id := fmt.Sprintf("%s_%s", topic, groupId)
	if partition == -1 {
		groupManager, ok := b.groups[id]
		if !ok {
			return ErrNoConsumer
		}
		partitions, err := groupManager.getPartitionsFromConsumerId(int(consumerId))
		if err != nil {
			fmt.Println("Error:", err)
			return err
		}
		data := make([][]byte, 0)
		for _, partition := range partitions {
			offset, err := groupManager.getOffsetFromPartition(partition)
			if err != nil {
				continue
			}
			d, err := b.topics[string(topic)].ReadData(partition, int(offset), int(max))

			if err != nil {
				continue
			}
			groupManager.updateOffset(partition, int(max))
			data = append(data, d...)
		}
		return b.sendData(consumerId, data)
	}

	if offset == -1 {
		offset, err = b.groups[id].getOffsetFromPartition(int(partition))
		if err != nil {
			return err
		}
	}
	d, err := b.topics[string(topic)].ReadData(int(partition), int(offset), int(max))
	if err != nil {
		return err
	}
	return b.sendData(consumerId, d)

}

func (b *Broker) sendData(consumerId uint16, data [][]byte) error {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, uint16(len(data)))

	for _, d := range data {
		binary.Write(buf, binary.BigEndian, uint16(len(d)))
		binary.Write(buf, binary.BigEndian, d)
	}

	return b.Transport.sendData(consumerId, buf.Bytes())

}
