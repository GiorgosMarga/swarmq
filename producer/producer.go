package producer

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"net"

	"github.com/GiorgosMarga/swarmq/messages"
)

type Producer struct {
	brokerAddr string
	conn       net.Conn
	id         uint16
}

func NewProducer(brokerAddr string) (*Producer, error) {
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		return nil, err
	}

	id := uint16(rand.Intn(1024))

	binary.Write(conn, binary.BigEndian, id)

	return &Producer{
		brokerAddr: brokerAddr,
		conn:       conn,
		id:         id,
	}, nil

}

func (p *Producer) sendMessage(buf []byte) error {
	b := new(bytes.Buffer)
	binary.Write(b, binary.BigEndian, uint16(len(buf)))
	b.Write(buf)
	_, err := p.conn.Write(b.Bytes())
	return err
}

func (p *Producer) CreateNewTopic(topicKey string, numOfPartitions int) error {

	b := new(bytes.Buffer)

	binary.Write(b, binary.BigEndian, uint16(messages.CreateTopic))

	var topicLen uint16 = uint16(len(topicKey))
	binary.Write(b, binary.BigEndian, topicLen)

	binary.Write(b, binary.BigEndian, []byte(topicKey))

	binary.Write(b, binary.BigEndian, uint16(numOfPartitions))

	return p.sendMessage(b.Bytes())
}

func (p *Producer) Pub(topicKey string, key string, val []byte) error {
	b := new(bytes.Buffer)
	binary.Write(b, binary.BigEndian, uint16(messages.Pub))
	// write topic key
	var topicKeyLen uint16 = uint16(len(topicKey))
	binary.Write(b, binary.BigEndian, topicKeyLen)
	binary.Write(b, binary.BigEndian, []byte(topicKey))
	// write key
	var keyLen uint16 = uint16(len(key))
	binary.Write(b, binary.BigEndian, keyLen)
	binary.Write(b, binary.BigEndian, []byte(key))

	// write value
	var valLen uint16 = uint16(len(val))
	binary.Write(b, binary.BigEndian, valLen)
	binary.Write(b, binary.BigEndian, val)

	return p.sendMessage(b.Bytes())
}
