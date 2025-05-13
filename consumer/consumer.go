package consumer

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"net"

	"github.com/GiorgosMarga/swarmq/messages"
)

type Consumer struct {
	brokerAddr string
	groupId    string
	topicIds   []string
	id         uint16
	conn       net.Conn
}

func NewConsumer(brokerAddr string) (*Consumer, error) {
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		return nil, err
	}

	consumderId := uint16(rand.Intn(1024))

	if err := binary.Write(conn, binary.BigEndian, consumderId); err != nil {
		return nil, err
	}

	return &Consumer{
		brokerAddr: brokerAddr,
		id:         consumderId,
		conn:       conn,
	}, nil
}
func (c *Consumer) sendMessage(buf []byte) error {
	b := new(bytes.Buffer)
	binary.Write(b, binary.BigEndian, uint16(len(buf)))
	b.Write(buf)
	_, err := c.conn.Write(b.Bytes())
	return err
}
func (c *Consumer) Join(groupId string, topicIds []string) error {
	b := new(bytes.Buffer)

	binary.Write(b, binary.BigEndian, uint16(messages.Join))

	binary.Write(b, binary.BigEndian, c.id)

	binary.Write(b, binary.BigEndian, uint16(len(groupId)))
	binary.Write(b, binary.BigEndian, []byte(groupId))

	binary.Write(b, binary.BigEndian, uint16(len(topicIds)))
	for _, topicId := range topicIds {
		binary.Write(b, binary.BigEndian, uint16(len(topicId)))
		binary.Write(b, binary.BigEndian, []byte(topicId))
	}

	c.groupId = groupId
	c.topicIds = topicIds
	return c.sendMessage(b.Bytes())
}

func (c *Consumer) Read(partition int, offset int, max int) ([][]byte, error) {
	b := new(bytes.Buffer)

	binary.Write(b, binary.BigEndian, uint16(messages.Sub))
	binary.Write(b, binary.BigEndian, c.id)

	// group id is optional
	binary.Write(b, binary.BigEndian, uint16(len(c.groupId)))
	binary.Write(b, binary.BigEndian, []byte(c.groupId))

	binary.Write(b, binary.BigEndian, uint16(len(c.topicIds[0])))
	binary.Write(b, binary.BigEndian, []byte(c.topicIds[0]))

	// parition and offset are optionals, -1 if not set
	binary.Write(b, binary.BigEndian, int32(partition))
	binary.Write(b, binary.BigEndian, int32(offset))

	binary.Write(b, binary.BigEndian, uint16(max))

	if err := c.sendMessage(b.Bytes()); err != nil {
		return nil, err
	}

	var (
		dataLength uint16
		dataSize   uint16
	)

	binary.Read(c.conn, binary.BigEndian, &dataLength)

	data := make([][]byte, dataLength)

	for i := range dataLength {
		binary.Read(c.conn, binary.BigEndian, &dataSize)
		d := make([]byte, dataSize)
		binary.Read(c.conn, binary.BigEndian, d)
		data[i] = d
	}

	return data, nil
}

func (c *Consumer) Close() error {

	b := new(bytes.Buffer)
	binary.Write(b, binary.BigEndian, uint16(messages.Close))
	binary.Write(b, binary.BigEndian, c.id)
	binary.Write(b, binary.BigEndian, uint16(len(c.groupId)))
	binary.Write(b, binary.BigEndian, []byte(c.groupId))

	binary.Write(b, binary.BigEndian, uint16(len(c.topicIds)))
	for _, topicId := range c.topicIds {
		binary.Write(b, binary.BigEndian, uint16(len(topicId)))
		binary.Write(b, binary.BigEndian, []byte(topicId))
	}

	if err := c.sendMessage(b.Bytes()); err != nil {
		return err
	}

	if err := c.conn.Close(); err != nil {
		return err
	}
	return nil
}
