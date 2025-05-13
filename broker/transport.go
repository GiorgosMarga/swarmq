package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

var (
	ErrNoPeer = errors.New("peer not found")
)

type Transport struct {
	ln      net.Listener
	address string
	msgChan chan []byte
	peers   map[uint16]net.Conn
}

func NewTransport(address string) *Transport {
	return &Transport{
		address: address,
		msgChan: make(chan []byte),
		peers:   make(map[uint16]net.Conn),
	}
}

func (t *Transport) Consume() []byte {
	return <-t.msgChan
}

func (t *Transport) Start() error {
	ln, err := net.Listen("tcp", t.address)

	if err != nil {
		return err
	}
	defer ln.Close()
	t.ln = ln
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			continue
		}
		go t.handleConn(conn)
	}
}

func (t *Transport) sendData(id uint16, data []byte) error {
	conn, ok := t.peers[id]
	if !ok {
		return ErrNoPeer
	}

	_, err := conn.Write(data)
	return err
}
func (t *Transport) handleConn(conn net.Conn) error {
	defer conn.Close()
	var peerId uint16

	err := binary.Read(conn, binary.BigEndian, &peerId)
	if err != nil {
		return err
	}
	t.peers[peerId] = conn

	var messageSize uint16
	for {
		err := binary.Read(conn, binary.BigEndian, &messageSize)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return err
			}
			fmt.Println("Error reading size:", err)
			continue
		}
		b := make([]byte, messageSize)
		_, err = io.ReadFull(conn, b)
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				return err
			default:
				fmt.Println(err)
				continue
			}
		}
		t.msgChan <- b

	}
}
