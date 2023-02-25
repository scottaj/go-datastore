package client

import (
	"bufio"
	"datastore/wire"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

type Client struct {
	address string
	port    int
	wire    wire.Protocol
}

func New(address string, port int) Client {
	return Client{
		address: address,
		port:    port,
		wire:    wire.Protocol{},
	}
}

func (c *Client) Read(key string) (string, bool, error) {
	readCommand, err := c.wire.EncodeMessage(wire.READ, key)
	if err != nil {
		return "", false, err
	}

	responseCommand, responseMessage, err := c.connectAndSendMessage(readCommand)
	if err != nil {
		return "", false, err
	}

	switch responseCommand {
	case wire.NULL:
		return "", false, nil
	case wire.ERR:
		err := c.wire.DecodeError(responseMessage)
		return "", false, err
	case wire.READ:
		value, err := c.wire.DecodeRead(responseMessage)
		if err != nil {
			return "", false, err
		}

		return value, true, nil
	default:
		return "", false, errors.New("invalid response for READ command")
	}
}

func (c *Client) Insert(key string, value string) (bool, error) {
	insertCommand, err := c.wire.EncodeMessage(wire.INSERT, key, value)
	if err != nil {
		return false, err
	}

	responseCommand, responseMessage, err := c.connectAndSendMessage(insertCommand)
	if err != nil {
		return false, err
	}

	switch responseCommand {
	case wire.NULL:
		return false, nil
	case wire.ERR:
		err := c.wire.DecodeError(responseMessage)
		return false, err
	case wire.ACK:
		return true, nil
	default:
		return false, errors.New("invalid response for INSERT command")
	}
}

// TODO, this doesn't do any kind of connection pooling
func (c *Client) connectAndSendMessage(message []byte) (wire.Command, []byte, error) {
	connection, err := net.Dial("tcp", fmt.Sprintf("%s:%d", c.address, c.port))
	if err != nil {
		return wire.ERR, nil, err
	}
	defer connection.Close()
	err = connection.SetDeadline(time.Now().Add(time.Second * 10))
	if err != nil {
		return wire.ERR, nil, err
	}

	_, err = connection.Write(message)
	if err != nil {
		return wire.ERR, nil, err
	}

	// https://stackoverflow.com/a/47585913
	connectionBuffer := bufio.NewReader(connection)
	messageSizeBytes, err := connectionBuffer.Peek(4)
	if err != nil {
		return wire.ERR, nil, err
	}
	if len(messageSizeBytes) != 4 {
		return wire.ERR, nil, err
	}

	messageSize := binary.LittleEndian.Uint32(messageSizeBytes[:4])
	responseMessage := make([]byte, messageSize)
	_, err = io.ReadFull(connectionBuffer, responseMessage)
	if err != nil {
		return wire.ERR, nil, err
	}

	responseCommand, err := c.wire.DecipherCommand(responseMessage)
	if err != nil {
		return wire.ERR, nil, err
	}

	return responseCommand, responseMessage, nil
}