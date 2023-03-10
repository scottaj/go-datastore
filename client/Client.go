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
		value, err := c.wire.DecodeReadResponse(responseMessage)
		if err != nil {
			return "", false, err
		}

		return value, true, nil
	default:
		return "", false, errors.New(fmt.Sprintf("invalid response for READ command %q", responseCommand))
	}
}

func (c *Client) Insert(key string, value string) (bool, error) {
	return c.executeAckOrNullCommand(wire.INSERT, key, value)
}

func (c *Client) ReadExpiration(key string) (time.Time, bool, error) {
	readCommand, err := c.wire.EncodeMessage(wire.READEXPIRATION, key)
	if err != nil {
		return time.Time{}, false, err
	}

	responseCommand, responseMessage, err := c.connectAndSendMessage(readCommand)
	if err != nil {
		return time.Time{}, false, err
	}

	switch responseCommand {
	case wire.NULL:
		return time.Time{}, false, nil
	case wire.ERR:
		err := c.wire.DecodeError(responseMessage)
		return time.Time{}, false, err
	case wire.READEXPIRATION:
		value, err := c.wire.DecodeReadExpirationResponse(responseMessage)
		if err != nil {
			return time.Time{}, false, err
		}

		return value, true, nil
	default:
		return time.Time{}, false, errors.New(fmt.Sprintf("invalid response for READEXPIRATION command %q", responseCommand))
	}
}

func (c *Client) Expire(key string, expiration time.Time) (bool, error) {
	return c.executeAckOrNullCommand(wire.EXPIRE, key, c.wire.EncodeTime(expiration))
}

func (c *Client) Update(key string, value string) (bool, error) {
	return c.executeAckOrNullCommand(wire.UPDATE, key, value)
}

func (c *Client) Delete(key string) (bool, error) {
	return c.executeAckOrNullCommand(wire.DELETE, key)
}

func (c *Client) Upsert(key string, value string) (bool, error) {
	return c.executeAckOrNullCommand(wire.UPSERT, key, value)
}

func (c *Client) Present(key string) (bool, error) {
	return c.executeAckOrNullCommand(wire.PRESENT, key)
}

func (c *Client) Truncate() (bool, error) {
	return c.executeAckOrNullCommand(wire.TRUNCATE)
}

func (c *Client) Count() (int, error) {
	countCommand, err := c.wire.EncodeMessage(wire.COUNT)
	if err != nil {
		return 0, err
	}

	responseCommand, responseMessage, err := c.connectAndSendMessage(countCommand)
	if err != nil {
		return 0, err
	}

	switch responseCommand {
	case wire.ERR:
		err := c.wire.DecodeError(responseMessage)
		return 0, err
	case wire.COUNT:
		value, err := c.wire.DecodeCountResponse(responseMessage)
		if err != nil {
			return 0, err
		}

		return value, nil
	default:
		return 0, errors.New(fmt.Sprintf("invalid response for COUNT command %q", responseCommand))
	}
}

func (c *Client) KeysBy(prefix string) ([]string, error) {
	keysByCommand, err := c.wire.EncodeMessage(wire.KEYSBY, prefix)
	if err != nil {
		return nil, err
	}

	responseCommand, responseMessage, err := c.connectAndSendMessage(keysByCommand)
	if err != nil {
		return nil, err
	}

	switch responseCommand {
	case wire.ERR:
		err := c.wire.DecodeError(responseMessage)
		return nil, err
	case wire.KEYSBY:
		value, err := c.wire.DecodeKeysByResponse(responseMessage)
		if err != nil {
			return nil, err
		}

		return value, nil
	default:
		return nil, errors.New(fmt.Sprintf("invalid response for KEYSBY command %q", responseCommand))
	}
}

func (c *Client) DeleteBy(prefix string) (int, error) {
	deleteByCommand, err := c.wire.EncodeMessage(wire.DELETEBY, prefix)
	if err != nil {
		return 0, err
	}

	responseCommand, responseMessage, err := c.connectAndSendMessage(deleteByCommand)
	if err != nil {
		return 0, err
	}

	switch responseCommand {
	case wire.ERR:
		err := c.wire.DecodeError(responseMessage)
		return 0, err
	case wire.DELETEBY:
		value, err := c.wire.DecodeDeleteByResponse(responseMessage)
		if err != nil {
			return 0, err
		}

		return value, nil
	default:
		return 0, errors.New(fmt.Sprintf("invalid response for DELETEBY command %q", responseCommand))
	}
}

func (c *Client) ExpireBy(prefix string, expiration time.Time) (int, error) {
	expireByCommand, err := c.wire.EncodeMessage(wire.EXPIREBY, prefix, c.wire.EncodeTime(expiration))
	if err != nil {
		return 0, err
	}

	responseCommand, responseMessage, err := c.connectAndSendMessage(expireByCommand)
	if err != nil {
		return 0, err
	}

	switch responseCommand {
	case wire.ERR:
		err := c.wire.DecodeError(responseMessage)
		return 0, err
	case wire.EXPIREBY:
		value, err := c.wire.DecodeExpireByResponse(responseMessage)
		if err != nil {
			return 0, err
		}

		return value, nil
	default:
		return 0, errors.New(fmt.Sprintf("invalid response for EXPIREBY command %q", responseCommand))
	}
}

func (c *Client) executeAckOrNullCommand(command wire.Command, args ...string) (bool, error) {
	parsedCommand, err := c.wire.EncodeMessage(command, args...)
	if err != nil {
		return false, err
	}

	responseCommand, responseMessage, err := c.connectAndSendMessage(parsedCommand)
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
		return false, errors.New(fmt.Sprintf("invalid response for %q command %q", command, responseCommand))
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
