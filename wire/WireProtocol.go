package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

// Protocol
/**
* Basic protocol format is: <4 bytes of total message size, little endian uint32>|<CMD ascii>|<4 bytes of field size
* little endian uint32>|<fieldvalue UTF-8>|… with no trailing separator
 */
type Protocol struct {
}

type Command string

const (
	READ   Command = "READ"
	INSERT Command = "INSERT"
)

const messageSeparatorBinary = byte(0x7C)

func (p *Protocol) DecipherCommand(request []byte) (Command, error) {
	var commandBytes []byte

	// the first 4 bytes are the message size, and the 5th byte is a separator
	// the command is every byte starting at the 6th byte until you hit another separator
	for i := 5; i < len(request); i++ {
		currentByte := request[i]

		if currentByte == messageSeparatorBinary {
			break
		}

		commandBytes = append(commandBytes, currentByte)
	}

	parsedCommand := Command(commandBytes)

	switch parsedCommand {
	case READ, INSERT:
		return parsedCommand, nil
	default:
		return "", errors.New(fmt.Sprintf("%s is not a valid command", parsedCommand))
	}
}

func (p *Protocol) EncodeCommand(command Command, params ...string) ([]byte, error) {
	var message []byte

	// start with the command
	message = append(message, messageSeparatorBinary)
	message = append(message, []byte(command)...)

	// Calculate the byte size of each argument and then add that and the argument
	for _, param := range params {
		paramBytes := []byte(param)
		paramSize := make([]byte, 4)
		binary.LittleEndian.PutUint32(paramSize, uint32(len(paramBytes)))

		message = append(message, messageSeparatorBinary)
		message = append(message, paramSize...)
		message = append(message, messageSeparatorBinary)
		message = append(message, paramBytes...)
	}

	// calculate the total message length and append that to the front of the message.
	// Include the 4 bytes for the message length in the total
	messageLength := len(message) + 4
	messageSize := make([]byte, 4)
	binary.LittleEndian.PutUint32(messageSize, uint32(messageLength))
	message = append(messageSize, message...)

	return message, nil
}

func (p *Protocol) DecodeRead(message []byte) (string, error) {
	arguments, err := p.decodeCommand(READ, message)

	if err != nil {
		return "", err
	}

	if len(arguments) != 1 {
		return "", errors.New(fmt.Sprintf("expected 1 argument for a read command but found %d: %v", len(arguments), arguments))
	}

	return arguments[0], nil
}

func (p *Protocol) EncodeRead(value string, expiration time.Time, present bool) []byte {
	return nil
}

func (p *Protocol) decodeCommand(command Command, message []byte) ([]string, error) {
	var arguments []string

	// first 5 bytes are message size + separator we can ignore
	// next n non separator bytes plus the separator following are the command which we can ignore
	prefixSize := 5 + len(command)

	// next we should have our argument pairs. Each will be prefixed by a separator, then have 4 bytes of argument
	// size, a separator, and then that many bytes of the actual argument value
	// If there is another separator after the argument value that means there is another argument pair
	messageOffset := prefixSize
	for messageOffset < len(message) && message[messageOffset] == messageSeparatorBinary {
		if messageOffset+5 > len(message) {
			return nil, errors.New(fmt.Sprintf("Malformed message, could not decode: %v", message))
		}
		argumentSize := int(binary.LittleEndian.Uint32(message[messageOffset+1 : messageOffset+5]))

		argumentStart := messageOffset + 6
		argumentEnd := argumentStart + argumentSize

		if argumentEnd > len(message) {
			return nil, errors.New(fmt.Sprintf("Malformed message, could not decode: %v", message))
		}
		arguments = append(arguments, string(message[argumentStart:argumentEnd]))

		messageOffset = argumentEnd
	}

	if messageOffset != len(message) {
		return nil, errors.New(fmt.Sprintf("Malformed message, could not decode: %v", message))
	}

	return arguments, nil
}
