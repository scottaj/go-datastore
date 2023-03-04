package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"
)

type Protocol struct {
}

type Command string

const (
	READ           Command = "READ"
	READEXPIRATION Command = "READEXPIRATION"
	INSERT         Command = "INSERT"
	UPDATE         Command = "UPDATE"
	UPSERT         Command = "UPSERT"
	DELETE         Command = "DELETE"
	PRESENT        Command = "PRESENT"
	EXPIRE         Command = "EXPIRE"
	TRUNCATE       Command = "TRUNCATE"
	COUNT          Command = "COUNT"
	KEYSBY         Command = "KEYSBY"
	DELETEBY       Command = "DELETEBY"
	EXPIREBY       Command = "EXPIREBY"

	ACK  Command = "ACK"
	NULL Command = "NULL"
	ERR  Command = "ERR"
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
	case READ, READEXPIRATION, INSERT, UPDATE, UPSERT, DELETE, PRESENT, EXPIRE, TRUNCATE, COUNT, KEYSBY, DELETEBY, EXPIREBY, ACK, NULL, ERR:
		return parsedCommand, nil
	default:
		return "", errors.New(fmt.Sprintf("%s is not a valid command", parsedCommand))
	}
}

func (p *Protocol) EncodeMessage(command Command, params ...string) ([]byte, error) {
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

func (p *Protocol) DecodeError(message []byte) error {
	arguments, err := p.decodeCommand(ERR, message)

	if err != nil {
		return err
	}

	if len(arguments) != 1 {
		return errors.New(fmt.Sprintf("expected 1 argument for an err command but found %d: %v", len(arguments), arguments))
	}

	return errors.New(arguments[0])
}

func (p *Protocol) EncodeErrResponse(err error) []byte {
	var message []byte

	message = append(message, []byte(ERR)...)
	message = append(message, messageSeparatorBinary)

	errLength := make([]byte, 4)
	binary.LittleEndian.PutUint32(errLength, uint32(len(err.Error())))
	message = append(message, errLength...)
	message = append(message, messageSeparatorBinary)
	message = append(message, []byte(err.Error())...)

	messageLength := make([]byte, 4)
	binary.LittleEndian.PutUint32(messageLength, uint32(len(message)+5))

	messageLength = append(messageLength, messageSeparatorBinary)
	message = append(messageLength, message...)

	return message
}

func (p *Protocol) EncodeNullResponse() []byte {
	// 0009|NULL
	return []byte{0x9, 0x0, 0x0, 0x0, messageSeparatorBinary, 0x4E, 0x55, 0x4C, 0x4C}
}

func (p *Protocol) EncodeAckResponse() []byte {
	// 0008|ACK
	return []byte{0x8, 0x0, 0x0, 0x0, messageSeparatorBinary, 0x41, 0x43, 0x4B}
}

func (p *Protocol) DecodeRead(message []byte) (string, error) {
	return p.decodeKeyCommand(READ, message)
}

func (p *Protocol) DecodeReadResponse(message []byte) (string, error) {
	return p.decodeKeyCommand(READ, message)
}

func (p *Protocol) EncodeReadResponse(value string, present bool) []byte {
	if present {
		message, err := p.EncodeMessage(READ, value)
		if err != nil {
			return p.EncodeErrResponse(err)
		}

		return message
	} else {
		return p.EncodeNullResponse()
	}
}

func (p *Protocol) DecodeInsert(message []byte) (string, string, error) {
	return p.decodeKeyValueCommand(INSERT, message)
}

func (p *Protocol) EncodeInsertResponse(valueInserted bool) []byte {
	return p.encodeAckOrNullResponse(valueInserted)
}

func (p *Protocol) DecodeTime(timestampString string) (time.Time, error) {
	timestamp, err := strconv.ParseInt(timestampString, 10, 64)
	if err != nil {
		return time.Time{}, errors.New(fmt.Sprintf("Expected a unix millisecond timestamp, but could not get that from arguement value %q: %q", timestampString, err))
	}

	return time.UnixMilli(timestamp), nil
}

// EncodeTime
// Times are encoded in the protocol as unix timestamps with milliseconds
func (p *Protocol) EncodeTime(time time.Time) string {
	return strconv.FormatInt(time.UnixMilli(), 10)
}

func (p *Protocol) DecodeReadExpiration(message []byte) (string, error) {
	return p.decodeKeyCommand(READEXPIRATION, message)
}

func (p *Protocol) DecodeReadExpirationResponse(message []byte) (time.Time, error) {
	arguments, err := p.decodeCommand(READEXPIRATION, message)

	if err != nil {
		return time.Time{}, err
	}

	if len(arguments) != 1 {
		return time.Time{}, errors.New(fmt.Sprintf("expected 1 argument for a READ response but found %d: %v", len(arguments), arguments))
	}

	decodedTime, err := p.DecodeTime(arguments[0])
	if err != nil {
		return time.Time{}, err
	}

	return decodedTime, nil
}

func (p *Protocol) EncodeReadExpiationResponse(expiration time.Time, expirationPresent bool) []byte {
	if expirationPresent {
		message, err := p.EncodeMessage(READEXPIRATION, p.EncodeTime(expiration))
		if err != nil {
			return p.EncodeErrResponse(err)
		}

		return message
	} else {
		return p.EncodeNullResponse()
	}
}

func (p *Protocol) DecodeExpire(message []byte) (string, time.Time, error) {
	arguments, err := p.decodeCommand(EXPIRE, message)

	if err != nil {
		return "", time.Time{}, err
	}

	if len(arguments) != 2 {
		return "", time.Time{}, errors.New(fmt.Sprintf("expected 2 arguments for an EXPIRE command but found %d: %v", len(arguments), arguments))
	}

	decodedTime, err := p.DecodeTime(arguments[1])
	if err != nil {
		return "", time.Time{}, err
	}

	return arguments[0], decodedTime, nil
}

func (p *Protocol) EncodeExpireResponse(expirationSet bool) []byte {
	return p.encodeAckOrNullResponse(expirationSet)
}

func (p *Protocol) DecodeUpdate(message []byte) (string, string, error) {
	return p.decodeKeyValueCommand(UPDATE, message)
}

func (p *Protocol) EncodeUpdateResponse(successful bool) []byte {
	return p.encodeAckOrNullResponse(successful)
}

func (p *Protocol) DecodeDelete(message []byte) (string, error) {
	return p.decodeKeyCommand(DELETE, message)
}

func (p *Protocol) EncodeDeleteResponse(success bool) []byte {
	return p.encodeAckOrNullResponse(success)
}

func (p *Protocol) DecodeUpsert(message []byte) (string, string, error) {
	return p.decodeKeyValueCommand(UPSERT, message)
}

func (p *Protocol) EncodeUpsertResponse(success bool) []byte {
	return p.encodeAckOrNullResponse(success)
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

func (p *Protocol) decodeKeyCommand(command Command, message []byte) (string, error) {
	arguments, err := p.decodeCommand(command, message)

	if err != nil {
		return "", err
	}

	if len(arguments) != 1 {
		return "", errors.New(fmt.Sprintf("expected 1 arguments for a %q command but found %d: %v", command, len(arguments), arguments))
	}

	return arguments[0], nil
}

func (p *Protocol) decodeKeyValueCommand(command Command, message []byte) (string, string, error) {
	arguments, err := p.decodeCommand(command, message)

	if err != nil {
		return "", "", err
	}

	if len(arguments) != 2 {
		return "", "", errors.New(fmt.Sprintf("expected 2 arguments for an %q command but found %d: %v", command, len(arguments), arguments))
	}

	return arguments[0], arguments[1], nil
}

func (p *Protocol) encodeAckOrNullResponse(success bool) []byte {
	if success {
		return p.EncodeAckResponse()
	} else {
		return p.EncodeNullResponse()
	}
}
