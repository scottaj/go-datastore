package server

import (
	"bufio"
	"datastore/engine"
	"datastore/wire"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

type Server struct {
	address   string
	port      int
	started   bool
	stopped   bool
	wire      wire.Protocol
	dataStore engine.DataStore
}

func New(address string, port int) Server {
	return Server{
		address:   address,
		port:      port,
		started:   false,
		stopped:   true,
		wire:      wire.Protocol{},
		dataStore: engine.NewDataStore(),
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.address, s.port))
	if err != nil {
		fmt.Printf("Error starting server: %s\n", err.Error())
		return err
	}

	s.started = true
	s.stopped = false
	fmt.Printf("Server listenting on %s:%d...\n", s.address, s.port)
	go s.listenForConnections(listener)
	return nil
}

func (s *Server) Stop() error {
	println("Stopping server")
	s.started = false

	if !s.stopped {
		// send a message to trigger shutdown
		connection, err := net.Dial("tcp", fmt.Sprintf("%s:%d", s.address, s.port))
		if err != nil {
			return err
		}
		defer connection.Close()
		err = connection.SetDeadline(time.Now().Add(time.Second * 60))
		if err != nil {
			return err
		}

		_, err = connection.Write([]byte{})
		if err != nil {
			return err
		}
	}

	for !s.stopped {
	}

	return nil
}

func (s *Server) listenForConnections(listener net.Listener) {
	defer func(listener net.Listener) {
		err := listener.Close()
		if err != nil {
			fmt.Println("Error closing listener:", err.Error())
		} else {
			s.stopped = true
		}
	}(listener)

	for {
		connection, err := listener.Accept()
		connection.SetDeadline(time.Now().Add(time.Second * 10))

		if !s.started {
			break
		}

		if err != nil {
			fmt.Printf("Error on connection: %s\n", err.Error())
		} else {
			go s.handleConnection(connection)
		}
	}
}

func (s *Server) handleConnection(connection net.Conn) {
	defer func(connection net.Conn) {
		err := connection.Close()
		if err != nil {
			fmt.Println("Error closing connection:", err.Error())
		}
	}(connection)

	// https://stackoverflow.com/a/47585913
	connectionBuffer := bufio.NewReader(connection)
	messageSizeBytes, err := connectionBuffer.Peek(4)
	if err != nil {
		s.sendErrorResponse(connection, err)
		return
	}
	if len(messageSizeBytes) != 4 {
		s.sendErrorResponse(connection, err)
		return
	}

	messageSize := binary.LittleEndian.Uint32(messageSizeBytes[:4])
	message := make([]byte, messageSize)
	_, err = io.ReadFull(connectionBuffer, message)
	if err != nil {
		s.sendErrorResponse(connection, err)
		return
	}

	command, err := s.wire.DecipherCommand(message)
	if err != nil {
		s.sendErrorResponse(connection, err)
		return
	}

	var response []byte
	switch command {
	case wire.READ:
		key, err := s.wire.DecodeRead(message)
		if err != nil {
			s.sendErrorResponse(connection, err)
			return
		}

		response = s.wire.EncodeReadResponse(s.dataStore.Read(key))
	case wire.INSERT:
		key, value, err := s.wire.DecodeInsert(message)
		if err != nil {
			s.sendErrorResponse(connection, err)
			return
		}

		response = s.wire.EncodeInsertResponse(s.dataStore.Insert(key, value))
	case wire.READEXPIRATION:
		key, err := s.wire.DecodeReadExpiration(message)
		if err != nil {
			s.sendErrorResponse(connection, err)
			return
		}

		response = s.wire.EncodeReadExpiationResponse(s.dataStore.ReadExpiration(key))
	case wire.EXPIRE:
		key, expiration, err := s.wire.DecodeExpire(message)
		if err != nil {
			s.sendErrorResponse(connection, err)
			return
		}

		response = s.wire.EncodeExpireResponse(s.dataStore.Expire(key, expiration))
	case wire.UPDATE:
		key, value, err := s.wire.DecodeUpdate(message)
		if err != nil {
			s.sendErrorResponse(connection, err)
			return
		}

		response = s.wire.EncodeUpdateResponse(s.dataStore.Update(key, value))
	case wire.DELETE:
		key, err := s.wire.DecodeDelete(message)
		if err != nil {
			s.sendErrorResponse(connection, err)
			return
		}

		response = s.wire.EncodeDeleteResponse(s.dataStore.Delete(key))
	case wire.UPSERT:
		key, value, err := s.wire.DecodeUpsert(message)
		if err != nil {
			s.sendErrorResponse(connection, err)
			return
		}

		response = s.wire.EncodeUpsertResponse(s.dataStore.Upsert(key, value))
	case wire.PRESENT:
		key, err := s.wire.DecodePresent(message)
		if err != nil {
			s.sendErrorResponse(connection, err)
			return
		}

		response = s.wire.EncodePresentResponse(s.dataStore.Present(key))
	default:
		response = s.wire.EncodeErrResponse(errors.New(fmt.Sprintf("Unknown command %q for message %v", command, message)))
	}

	_, err = connection.Write(response)
	if err != nil {
		fmt.Println("Error writing response:", err.Error())
		return
	}
}

func (s *Server) sendErrorResponse(connection net.Conn, err error) {
	_, writeErr := connection.Write(s.wire.EncodeErrResponse(err))
	fmt.Println(writeErr.Error())
}
