package server

import (
	"datastore/engine"
	"datastore/wire"
	"fmt"
	"net"
)

type Server struct {
	address   string
	port      int
	started   bool
	stopped   bool
	wire      wire.Protocol
	dataStore engine.DataStore
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.address, s.port))
	if err != nil {
		fmt.Printf("Error starting server: %s\n", err.Error())
		return err
	}

	s.started = true
	fmt.Printf("Server listenting on %s:%d...\n", s.address, s.port)
	go s.listenForConnections(listener)
	return nil
}

func (s *Server) Stop() {
	s.started = false

	for !s.stopped {
	}
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

	for s.started {
		connection, err := listener.Accept()

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

	// TODO can use the message size at the front of the message to properly size this
	buffer := make([]byte, 1024)
	// Read the incoming connection into the buffer.
	_, err := connection.Read(buffer)
	if err != nil {
		fmt.Println("Error reading request:", err.Error())
		return
	}

	command, err := s.wire.DecipherCommand(buffer)
	if err != nil {
		fmt.Println("Error parsing command:", err.Error())
		return
	}

	var response []byte
	switch command {
	case wire.READ:
		key, err := s.wire.DecodeRead(buffer)
		if err != nil {
			fmt.Println("Error decoding command:", err.Error())
			return
		}
		response = s.wire.EncodeRead(s.dataStore.Read(key))
	}

	_, err = connection.Write(response)
	if err != nil {
		fmt.Println("Error writing response:", err.Error())
		return
	}
}
