package wire

import "testing"

func TestEncodeCommand(t *testing.T) {
	protocol := Protocol{}

	commandBytes, err := protocol.EncodeCommand(READ, "")
	if err != nil || len(commandBytes) != 15 {
		t.Fatalf("Expected a 16 byte command but got %v (%s): %q", commandBytes, commandBytes, err)
	}

	commandBytes, err = protocol.EncodeCommand(READ, "key1")
	if err != nil || len(commandBytes) != 19 {
		t.Fatalf("Expected a 20 byte command but got %v (%s): %q", commandBytes, commandBytes, err)
	}
}

func TestDecipherCommand(t *testing.T) {
	protocol := Protocol{}

	message, _ := protocol.EncodeCommand(READ, "my:test:key")
	command, err := protocol.DecipherCommand(message)
	if err != nil || command != READ {
		t.Fatalf("Expected to parse a read command but got %q: %q", command, err)
	}

	message, _ = protocol.EncodeCommand(INSERT, "my:test:key", "abc123")
	command, err = protocol.DecipherCommand(message)
	if err != nil || command != INSERT {
		t.Fatalf("Expected to parse an insert command but got %q: %q", command, err)
	}

	message, _ = protocol.EncodeCommand("NOTACOMMAND", "my:test:key", "abc123")
	command, err = protocol.DecipherCommand(message)
	if err == nil {
		t.Fatalf("Expected an error parsing the command but got %q: %q", command, err)
	}

	command, err = protocol.DecipherCommand(nil)
	if err == nil {
		t.Fatalf("Expected an error parsing the command but got %q: %q", command, err)
	}

	command, err = protocol.DecipherCommand([]byte{127, 31, 28})
	if err == nil {
		t.Fatalf("Expected an error parsing the command but got %q: %q", command, err)
	}
}

func TestDecodeRead(t *testing.T) {
	protocol := Protocol{}
	keyParam := "key1"
	commandBytes, _ := protocol.EncodeCommand(READ, keyParam)

	readArg, err := protocol.DecodeRead(commandBytes)
	if err != nil || readArg != keyParam {
		t.Fatalf("Expected to read an argument %q back but was %q: %q", keyParam, readArg, err)
	}

	commandBytes, _ = protocol.EncodeCommand(READ, "")

	readArg, err = protocol.DecodeRead(commandBytes)
	if err != nil || readArg != "" {
		t.Fatalf("Expected to read an argument %q back but was %q: %q", "", readArg, err)
	}

	commandBytes, _ = protocol.EncodeCommand(READ, keyParam, "invalid")

	readArg, err = protocol.DecodeRead(commandBytes)
	if err == nil {
		t.Fatalf("Expected an error")
	}

	commandBytes, _ = protocol.EncodeCommand(READ, keyParam)
	commandBytes = append(commandBytes, 0x7C)
	readArg, err = protocol.DecodeRead(commandBytes)
	if err == nil {
		t.Fatalf("Expected an error")
	}

	commandBytes, _ = protocol.EncodeCommand(READ, keyParam)
	commandBytes = append(commandBytes, 0x46)
	readArg, err = protocol.DecodeRead(commandBytes)
	if err == nil {
		t.Fatalf("Expected an error")
	}

	commandBytes, _ = protocol.EncodeCommand(READ, keyParam)
	// this doesn't work to just remove a byte from the slice https://stackoverflow.com/a/63362043
	//commandBytes = commandBytes[0 : len(commandBytes)-1]
	modifiedBytes := [18]byte{}
	for i := 0; i < len(commandBytes)-1; i++ {
		modifiedBytes[i] = commandBytes[i]
	}
	readArg, err = protocol.DecodeRead(modifiedBytes[:18])
	if err == nil {
		t.Fatalf("Expected an error %q", err)
	}
}
