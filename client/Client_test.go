package client

import (
	"datastore/server"
	"testing"
	"time"
)

func TestE2EClientInsertAndRead(t *testing.T) {
	runningServer := server.New("localhost", 8888)
	client := New("localhost", 8888)

	err := runningServer.Start()
	if err != nil {
		t.Fatalf("Error starting server %q", err)
	}

	time.Sleep(time.Second * 1) // give runningServer time to fully start

	key, value := "key1", "abc123"

	readValue, present, err := client.Read(key)
	if err != nil || present != false {
		t.Fatalf("Expected to have no error and no value present but got value %q and error %q", readValue, err)
	}

	success, err := client.Insert(key, value)
	if err != nil || success != true {
		t.Fatalf("expected to write value with no issue but got %q", err)
	}

	readValue, present, err = client.Read(key)
	if err != nil || present != true {
		t.Fatalf("Expected read value %q for key %q but got %q: %q", key, value, readValue, err)
	}

	err = runningServer.Stop()
	if err != nil {
		t.Fatalf("Got an error shutting down server %q", err)
	}
}
