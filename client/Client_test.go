package client

import (
	"datastore/server"
	"testing"
	"time"
)

func TestE2EClient(t *testing.T) {
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
	if err != nil || present != true || readValue != value {
		t.Fatalf("Expected to read value %q for key %q but got %q: %q", key, value, readValue, err)
	}

	_, expirationPresent, err := client.ReadExpiration(key)
	if err != nil || expirationPresent != false {
		t.Fatalf("Expected to not read expiration %q", err)
	}

	setExpiration := time.Now().Add(time.Minute * 30)
	success, err = client.Expire(key, setExpiration)
	if success != true || err != nil {
		t.Fatalf("Got error setting expiration %q", err)
	}

	expiration, expirationPresent, err := client.ReadExpiration(key)
	if err != nil || expirationPresent != true || expiration.UnixMilli() != setExpiration.UnixMilli() {
		t.Fatalf("Expected to read expiration %q but instead read %q: %q", setExpiration, expiration, err)
	}

	newValue := "def456"
	success, err = client.Update(key, newValue)
	if success != true || err != nil {
		t.Fatalf("Got error updating %q", err)
	}

	readValue, present, err = client.Read(key)
	if err != nil || present != true || readValue != newValue {
		t.Fatalf("Expected to read value %q for key %q but got %q: %q", key, value, readValue, err)
	}

	success, err = client.Delete(key)
	if success != true || err != nil {
		t.Fatalf("Got error deleting %q", err)
	}

	readValue, present, err = client.Read(key)
	if err != nil || present != false {
		t.Fatalf("Expected to not read deleted value %q for key %q but got %q: %q", key, value, readValue, err)
	}

	success, err = client.Upsert(key, newValue)
	if success != true || err != nil {
		t.Fatalf("Got error upserting %q", err)
	}

	readValue, present, err = client.Read(key)
	if err != nil || present != true || readValue != newValue {
		t.Fatalf("Expected to read value %q for key %q but got %q: %q", key, value, readValue, err)
	}

	present, err = client.Present(key)
	if err != nil || present != true {
		t.Fatalf("Expected to find a value for key %q but was absent: %q", key, err)
	}

	success, err = client.Truncate()
	if success != true || err != nil {
		t.Fatalf("Got error truncating %q", err)
	}

	count, err := client.Count()
	if err != nil || count != 0 {
		t.Fatalf("Expected 0 keys but found %d: %v", count, err)
	}

	client.Insert("state:MI:city:Detroit", "123")
	client.Insert("state:MI:city:Grand Rapids", "456")
	client.Insert("state:MI:city:China", "789")
	client.Insert("state:OH:city:Sandusky", "123")
	client.Insert("state:OH:city:Toledo", "456")
	client.Insert("state:IN:city:Gary", "123")

	count, err = client.Count()
	if err != nil || count != 6 {
		t.Fatalf("Expected 6 keys but found %d: %v", count, err)
	}

	keys, err := client.KeysBy("state:OH")
	if err != nil || len(keys) != 2 {
		t.Fatalf("Expected 2 keys to be returned but found %d: %q", len(keys), err)
	}

	count, err = client.DeleteBy("state:MI")
	if count != 3 || err != nil {
		t.Fatalf("Got error deleting keys by prefix, expected to delete 3 items but %d was returned: %q", count, err)
	}

	count, err = client.Count()
	if err != nil || count != 3 {
		t.Fatalf("Expected 3 keys but found %d: %v", count, err)
	}

	count, err = client.ExpireBy("state:OH", time.Now().Add(time.Millisecond*100))
	if count != 2 || err != nil {
		t.Fatalf("Got error expiring keys by prefix, expected to expire 2 items but %d was returned: %q", count, err)
	}

	count, err = client.Count()
	if err != nil || count != 3 {
		t.Fatalf("Expected 3 keys but found %d: %v", count, err)
	}

	time.Sleep(time.Millisecond * 100)
	keys, err = client.KeysBy("")
	if err != nil || len(keys) != 1 {
		t.Fatalf("Expected 1 key but found %d: %v", len(keys), err)
	}

	err = runningServer.Stop()
	if err != nil {
		t.Fatalf("Got an error shutting down server %q", err)
	}
}
