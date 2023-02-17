package datastore

import (
	"testing"
	"time"
)

func TestInsertAndRead(t *testing.T) {
	Truncate()

	data := "abc123"
	key := "testkey"
	setValue, success := Insert(key, data)
	if setValue != data || success == false {
		t.Fatalf("failed to insert key %q, expected %q to equal %q", key, setValue, data)
	}

	readValue, _, present := Read(key)
	if readValue != data || present == false {
		t.Fatalf("failed to read value %q from key %q got %q", data, key, readValue)
	}
}

func TestInsertDuplicate(t *testing.T) {
	Truncate()

	data := "abc123"
	key := "testkey"
	setValue, success := Insert(key, data)
	if setValue != data || success == false {
		t.Fatalf("failed to insert key %q", key)
	}

	updatedData := "def456"
	setValue, success = Insert(key, updatedData)
	if setValue != data || success == true {
		t.Fatalf("expected data %q not to be overwritten but is now %q", data, setValue)
	}
}

func TestReadAbsent(t *testing.T) {
	Truncate()

	value, _, present := Read("def456")
	if value != "" || present == true {
		t.Fatalf("expected no value but found %q", value)
	}
}

func TestReadEmptyString(t *testing.T) {
	Truncate()

	data := ""
	key := "testkey"
	setValue, success := Insert(key, data)
	if setValue != data || success == false {
		t.Fatalf("failed to insert key %q, expected %q to equal %q", key, setValue, data)
	}

	readValue, _, present := Read(key)
	if readValue != data || present == false {
		t.Fatalf("failed to read value %q from key %q got %q", data, key, readValue)
	}
}

func TestUpdateExistingValueAndRead(t *testing.T) {
	Truncate()

	data := "abc123"
	key := "testkey"
	_, _ = Insert(key, data)

	updatedData := "def456"
	value, success := Update(key, updatedData)

	if value != updatedData || success == false {
		t.Fatalf("expected value for key %q to be updated to %q but was %q", key, updatedData, value)
	}

	readValue, _, _ := Read(key)
	if readValue != updatedData {
		t.Fatalf("expected to read updated value %q but was %q", updatedData, readValue)
	}
}

func TestUpdateAbsentValueAndRead(t *testing.T) {
	Truncate()

	key := "testkey"

	updatedData := "def456"
	value, success := Update(key, updatedData)

	if value == updatedData || success == true {
		t.Fatalf("expected update not to work but got value %q", value)
	}

	readValue, _, present := Read(key)
	if readValue == updatedData || present == true {
		t.Fatalf("expected update not to work but read value %q", readValue)
	}
}

func TestUpsertNewValueAndUpdateIt(t *testing.T) {
	Truncate()

	data := "abc123"
	key := "testkey"

	value := Upsert(key, data)
	if value != data {
		t.Fatalf("expected upsert to insert new data %q", value)
	}
	readValue, _, present := Read(key)
	if readValue != data || present == false {
		t.Fatalf("expected update to work but read value %q", readValue)
	}

	updatedData := "def456"
	value = Upsert(key, updatedData)
	if value != updatedData {
		t.Fatalf("expected upsert to update existing data %q", value)
	}

	readValue, _, present = Read(key)
	if readValue != updatedData || present == false {
		t.Fatalf("expected update to work but read value %q", readValue)
	}
}

func TestDeleteExistingValue(t *testing.T) {
	Truncate()

	data := "abc123"
	key := "testkey"

	_ = Upsert(key, data)

	present := Delete(key)

	if present == false {
		t.Fatalf("failed to delete key %q", key)
	}

	_, _, present = Read(key)
	if present == true {
		t.Fatalf("Expected key %q to be deleted but was able to read it", key)
	}
}

func TestDeleteAbsentValue(t *testing.T) {
	Truncate()

	key := "testkey"

	present := Delete(key)

	if present == true {
		t.Fatalf("deleted key %q that should not have been present", key)
	}

	_, _, present = Read(key)
	if present == true {
		t.Fatalf("Expected key %q to be deleted but was able to read it", key)
	}
}

func TestInsertAndPresent(t *testing.T) {
	Truncate()

	data := "abc123"
	key := "testkey"

	present := Present(key)
	if present == true {
		t.Fatalf("expected key %q not to exist but it did", key)
	}

	setValue, success := Insert(key, data)
	if setValue != data || success == false {
		t.Fatalf("failed to insert key %q, expected %q to equal %q", key, setValue, data)
	}

	present = Present(key)
	if present == false {
		t.Fatalf("expected key %q to exist but it didn't", key)
	}
}

func TestCount(t *testing.T) {
	Truncate()

	count := Count()
	if count != 0 {
		t.Fatalf("expected count 0 but was %q", count)
	}

	_, _ = Insert("a", "1")
	count = Count()
	if count != 1 {
		t.Fatalf("expected count 1 but was %q", count)
	}

	_, _ = Insert("a", "1")
	count = Count()
	if count != 1 {
		t.Fatalf("expected count 1 but was %q", count)
	}

	_, _ = Insert("b", "2")
	count = Count()
	if count != 2 {
		t.Fatalf("expected count 2 but was %q", count)
	}

	_, _ = Update("a", "3")
	count = Count()
	if count != 2 {
		t.Fatalf("expected count 2 but was %q", count)
	}

	_ = Delete("a")
	count = Count()
	if count != 1 {
		t.Fatalf("expected count 1 but was %q", count)
	}
}

func TestReadExpiredValue(t *testing.T) {
	Truncate()

	data := "abc123"
	key := "testkey"
	_, _ = Insert(key, data)

	expiration := time.Now().Add(time.Millisecond * 100).UTC()
	success := Expire(key, expiration)
	if success != true {
		t.Fatalf("Failed to set expiration %q for key %q", expiration, key)
	}

	readValue, readExperation, present := Read(key)
	if readValue != data || readExperation != expiration || present == false {
		t.Fatalf("failed to read value %q with expiration %q from key %q got %q with expiration %q", data, expiration, key, readValue, readExperation)
	}

	time.Sleep(time.Millisecond * 100)

	_, _, present = Read(key)
	if present == true {
		t.Fatalf("expected to not find expired value for key %q", key)
	}
}

func TestExpireNonExistentKey(t *testing.T) {
	Truncate()

	success := Expire("xyz987", time.Now())
	if success == true {
		t.Fatalf("did not expect to be able to expire non existing key")
	}
}

func TestInsertExpiredKeyRemovesExpiration(t *testing.T) {
	Truncate()

	data := "abc123"
	key := "testkey"
	_, _ = Insert(key, data)

	expiration := time.Now().Add(time.Millisecond * 100).UTC()
	_ = Expire(key, expiration)

	time.Sleep(time.Millisecond * 100)

	_, _, present := Read(key)
	if present == true {
		t.Fatalf("expected to not find expired value for key %q", key)
	}

	newData := "def456"
	_, _ = Insert(key, newData)
	readValue, readExpiration, present := Read(key)
	if readValue != newData || !readExpiration.IsZero() || present == false {
		t.Fatalf("expected to find value %q for key %q with no expiration, but it had value %q with expiration %q", newData, key, readValue, readExpiration)
	}
}

func TestUpsertExpiredKeyRemovesExpiration(t *testing.T) {
	Truncate()

	data := "abc123"
	key := "testkey"
	_ = Upsert(key, data)

	expiration := time.Now().Add(time.Millisecond * 100).UTC()
	_ = Expire(key, expiration)

	time.Sleep(time.Millisecond * 100)

	_, _, present := Read(key)
	if present == true {
		t.Fatalf("expected to not find expired value for key %q", key)
	}

	newData := "def456"
	_ = Upsert(key, newData)
	readValue, readExpiration, present := Read(key)
	if readValue != newData || !readExpiration.IsZero() || present == false {
		t.Fatalf("expected to find value %q for key %q with no expiration, but it had value %q with expiration %q", newData, key, readValue, readExpiration)
	}
}

func TestDeleteKeyWithExpirationThenRecreateItRemovesExpiration(t *testing.T) {
	Truncate()

	data := "abc123"
	key := "testkey"
	_, _ = Insert(key, data)

	expiration := time.Now().Add(time.Millisecond * 100).UTC()
	_ = Expire(key, expiration)

	_ = Delete(key)

	newData := "def456"
	_, _ = Insert(key, newData)

	time.Sleep(time.Millisecond * 100)

	readValue, readExpiration, present := Read(key)
	if readValue != newData || !readExpiration.IsZero() || present == false {
		t.Fatalf("expected to find value %q for key %q with no expiration, but it had value %q with expiration %q", newData, key, readValue, readExpiration)
	}
}
