package datastore

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestInsertAndRead(t *testing.T) {
	ds := New()

	data := "abc123"
	key := "testkey"
	setValue, success := ds.Insert(key, data)
	if setValue != data || success == false {
		t.Fatalf("failed to insert key %q, expected %q to equal %q", key, setValue, data)
	}

	readValue, _, present := ds.Read(key)
	if readValue != data || present == false {
		t.Fatalf("failed to read value %q from key %q got %q", data, key, readValue)
	}
}

func TestInsertDuplicate(t *testing.T) {
	ds := New()

	data := "abc123"
	key := "testkey"
	setValue, success := ds.Insert(key, data)
	if setValue != data || success == false {
		t.Fatalf("failed to insert key %q", key)
	}

	updatedData := "def456"
	setValue, success = ds.Insert(key, updatedData)
	if setValue != data || success == true {
		t.Fatalf("expected data %q not to be overwritten but is now %q", data, setValue)
	}
}

func TestReadAbsent(t *testing.T) {
	ds := New()

	value, _, present := ds.Read("def456")
	if value != "" || present == true {
		t.Fatalf("expected no value but found %q", value)
	}
}

func TestReadEmptyString(t *testing.T) {
	ds := New()

	data := ""
	key := "testkey"
	setValue, success := ds.Insert(key, data)
	if setValue != data || success == false {
		t.Fatalf("failed to insert key %q, expected %q to equal %q", key, setValue, data)
	}

	readValue, _, present := ds.Read(key)
	if readValue != data || present == false {
		t.Fatalf("failed to read value %q from key %q got %q", data, key, readValue)
	}
}

func TestUpdateExistingValueAndRead(t *testing.T) {
	ds := New()

	data := "abc123"
	key := "testkey"
	_, _ = ds.Insert(key, data)

	updatedData := "def456"
	value, success := ds.Update(key, updatedData)

	if value != updatedData || success == false {
		t.Fatalf("expected value for key %q to be updated to %q but was %q", key, updatedData, value)
	}

	readValue, _, _ := ds.Read(key)
	if readValue != updatedData {
		t.Fatalf("expected to read updated value %q but was %q", updatedData, readValue)
	}
}

func TestUpdateAbsentValueAndRead(t *testing.T) {
	ds := New()

	key := "testkey"

	updatedData := "def456"
	value, success := ds.Update(key, updatedData)

	if value == updatedData || success == true {
		t.Fatalf("expected update not to work but got value %q", value)
	}

	readValue, _, present := ds.Read(key)
	if readValue == updatedData || present == true {
		t.Fatalf("expected update not to work but read value %q", readValue)
	}
}

func TestUpsertNewValueAndUpdateIt(t *testing.T) {
	ds := New()

	data := "abc123"
	key := "testkey"

	value := ds.Upsert(key, data)
	if value != data {
		t.Fatalf("expected upsert to insert new data %q", value)
	}
	readValue, _, present := ds.Read(key)
	if readValue != data || present == false {
		t.Fatalf("expected update to work but read value %q", readValue)
	}

	updatedData := "def456"
	value = ds.Upsert(key, updatedData)
	if value != updatedData {
		t.Fatalf("expected upsert to update existing data %q", value)
	}

	readValue, _, present = ds.Read(key)
	if readValue != updatedData || present == false {
		t.Fatalf("expected update to work but read value %q", readValue)
	}
}

func TestDeleteExistingValue(t *testing.T) {
	ds := New()

	data := "abc123"
	key := "testkey"

	_ = ds.Upsert(key, data)

	present := ds.Delete(key)

	if present == false {
		t.Fatalf("failed to delete key %q", key)
	}

	_, _, present = ds.Read(key)
	if present == true {
		t.Fatalf("Expected key %q to be deleted but was able to read it", key)
	}
}

func TestDeleteAbsentValue(t *testing.T) {
	ds := New()

	key := "testkey"

	present := ds.Delete(key)

	if present == true {
		t.Fatalf("deleted key %q that should not have been present", key)
	}

	_, _, present = ds.Read(key)
	if present == true {
		t.Fatalf("Expected key %q to be deleted but was able to read it", key)
	}
}

func TestInsertAndPresent(t *testing.T) {
	ds := New()

	data := "abc123"
	key := "testkey"

	present := ds.Present(key)
	if present == true {
		t.Fatalf("expected key %q not to exist but it did", key)
	}

	setValue, success := ds.Insert(key, data)
	if setValue != data || success == false {
		t.Fatalf("failed to insert key %q, expected %q to equal %q", key, setValue, data)
	}

	present = ds.Present(key)
	if present == false {
		t.Fatalf("expected key %q to exist but it didn't", key)
	}
}

func TestCount(t *testing.T) {
	ds := New()

	count := ds.Count()
	if count != 0 {
		t.Fatalf("expected count 0 but was %q", count)
	}

	_, _ = ds.Insert("a", "1")
	count = ds.Count()
	if count != 1 {
		t.Fatalf("expected count 1 but was %q", count)
	}

	_, _ = ds.Insert("a", "1")
	count = ds.Count()
	if count != 1 {
		t.Fatalf("expected count 1 but was %q", count)
	}

	_, _ = ds.Insert("b", "2")
	count = ds.Count()
	if count != 2 {
		t.Fatalf("expected count 2 but was %q", count)
	}

	_, _ = ds.Update("a", "3")
	count = ds.Count()
	if count != 2 {
		t.Fatalf("expected count 2 but was %q", count)
	}

	_ = ds.Delete("a")
	count = ds.Count()
	if count != 1 {
		t.Fatalf("expected count 1 but was %q", count)
	}
}

func TestReadExpiredValue(t *testing.T) {
	ds := New()

	data := "abc123"
	key := "testkey"
	_, _ = ds.Insert(key, data)

	expiration := time.Now().Add(time.Millisecond * 100).UTC()
	success := ds.Expire(key, expiration)
	if success != true {
		t.Fatalf("Failed to set expiration %q for key %q", expiration, key)
	}

	readValue, readExperation, present := ds.Read(key)
	if readValue != data || readExperation != expiration || present == false {
		t.Fatalf("failed to read value %q with expiration %q from key %q got %q with expiration %q", data, expiration, key, readValue, readExperation)
	}

	time.Sleep(time.Millisecond * 100)

	_, _, present = ds.Read(key)
	if present == true {
		t.Fatalf("expected to not find expired value for key %q", key)
	}
}

func TestExpireNonExistentKey(t *testing.T) {
	ds := New()

	success := ds.Expire("xyz987", time.Now())
	if success == true {
		t.Fatalf("did not expect to be able to expire non existing key")
	}
}

func TestInsertExpiredKeyRemovesExpiration(t *testing.T) {
	ds := New()

	data := "abc123"
	key := "testkey"
	_, _ = ds.Insert(key, data)

	expiration := time.Now().Add(time.Millisecond * 100).UTC()
	_ = ds.Expire(key, expiration)

	time.Sleep(time.Millisecond * 100)

	_, _, present := ds.Read(key)
	if present == true {
		t.Fatalf("expected to not find expired value for key %q", key)
	}

	newData := "def456"
	_, _ = ds.Insert(key, newData)
	readValue, readExpiration, present := ds.Read(key)
	if readValue != newData || !readExpiration.IsZero() || present == false {
		t.Fatalf("expected to find value %q for key %q with no expiration, but it had value %q with expiration %q", newData, key, readValue, readExpiration)
	}
}

func TestUpsertExpiredKeyRemovesExpiration(t *testing.T) {
	ds := New()

	data := "abc123"
	key := "testkey"
	_ = ds.Upsert(key, data)

	expiration := time.Now().Add(time.Millisecond * 100).UTC()
	_ = ds.Expire(key, expiration)

	time.Sleep(time.Millisecond * 100)

	_, _, present := ds.Read(key)
	if present == true {
		t.Fatalf("expected to not find expired value for key %q", key)
	}

	newData := "def456"
	_ = ds.Upsert(key, newData)
	readValue, readExpiration, present := ds.Read(key)
	if readValue != newData || !readExpiration.IsZero() || present == false {
		t.Fatalf("expected to find value %q for key %q with no expiration, but it had value %q with expiration %q", newData, key, readValue, readExpiration)
	}
}

func TestDeleteKeyWithExpirationThenRecreateItRemovesExpiration(t *testing.T) {
	ds := New()

	data := "abc123"
	key := "testkey"
	_, _ = ds.Insert(key, data)

	expiration := time.Now().Add(time.Millisecond * 100).UTC()
	_ = ds.Expire(key, expiration)

	_ = ds.Delete(key)

	newData := "def456"
	_, _ = ds.Insert(key, newData)

	time.Sleep(time.Millisecond * 100)

	readValue, readExpiration, present := ds.Read(key)
	if readValue != newData || !readExpiration.IsZero() || present == false {
		t.Fatalf("expected to find value %q for key %q with no expiration, but it had value %q with expiration %q", newData, key, readValue, readExpiration)
	}
}

func TestInsertTriggersAsyncExpirationCleanup(t *testing.T) {
	ds := New()

	key1, data1 := "key1", "abc123"
	key2, data2 := "key2", "abc456"
	key3, data3 := "key3", "def123"
	key4, data4 := "key4", "def456"

	_, _ = ds.Insert(key1, data1)
	_, _ = ds.Insert(key2, data2)
	_, _ = ds.Insert(key3, data3)

	expiration := time.Now().Add(time.Millisecond * 100)

	_ = ds.Expire(key1, expiration)
	_ = ds.Expire(key2, expiration)
	_ = ds.Expire(key3, expiration)

	time.Sleep(time.Millisecond * 100)

	count := ds.Count()
	if count != 3 {
		t.Fatalf("expected count to be 3 because there was no write to cleanup but was %d", count)
	}

	_, _ = ds.Insert(key4, data4)

	time.Sleep(time.Millisecond * 10)

	count = ds.Count()
	if count != 1 {
		t.Fatalf("expected count to be 1 because write cause cleanup but was %d", count)
	}
}

func TestUpdateTriggersAsyncExpirationCleanup(t *testing.T) {
	ds := New()

	key1, data1 := "key1", "abc123"
	key2, data2 := "key2", "abc456"
	key3, data3 := "key3", "def123"

	_, _ = ds.Insert(key1, data1)
	_, _ = ds.Insert(key2, data2)
	_, _ = ds.Insert(key3, data3)

	expiration := time.Now().Add(time.Millisecond * 100)

	_ = ds.Expire(key1, expiration)
	_ = ds.Expire(key2, expiration)

	time.Sleep(time.Millisecond * 100)

	count := ds.Count()
	if count != 3 {
		t.Fatalf("expected count to be 3 because there was no write to cleanup but was %d", count)
	}

	_, _ = ds.Update(key3, data1)

	time.Sleep(time.Millisecond * 10)

	count = ds.Count()
	if count != 1 {
		t.Fatalf("expected count to be 1 because write cause cleanup but was %d", count)
	}
}

func TestUpsertTriggersAsyncExpirationCleanup(t *testing.T) {
	ds := New()

	key1, data1 := "key1", "abc123"
	key2, data2 := "key2", "abc456"
	key3, data3 := "key3", "def123"
	key4, data4 := "key4", "def456"

	_, _ = ds.Insert(key1, data1)
	_, _ = ds.Insert(key2, data2)
	_, _ = ds.Insert(key3, data3)

	expiration := time.Now().Add(time.Millisecond * 100)

	_ = ds.Expire(key1, expiration)
	_ = ds.Expire(key2, expiration)
	_ = ds.Expire(key3, expiration)

	time.Sleep(time.Millisecond * 100)

	count := ds.Count()
	if count != 3 {
		t.Fatalf("expected count to be 3 because there was no write to cleanup but was %d", count)
	}

	_ = ds.Upsert(key4, data4)

	time.Sleep(time.Millisecond * 10)

	count = ds.Count()
	if count != 1 {
		t.Fatalf("expected count to be 1 because write cause cleanup but was %d", count)
	}
}

func TestDeleteTriggersAsyncExpirationCleanup(t *testing.T) {
	ds := New()

	key1, data1 := "key1", "abc123"
	key2, data2 := "key2", "abc456"
	key3, data3 := "key3", "def123"
	key4, data4 := "key4", "def456"

	_, _ = ds.Insert(key1, data1)
	_, _ = ds.Insert(key2, data2)
	_, _ = ds.Insert(key3, data3)
	_, _ = ds.Insert(key4, data4)

	expiration := time.Now().Add(time.Millisecond * 100)

	_ = ds.Expire(key1, expiration)
	_ = ds.Expire(key2, expiration)
	_ = ds.Expire(key3, expiration)

	time.Sleep(time.Millisecond * 100)

	count := ds.Count()
	if count != 4 {
		t.Fatalf("expected count to be 4 because there was no write to cleanup but was %d", count)
	}

	_ = ds.Delete(key4)

	time.Sleep(time.Millisecond * 10)

	count = ds.Count()
	if count != 0 {
		t.Fatalf("expected count to be 0 because write cause cleanup but was %d", count)
	}
}

func TestThreadSafetyOfWriteOperationsWithAsyncCleanup(t *testing.T) {
	ds := New()

	// Without mutexes on updates to the internal data store this test will crash
	for i := 0; i < 1000; i++ {
		if i%4 == 0 {
			// ds.Insert with expiration
			key := fmt.Sprintf("key%d", i)
			_, _ = ds.Insert(key, "abc123")
			_ = ds.Expire(key, time.Now())
		} else if i%4 == 1 {
			// insert without expiration
			key := fmt.Sprintf("key%d", i)
			_, _ = ds.Insert(key, "abc123")
		} else if i%4 == 2 {
			// update inserted value from last clause
			key := fmt.Sprintf("key%d", i-1)
			_, _ = ds.Update(key, "def456")
		} else if i%4 == 3 {
			// 33/33/33 of delete the updated value or upsert to update it with an expiration or upsert a new value
			// without an expiration
			key := fmt.Sprintf("key%d", i-2)

			rand.Seed(time.Now().UnixNano())
			choice := rand.Intn(3-1+1) + 1

			if choice == 1 {
				_ = ds.Delete(key)
			} else if choice == 2 {
				_ = ds.Upsert(key, "abc456")
				_ = ds.Expire(key, time.Now())
			} else if choice == 3 {
				newKey := fmt.Sprintf("key%d", i)
				ds.Upsert(newKey, "def123")
			}
		}
	}
}
