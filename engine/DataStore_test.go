package engine

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestInsertAndRead(t *testing.T) {
	ds := NewDataStore()

	data := "abc123"
	key := "testkey"
	success := ds.Insert(key, data)
	if success == false {
		t.Fatalf("failed to insert key %q", key)
	}

	readValue, present := ds.Read(key)
	if readValue != data || present == false {
		t.Fatalf("failed to read value %q from key %q got %q", data, key, readValue)
	}
}

func TestInsertDuplicate(t *testing.T) {
	ds := NewDataStore()

	data := "abc123"
	key := "testkey"
	success := ds.Insert(key, data)
	if success == false {
		t.Fatalf("failed to insert key %q", key)
	}

	updatedData := "def456"
	success = ds.Insert(key, updatedData)
	setValue, _ := ds.Read(key)
	if success == true {
		t.Fatalf("expected data %q not to be overwritten but is now %q", data, setValue)
	}
}

func TestReadAbsent(t *testing.T) {
	ds := NewDataStore()

	value, present := ds.Read("def456")
	if value != "" || present == true {
		t.Fatalf("expected no value but found %q", value)
	}
}

func TestReadEmptyString(t *testing.T) {
	ds := NewDataStore()

	data := ""
	key := "testkey"
	success := ds.Insert(key, data)
	if success == false {
		t.Fatalf("failed to insert key %q", key)
	}

	readValue, present := ds.Read(key)
	if readValue != data || present == false {
		t.Fatalf("failed to read value %q from key %q got %q", data, key, readValue)
	}
}

func TestReadExpiration(t *testing.T) {
	ds := NewDataStore()

	data := ""
	key := "testkey"
	ds.Insert(key, data)

	readExpiration, present := ds.ReadExpiration(key)
	if present {
		t.Fatalf("expected no expiration but found %q", readExpiration)
	}

	expireTime := time.Now().Add(time.Second * 10)
	ds.Expire(key, expireTime)

	readExpiration, present = ds.ReadExpiration(key)
	if !present || readExpiration != expireTime {
		t.Fatalf("expected key %q to have expiration %q", key, readExpiration)
	}
}

func TestUpdateExistingValueAndRead(t *testing.T) {
	ds := NewDataStore()

	data := "abc123"
	key := "testkey"
	_ = ds.Insert(key, data)

	updatedData := "def456"
	success := ds.Update(key, updatedData)

	if success == false {
		t.Fatalf("expected value for key %q to be updated", key)
	}

	readValue, _ := ds.Read(key)
	if readValue != updatedData {
		t.Fatalf("expected to read updated value %q but was %q", updatedData, readValue)
	}
}

func TestUpdateAbsentValueAndRead(t *testing.T) {
	ds := NewDataStore()

	key := "testkey"

	updatedData := "def456"
	success := ds.Update(key, updatedData)

	if success == true {
		t.Fatalf("expected update not to work")
	}

	readValue, present := ds.Read(key)
	if readValue == updatedData || present == true {
		t.Fatalf("expected update not to work but read value %q", readValue)
	}
}

func TestUpsertNewValueAndUpdateIt(t *testing.T) {
	ds := NewDataStore()

	data := "abc123"
	key := "testkey"

	value := ds.Upsert(key, data)
	if value != true {
		t.Fatalf("expected upsert to insert new data %t", value)
	}
	readValue, present := ds.Read(key)
	if readValue != data || present == false {
		t.Fatalf("expected update to work but read value %q", readValue)
	}

	updatedData := "def456"
	value = ds.Upsert(key, updatedData)
	if value != true {
		t.Fatalf("expected upsert to update existing data %t", value)
	}

	value = ds.Upsert(key, updatedData)
	if value != false {
		t.Fatalf("expected upsert to make no change because value was the same %t", value)
	}

	readValue, present = ds.Read(key)
	if readValue != updatedData || present == false {
		t.Fatalf("expected update to work but read value %q", readValue)
	}
}

func TestDeleteExistingValue(t *testing.T) {
	ds := NewDataStore()

	data := "abc123"
	key := "testkey"

	_ = ds.Upsert(key, data)

	present := ds.Delete(key)

	if present == false {
		t.Fatalf("failed to delete key %q", key)
	}

	_, present = ds.Read(key)
	if present == true {
		t.Fatalf("Expected key %q to be deleted but was able to read it", key)
	}
}

func TestDeleteAbsentValue(t *testing.T) {
	ds := NewDataStore()

	key := "testkey"

	present := ds.Delete(key)

	if present == true {
		t.Fatalf("deleted key %q that should not have been present", key)
	}

	_, present = ds.Read(key)
	if present == true {
		t.Fatalf("Expected key %q to be deleted but was able to read it", key)
	}
}

func TestInsertAndPresent(t *testing.T) {
	ds := NewDataStore()

	data := "abc123"
	key := "testkey"

	present := ds.Present(key)
	if present == true {
		t.Fatalf("expected key %q not to exist but it did", key)
	}

	success := ds.Insert(key, data)
	if success == false {
		t.Fatalf("failed to insert key %q", key)
	}

	present = ds.Present(key)
	if present == false {
		t.Fatalf("expected key %q to exist but it didn't", key)
	}
}

func TestCount(t *testing.T) {
	ds := NewDataStore()

	count := ds.Count()
	if count != 0 {
		t.Fatalf("expected count 0 but was %q", count)
	}

	ds.Insert("a", "1")
	count = ds.Count()
	if count != 1 {
		t.Fatalf("expected count 1 but was %q", count)
	}

	ds.Insert("a", "1")
	count = ds.Count()
	if count != 1 {
		t.Fatalf("expected count 1 but was %q", count)
	}

	ds.Insert("b", "2")
	count = ds.Count()
	if count != 2 {
		t.Fatalf("expected count 2 but was %q", count)
	}

	ds.Update("a", "3")
	count = ds.Count()
	if count != 2 {
		t.Fatalf("expected count 2 but was %q", count)
	}

	ds.Delete("a")
	count = ds.Count()
	if count != 1 {
		t.Fatalf("expected count 1 but was %q", count)
	}
}

func TestReadExpiredValue(t *testing.T) {
	ds := NewDataStore()

	data := "abc123"
	key := "testkey"
	ds.Insert(key, data)

	expiration := time.Now().Add(time.Millisecond * 100).UTC()
	success := ds.Expire(key, expiration)
	if success != true {
		t.Fatalf("Failed to set expiration %q for key %q", expiration, key)
	}

	readValue, present := ds.Read(key)
	if readValue != data || present == false {
		t.Fatalf("failed to read value %q with expiration %q from key %q got %q", data, expiration, key, readValue)
	}

	time.Sleep(time.Millisecond * 100)

	_, present = ds.Read(key)
	if present == true {
		t.Fatalf("expected to not find expired value for key %q", key)
	}
}

func TestExpireNonExistentKey(t *testing.T) {
	ds := NewDataStore()

	success := ds.Expire("xyz987", time.Now())
	if success == true {
		t.Fatalf("did not expect to be able to expire non existing key")
	}
}

func TestInsertExpiredKeyRemovesExpiration(t *testing.T) {
	ds := NewDataStore()

	data := "abc123"
	key := "testkey"
	ds.Insert(key, data)

	expiration := time.Now().Add(time.Millisecond * 100).UTC()
	_ = ds.Expire(key, expiration)

	time.Sleep(time.Millisecond * 100)

	_, present := ds.Read(key)
	if present == true {
		t.Fatalf("expected to not find expired value for key %q", key)
	}

	newData := "def456"
	ds.Insert(key, newData)
	// TODO ReadExpired
	readValue, present := ds.Read(key)
	readExpiration, _ := ds.ReadExpiration(key)
	if readValue != newData || !readExpiration.IsZero() || present == false {
		t.Fatalf("expected to find value %q for key %q with no expiration, but it had value %q with expiration %q", newData, key, readValue, readExpiration)
	}
}

func TestUpsertExpiredKeyRemovesExpiration(t *testing.T) {
	ds := NewDataStore()

	data := "abc123"
	key := "testkey"
	_ = ds.Upsert(key, data)

	expiration := time.Now().Add(time.Millisecond * 100).UTC()
	_ = ds.Expire(key, expiration)

	time.Sleep(time.Millisecond * 100)

	present := ds.Present(key)
	if present == true {
		t.Fatalf("expected to not find expired value for key %q", key)
	}

	newData := "def456"
	_ = ds.Upsert(key, newData)
	readValue, present := ds.Read(key)
	readExpiration, _ := ds.ReadExpiration(key)
	if readValue != newData || !readExpiration.IsZero() || present == false {
		t.Fatalf("expected to find value %q for key %q with no expiration, but it had value %q with expiration %q", newData, key, readValue, readExpiration)
	}
}

func TestDeleteKeyWithExpirationThenRecreateItRemovesExpiration(t *testing.T) {
	ds := NewDataStore()

	data := "abc123"
	key := "testkey"
	ds.Insert(key, data)

	expiration := time.Now().Add(time.Millisecond * 100).UTC()
	_ = ds.Expire(key, expiration)

	_ = ds.Delete(key)

	newData := "def456"
	ds.Insert(key, newData)

	time.Sleep(time.Millisecond * 100)

	readValue, present := ds.Read(key)
	readExpiration, _ := ds.ReadExpiration(key)
	if readValue != newData || !readExpiration.IsZero() || present == false {
		t.Fatalf("expected to find value %q for key %q with no expiration, but it had value %q with expiration %q", newData, key, readValue, readExpiration)
	}
}

func TestInsertTriggersAsyncExpirationCleanup(t *testing.T) {
	ds := NewDataStore()

	key1, data1 := "key1", "abc123"
	key2, data2 := "key2", "abc456"
	key3, data3 := "key3", "def123"
	key4, data4 := "key4", "def456"

	ds.Insert(key1, data1)
	ds.Insert(key2, data2)
	ds.Insert(key3, data3)

	expiration := time.Now().Add(time.Millisecond * 100)

	ds.Expire(key1, expiration)
	ds.Expire(key2, expiration)
	ds.Expire(key3, expiration)

	time.Sleep(time.Millisecond * 100)

	count := ds.Count()
	if count != 3 {
		t.Fatalf("expected count to be 3 because there was no write to cleanup but was %d", count)
	}

	ds.Insert(key4, data4)

	time.Sleep(time.Millisecond * 10)

	count = ds.Count()
	if count != 1 {
		t.Fatalf("expected count to be 1 because write cause cleanup but was %d", count)
	}
}

func TestUpdateTriggersAsyncExpirationCleanup(t *testing.T) {
	ds := NewDataStore()

	key1, data1 := "key1", "abc123"
	key2, data2 := "key2", "abc456"
	key3, data3 := "key3", "def123"

	ds.Insert(key1, data1)
	ds.Insert(key2, data2)
	ds.Insert(key3, data3)

	expiration := time.Now().Add(time.Millisecond * 100)

	ds.Expire(key1, expiration)
	ds.Expire(key2, expiration)

	time.Sleep(time.Millisecond * 100)

	count := ds.Count()
	if count != 3 {
		t.Fatalf("expected count to be 3 because there was no write to cleanup but was %d", count)
	}

	ds.Update(key3, data1)

	time.Sleep(time.Millisecond * 10)

	count = ds.Count()
	if count != 1 {
		t.Fatalf("expected count to be 1 because write cause cleanup but was %d", count)
	}
}

func TestUpsertTriggersAsyncExpirationCleanup(t *testing.T) {
	ds := NewDataStore()

	key1, data1 := "key1", "abc123"
	key2, data2 := "key2", "abc456"
	key3, data3 := "key3", "def123"
	key4, data4 := "key4", "def456"

	ds.Insert(key1, data1)
	ds.Insert(key2, data2)
	ds.Insert(key3, data3)

	expiration := time.Now().Add(time.Millisecond * 100)

	ds.Expire(key1, expiration)
	ds.Expire(key2, expiration)
	ds.Expire(key3, expiration)

	time.Sleep(time.Millisecond * 100)

	count := ds.Count()
	if count != 3 {
		t.Fatalf("expected count to be 3 because there was no write to cleanup but was %d", count)
	}

	ds.Upsert(key4, data4)

	time.Sleep(time.Millisecond * 10)

	count = ds.Count()
	if count != 1 {
		t.Fatalf("expected count to be 1 because write cause cleanup but was %d", count)
	}
}

func TestDeleteTriggersAsyncExpirationCleanup(t *testing.T) {
	ds := NewDataStore()

	key1, data1 := "key1", "abc123"
	key2, data2 := "key2", "abc456"
	key3, data3 := "key3", "def123"
	key4, data4 := "key4", "def456"

	ds.Insert(key1, data1)
	ds.Insert(key2, data2)
	ds.Insert(key3, data3)
	ds.Insert(key4, data4)

	expiration := time.Now().Add(time.Millisecond * 100)

	ds.Expire(key1, expiration)
	ds.Expire(key2, expiration)
	ds.Expire(key3, expiration)

	time.Sleep(time.Millisecond * 100)

	count := ds.Count()
	if count != 4 {
		t.Fatalf("expected count to be 4 because there was no write to cleanup but was %d", count)
	}

	ds.Delete(key4)

	time.Sleep(time.Millisecond * 10)

	count = ds.Count()
	if count != 0 {
		t.Fatalf("expected count to be 0 because write cause cleanup but was %d", count)
	}
}

func TestThreadSafetyOfWriteOperationsWithAsyncCleanup(t *testing.T) {
	ds := NewDataStore()

	// Without mutexes on updates to the internal data store this test will crash
	for i := 0; i < 1000; i++ {
		if i%4 == 0 {
			// ds.Insert with expiration
			key := fmt.Sprintf("key%d", i)
			ds.Insert(key, "abc123")
			_ = ds.Expire(key, time.Now())
		} else if i%4 == 1 {
			// insert without expiration
			key := fmt.Sprintf("key%d", i)
			ds.Insert(key, "abc123")
		} else if i%4 == 2 {
			// update inserted value from last clause
			key := fmt.Sprintf("key%d", i-1)
			ds.Update(key, "def456")
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

	ds.Truncate()
}

func TestTruncate(t *testing.T) {
	ds := NewDataStore()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		ds.Insert(key, "abc123")
	}

	count := ds.Count()
	if count != 100 {
		t.Fatalf("Expected 100 items but found %d", count)
	}

	ds.Truncate()
	count = ds.Count()
	if count != 0 {
		t.Fatalf("Expected 0 items but found %d", count)
	}
}

func TestFindingKeysByPrefix(t *testing.T) {
	ds := NewDataStore()

	data := "abc123"

	key0 := "region:1:store:1:employee:1"
	key1 := "region:1:store:1:employee:2"
	key2 := "region:1:manager"
	key3 := "region:1:store:2:employee:4"
	key4 := "region:1:store:3:employee:2"
	key5 := "region:1:store:1"
	key6 := "region:2:store:4:employee:7"
	key7 := "region:2:store:4:employee:8"
	key8 := "region:2:store:5:employee:7"
	key9 := "category:3:product:7"

	ds.Insert(key0, data)
	ds.Insert(key1, data)
	ds.Insert(key2, data)
	ds.Insert(key3, data)
	ds.Insert(key4, data)
	ds.Insert(key5, data)
	ds.Insert(key6, data)
	ds.Insert(key7, data)
	ds.Insert(key8, data)
	ds.Upsert(key9, data)

	allKeys := ds.KeysBy("")
	if len(allKeys) != 10 {
		t.Fatalf("expected 10 keys but found %d: %q", len(allKeys), allKeys)
	}

	regionKeys := ds.KeysBy("region")
	if len(regionKeys) != 9 {
		t.Fatalf("expected 9 keys but found %d: %q", len(regionKeys), regionKeys)
	}

	store1Keys := ds.KeysBy("region:1:store:1")
	if len(store1Keys) != 3 {
		t.Fatalf("expected 3 keys but found %d: %q", len(store1Keys), store1Keys)
	}

	noKeys := ds.KeysBy("region:5")
	if noKeys != nil {
		t.Fatalf("expected no keys but found %d: %q", len(noKeys), noKeys)
	}
}

func TestPrefixSearchUpdatedOnDelete(t *testing.T) {
	ds := NewDataStore()

	data := "abc123"

	key0 := "region:1:store:1:employee:1"
	key1 := "region:1:store:1:employee:2"
	key2 := "region:1:manager"

	ds.Insert(key0, data)
	ds.Insert(key1, data)
	ds.Insert(key2, data)

	ds.Delete(key1)

	allKeys := ds.KeysBy("")
	if len(allKeys) != 2 {
		t.Fatalf("expected 2 keys but found %d: %q", len(allKeys), allKeys)
	}
}

func TestPrefixSearchUpdatedOnExpire(t *testing.T) {
	ds := NewDataStore()

	data := "abc123"

	key0 := "region:1:store:1:employee:1"
	key1 := "region:1:store:1:employee:2"
	key2 := "region:1:manager"

	ds.Insert(key0, data)
	ds.Insert(key1, data)
	ds.Insert(key2, data)

	ds.Expire(key1, time.Now())
	time.Sleep(time.Millisecond * 10)

	allKeys := ds.KeysBy("")
	if len(allKeys) != 2 {
		t.Fatalf("expected 2 keys but found %d: %q", len(allKeys), allKeys)
	}
}

func TestDeleteKeysByPrefix(t *testing.T) {
	ds := NewDataStore()

	data := "abc123"

	key0 := "region:1:store:1:employee:1"
	key1 := "region:1:store:1:employee:2"
	key2 := "region:1:manager"
	key3 := "region:1:store:2:employee:4"
	key4 := "region:1:store:3:employee:2"
	key5 := "region:1:store:1"
	key6 := "region:2:store:4:employee:7"
	key7 := "region:2:store:4:employee:8"
	key8 := "region:2:store:5:employee:7"
	key9 := "category:3:product:7"

	ds.Insert(key0, data)
	ds.Insert(key1, data)
	ds.Insert(key2, data)
	ds.Insert(key3, data)
	ds.Insert(key4, data)
	ds.Insert(key5, data)
	ds.Insert(key6, data)
	ds.Insert(key7, data)
	ds.Insert(key8, data)
	ds.Upsert(key9, data)

	deletedCount := ds.DeleteBy("region:5")
	allKeys := ds.KeysBy("")
	if deletedCount != 0 || len(allKeys) != 10 {
		t.Fatalf("expected 10 keys left but found %d: %q", len(allKeys), allKeys)
	}

	deletedCount = ds.DeleteBy("region:1:store:1")
	notStore1Keys := ds.KeysBy("")
	if deletedCount != 3 || len(notStore1Keys) != 7 {
		t.Fatalf("expected 7 keys left but found %d: %q", len(notStore1Keys), notStore1Keys)
	}

	deletedCount = ds.DeleteBy("region")
	notRegionKeys := ds.KeysBy("")
	if deletedCount != 6 || len(notRegionKeys) != 1 {
		t.Fatalf("expected 1 left keys but found %d: %q", len(notRegionKeys), notRegionKeys)
	}

	deletedCount = ds.DeleteBy("")
	noKeys := ds.KeysBy("")
	if deletedCount != 1 || noKeys != nil {
		t.Fatalf("expected no keys left but found %d: %q", len(noKeys), noKeys)
	}
}

func TestExpireKeysByPrefix(t *testing.T) {
	ds := NewDataStore()

	data := "abc123"

	key0 := "region:1:store:1:employee:1"
	key1 := "region:1:store:1:employee:2"
	key2 := "region:1:manager"
	key3 := "region:1:store:2:employee:4"
	key4 := "region:1:store:3:employee:2"
	key5 := "region:1:store:1"
	key6 := "region:2:store:4:employee:7"
	key7 := "region:2:store:4:employee:8"
	key8 := "region:2:store:5:employee:7"
	key9 := "category:3:product:7"

	ds.Insert(key0, data)
	ds.Insert(key1, data)
	ds.Insert(key2, data)
	ds.Insert(key3, data)
	ds.Insert(key4, data)
	ds.Insert(key5, data)
	ds.Insert(key6, data)
	ds.Insert(key7, data)
	ds.Insert(key8, data)
	ds.Upsert(key9, data)

	expiredCount := ds.ExpireBy("region:5", time.Now().Add(time.Millisecond*5))
	time.Sleep(time.Millisecond * 10)

	allKeys := ds.KeysBy("")
	if expiredCount != 0 || len(allKeys) != 10 {
		t.Fatalf("expected 10 keys left but found %d: %q", len(allKeys), allKeys)
	}

	expiredCount = ds.ExpireBy("region:1:store:1", time.Now().Add(time.Millisecond*5))
	time.Sleep(time.Millisecond * 10)

	notStore1Keys := ds.KeysBy("")
	if expiredCount != 3 || len(notStore1Keys) != 7 {
		t.Fatalf("expected 7 keys left but found %d: %q", len(notStore1Keys), notStore1Keys)
	}

	expiredCount = ds.ExpireBy("region", time.Now().Add(time.Millisecond*5))
	time.Sleep(time.Millisecond * 10)

	notRegionKeys := ds.KeysBy("")
	if expiredCount != 6 || len(notRegionKeys) != 1 {
		t.Fatalf("expected 1 key left but found %d: %q", len(notRegionKeys), notRegionKeys)
	}

	expiredCount = ds.ExpireBy("", time.Now().Add(time.Millisecond*5))
	time.Sleep(time.Millisecond * 10)

	noKeys := ds.KeysBy("")
	if expiredCount != 1 || noKeys != nil {
		t.Fatalf("expected no keys left but found %d: %q", len(noKeys), noKeys)
	}
}

func TestUpdateAndUpsertDoNotRemoveExpirations(t *testing.T) {
	ds := NewDataStore()
	key, value := "key1", "abc123"
	ds.Insert(key, value)
	expiration := time.Now().Add(time.Minute * 30)
	ds.Expire(key, expiration)

	readExpiration, present := ds.ReadExpiration(key)
	if present != true || readExpiration != expiration {
		t.Fatalf("Expected expiration to be set to %q but was not: %q", expiration, readExpiration)
	}

	ds.Update(key, "def456")
	readExpiration, present = ds.ReadExpiration(key)
	if present != true || readExpiration != expiration {
		t.Fatalf("Expected expiration to be set to %q but was not: %q", expiration, readExpiration)
	}

	ds.Upsert(key, "ghi789")
	readExpiration, present = ds.ReadExpiration(key)
	if present != true || readExpiration != expiration {
		t.Fatalf("Expected expiration to be set to %q but was not: %q", expiration, readExpiration)
	}
}
