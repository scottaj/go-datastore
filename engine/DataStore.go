package engine

import (
	"sync"
	"time"
)

type dataNode struct {
	value         string
	hasExpiration bool
	expiration    time.Time
}

type DataStore struct {
	inMemoryStore      map[string]dataNode
	keyIndex           PrefixTrie
	internalStoreMutex sync.Mutex
}

func NewDataStore() DataStore {
	return DataStore{
		inMemoryStore: map[string]dataNode{},
		keyIndex:      NewPrefixTrie(),
	}
}

// Read
/*
* Read a value from the data store that has the provided key
*
* Returns the value of the key if it was present and the empty string "" if it was not.
* To clarify cases where the empty string could be the actual value, also returns a bool indicating if the key was
* present when reading
 */
func (ds *DataStore) Read(key string) (string, bool) {
	ds.internalStoreMutex.Lock()
	readValue, present := ds.inMemoryStore[key]
	ds.internalStoreMutex.Unlock()

	if readValue.hasExpiration && readValue.expiration.Before(time.Now()) {
		return "", false
	}
	return readValue.value, present
}

// ReadExpiration
/*
* Read an expiration from the data store that has the provided key
*
* Returns the expiration of the key if it was present and the and empty time value if it was not.
* If the key was present returns the expiration time of the key or the empty time (epoch) if there is no expiration
* To clarify cases where the empty time could be the actual value, also returns a bool indicating if the key was
* had an expiration set when reading
 */
func (ds *DataStore) ReadExpiration(key string) (time.Time, bool) {
	ds.internalStoreMutex.Lock()
	readValue, present := ds.inMemoryStore[key]
	ds.internalStoreMutex.Unlock()

	if !present || readValue.hasExpiration && readValue.expiration.Before(time.Now()) {
		return time.Time{}, false
	}
	return readValue.expiration, readValue.hasExpiration
}

// Present
/**
* Determine if the provided key is present in the data store
*
* returns a boolean indicating if the key was present or not
 */
func (ds *DataStore) Present(key string) bool {
	_, present := ds.Read(key)
	return present
}

// Insert
/*
* Insert the provided value into the data store under the provided key
*
* Will not overwrite an existing value if the key already exists.
*
* returns the value of the key in the data store and a boolean indicating if the new value was inserted. If the new
* value was not inserted because the key already existed this will return the current value of the key.
 */
func (ds *DataStore) Insert(key string, value string) bool {
	go ds.cleanupExpirations()
	valueExists := ds.Present(key)
	if !valueExists {
		ds.internalStoreMutex.Lock()
		ds.inMemoryStore[key] = dataNode{value: value}
		ds.keyIndex.Add(key)
		ds.internalStoreMutex.Unlock()
		return true
	}

	return false
}

// Update
/*
* Update the provided key in the datastore to have the new provided value
*
* This will not insert a new key if the key does not already exist in the data store.
*
* Returns the new value of the key and a boolean indicating if the update was successful. If the update was not
* successful it returns the empty string "" for the value.
 */
func (ds *DataStore) Update(key string, value string) bool {
	go ds.cleanupExpirations()
	valueExists := ds.Present(key)
	if valueExists {
		ds.internalStoreMutex.Lock()
		ds.inMemoryStore[key] = dataNode{value: value}
		ds.internalStoreMutex.Unlock()
		return true
	}

	return false
}

// Upsert
/**
* Insert the provided value for the provided key, or Update the value if the key already exists
*
* return the updated value of the key.
 */
func (ds *DataStore) Upsert(key string, value string) string {
	go ds.cleanupExpirations()

	ds.internalStoreMutex.Lock()
	ds.inMemoryStore[key] = dataNode{value: value}
	ds.keyIndex.Add(key)

	ds.internalStoreMutex.Unlock()

	return value
}

// Delete
/**
* Delete the provided key and its value from the data store
*
* returns a boolean indicating whether a value was deleted or not
 */
func (ds *DataStore) Delete(key string) bool {
	go ds.cleanupExpirations()
	valueExists := ds.Present(key)

	ds.internalStoreMutex.Lock()
	delete(ds.inMemoryStore, key)
	ds.keyIndex.Delete(key)
	ds.internalStoreMutex.Unlock()

	return valueExists
}

// Count
/**
* Count the number of keys in the datastore
*
* Count will return an approximation of the number of active keys, but for performance reasons it may count some
* expired keys that have not yet been cleaned up.
*
* returns the number of items in the datastore as an int
 */
func (ds *DataStore) Count() int {
	return len(ds.inMemoryStore)
}

// Truncate
/**
* Delete all values from the data store
 */
func (ds *DataStore) Truncate() {
	ds.internalStoreMutex.Lock()
	ds.inMemoryStore = map[string]dataNode{}
	ds.internalStoreMutex.Unlock()
}

// Expire
/**
* Sets an expiration time for a key
*
* Once the expiration time for a key passes it will behave as if it has been deleted. The actusal deletion of
* underlying expired data will happen asynchronously
 */
func (ds *DataStore) Expire(key string, expiration time.Time) bool {
	valueExists := ds.Present(key)
	if valueExists {
		ds.internalStoreMutex.Lock()
		valueToUpdate := ds.inMemoryStore[key]
		valueToUpdate.hasExpiration = true
		valueToUpdate.expiration = expiration
		ds.inMemoryStore[key] = valueToUpdate
		ds.internalStoreMutex.Unlock()

		return true
	}

	return false
}

// KeysBy
/**
* Find all keys in the datastore that match the provided prefix
*
* Only works on a complete prefix subset bounded by the delimiter. For exampke if you have a key "country:USA:state:MI"
* and a configured delimiter of ":"; then you could find that key with the searches "", "country", and "country:USA"
* but not the searches "cou", "country:", or "country:Canada"
*
* Return a slice of all the string keys that match the prefix
 */
func (ds *DataStore) KeysBy(prefix string) []string {
	allKeys := ds.keyIndex.Find(prefix)
	var unexpiredKeys []string
	for _, key := range allKeys {
		if ds.Present(key) {
			unexpiredKeys = append(unexpiredKeys, key)
		}
	}

	return unexpiredKeys
}

// DeleteBy
/**
* Delete all keys that match a provided prefix
*
* The same restrictions as to what constitute matching a key as described in KeysBy apply to this method
 */
func (ds *DataStore) DeleteBy(prefix string) int {
	keysToRemove := ds.KeysBy(prefix)
	ds.internalStoreMutex.Lock()
	ds.keyIndex.DeleteAll(prefix)
	for _, key := range keysToRemove {
		delete(ds.inMemoryStore, key)
	}
	ds.internalStoreMutex.Unlock()

	return len(keysToRemove)
}

// ExpireBy
/**
* Set the provided expiration on all keys matching the provided prefix
*
* The same restrictions as to what constitute matching a key as described in KeysBy apply to this method
 */
func (ds *DataStore) ExpireBy(prefix string, expiration time.Time) int {
	keysToExpire := ds.KeysBy(prefix)

	for _, key := range keysToExpire {
		ds.Expire(key, expiration)
	}

	return len(keysToExpire)
}

// cleanupExpirations
/**
* Cleans up expired items in the data store
*
* Internally this is run async whenever a modification is made to the data store
 */
func (ds *DataStore) cleanupExpirations() {
	ds.internalStoreMutex.Lock()
	timestamp := time.Now()
	for key, value := range ds.inMemoryStore {
		if value.hasExpiration && value.expiration.Before(timestamp) {
			delete(ds.inMemoryStore, key)
			ds.keyIndex.Delete(key)
		}
	}
	ds.internalStoreMutex.Unlock()
}
