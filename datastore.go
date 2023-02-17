package datastore

import (
	"sync"
	"time"
)

type DataStore struct {
	inMemoryStore      map[string]string
	expirationTracker  map[string]time.Time
	internalStoreMutex sync.Mutex
}

func New() DataStore {
	return DataStore{
		inMemoryStore:     map[string]string{},
		expirationTracker: map[string]time.Time{},
	}
}

// Read
/*
* Read a value from the data store that has the provided key
*
* Returns the value of the key if it was present and the empty string "" if it was not.
* If the key was present returns the expiration time of the key or the empty time (epoch) if there is no expiration
* To clarify cases where the empty string could be the actual value,also returns a bool indicating if the key was
* present when reading
 */
func (ds *DataStore) Read(key string) (string, time.Time, bool) {
	ds.internalStoreMutex.Lock()
	readValue, present := ds.inMemoryStore[key]
	expiration, expirationPresent := ds.expirationTracker[key]
	ds.internalStoreMutex.Unlock()

	if expirationPresent && expiration.Before(time.Now()) {
		return "", time.Time{}, false
	}
	return readValue, expiration, present
}

// Present
/**
* Determine if the provided key is present in the data store
*
* returns a boolean indicating if the key was present or not
 */
func (ds *DataStore) Present(key string) bool {
	_, _, present := ds.Read(key)
	return present
}

// Insert
/*
* Insert the provided value into the data stroe under the provided key
*
* Will not overwrite an existing value if the key already exists.
*
* returns the value of the key in the data store and a boolean indicating if the new value was inserted. If the new
* value was not inserted because the key already existed this will return the current value of the key.
 */
func (ds *DataStore) Insert(key string, value string) (string, bool) {
	go ds.cleanupExpirations()
	existingValue, _, valueExists := ds.Read(key)
	if !valueExists {
		ds.internalStoreMutex.Lock()
		ds.inMemoryStore[key] = value
		delete(ds.expirationTracker, key)
		ds.internalStoreMutex.Unlock()
		return value, true
	}

	return existingValue, false
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
func (ds *DataStore) Update(key string, value string) (string, bool) {
	go ds.cleanupExpirations()
	valueExists := ds.Present(key)
	if valueExists {
		ds.internalStoreMutex.Lock()
		ds.inMemoryStore[key] = value
		ds.internalStoreMutex.Unlock()
		return value, true
	}

	return "", false
}

// Upsert
/**
* Insert the provided value for the provided key, or Update the value if the key already exists
*
* return the updated value of the key.
 */
func (ds *DataStore) Upsert(key string, value string) string {
	go ds.cleanupExpirations()
	valueExists := ds.Present(key)

	ds.internalStoreMutex.Lock()
	ds.inMemoryStore[key] = value

	if !valueExists {
		delete(ds.expirationTracker, key)
	}
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
	delete(ds.expirationTracker, key)
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
	// TODO needs some love
	ds.inMemoryStore = map[string]string{}
}

// Expire
/**
* Sets an expiration time for a key
*
* Once the expiration time for a key passes it will behave as if it has been deleted. The actusal deletion of
* underlying expired data will happen asyncronously
 */
func (ds *DataStore) Expire(key string, expiration time.Time) bool {
	valueExists := ds.Present(key)
	if valueExists {
		ds.expirationTracker[key] = expiration
		return true
	}

	return false
}

// cleanupExpirations
/**
* Cleans up expired items in the data store
*
* Internally this is run async whenever a modification is made to the data store
 */
func (ds *DataStore) cleanupExpirations() {
	timestamp := time.Now()
	ds.internalStoreMutex.Lock()
	for key, expiration := range ds.expirationTracker {
		if expiration.Before(timestamp) {
			delete(ds.expirationTracker, key)
			delete(ds.inMemoryStore, key)
		}
	}
	ds.internalStoreMutex.Unlock()
}
