package datastore

import (
	"sync"
	"time"
)

var inMemoryStore = map[string]string{}
var expirationTracker = map[string]time.Time{}
var internalStoreMutex = sync.Mutex{}

// Read
/*
* Read a value from the data store that has the provided key
*
* Returns the value of the key if it was present and the empty string "" if it was not.
* If the key was present returns the expiration time of the key or the empty time (epoch) if there is no expiration
* To clarify cases where the empty string could be the actual value,also returns a bool indicating if the key was
* present when reading
 */
func Read(key string) (string, time.Time, bool) {
	internalStoreMutex.Lock()
	readValue, present := inMemoryStore[key]
	expiration, expirationPresent := expirationTracker[key]
	internalStoreMutex.Unlock()

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
func Present(key string) bool {
	_, _, present := Read(key)
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
func Insert(key string, value string) (string, bool) {
	go cleanupExpirations()
	existingValue, _, valueExists := Read(key)
	if !valueExists {
		internalStoreMutex.Lock()
		inMemoryStore[key] = value
		delete(expirationTracker, key)
		internalStoreMutex.Unlock()
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
func Update(key string, value string) (string, bool) {
	go cleanupExpirations()
	valueExists := Present(key)
	if valueExists {
		internalStoreMutex.Lock()
		inMemoryStore[key] = value
		internalStoreMutex.Unlock()
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
func Upsert(key string, value string) string {
	go cleanupExpirations()
	valueExists := Present(key)

	internalStoreMutex.Lock()
	inMemoryStore[key] = value

	if !valueExists {
		delete(expirationTracker, key)
	}
	internalStoreMutex.Unlock()

	return value
}

// Delete
/**
* Delete the provided key and its value from the data store
*
* returns a boolean indicating whether a value was deleted or not
 */
func Delete(key string) bool {
	go cleanupExpirations()
	valueExists := Present(key)

	internalStoreMutex.Lock()
	delete(inMemoryStore, key)
	internalStoreMutex.Unlock()

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
func Count() int {
	return len(inMemoryStore)
}

// Truncate
/**
* Delete all values from the data store
 */
func Truncate() {
	inMemoryStore = map[string]string{}
}

// Expire
/**
* Sets an expiration time for a key
*
* Once the expiration time for a key passes it will behave as if it has been deleted. The actusal deletion of
* underlying expired data will happen asyncronously
 */
func Expire(key string, expiration time.Time) bool {
	valueExists := Present(key)
	if valueExists {
		expirationTracker[key] = expiration
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
func cleanupExpirations() {
	timestamp := time.Now()
	internalStoreMutex.Lock()
	for key, expiration := range expirationTracker {
		if expiration.Before(timestamp) {
			delete(expirationTracker, key)
			delete(inMemoryStore, key)
		}
	}
	internalStoreMutex.Unlock()
}
