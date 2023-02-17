package datastore

var inMemoryStore = map[string]string{}

// Read
/*
* Read a value from the data store that has the provided key
*
* Returns the value of the key if it was present and the empty string "" if it was not.
* To clarify cases where the empty string could be the actual value,also returns a bool indicating if the key was
* present when reading
 */
func Read(key string) (string, bool) {
	readValue, present := inMemoryStore[key]
	return readValue, present
}

// Present
/**
* Determine if the provided key is present in the data store
*
* returns a boolean indicating if the key was present or not
 */
func Present(key string) bool {
	_, present := Read(key)
	return present
}

// Insert
/*
* Insert the provided value into the data stroe under the provided key
*
* Will not overwirte an existing value if the key already exists
*
* returns the value of the key in the data store and a boolean indicating if the new value was inserted. If the new
* value was not inserted because the key already existed this will return the current value of the key.
 */
func Insert(key string, value string) (string, bool) {
	existingValue, valueExists := inMemoryStore[key]
	if !valueExists {
		inMemoryStore[key] = value
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
	_, valueExists := inMemoryStore[key]
	if valueExists {
		inMemoryStore[key] = value
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
	inMemoryStore[key] = value
	return value
}

// Delete
/**
* Delete the provided key and its value from the data store
*
* returns a boolean indicating whether a value was deleted or not
 */
func Delete(key string) bool {
	_, valueExists := inMemoryStore[key]
	delete(inMemoryStore, key)
	return valueExists
}

// Count
/**
* Count the number of keys in the datastore
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
