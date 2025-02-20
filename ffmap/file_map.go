package ffmap

import (
	"errors"
)

// OpenCSV will create or read an existing CSV map file.
func OpenCSV(filename string) (*KeyValueCSV, error) {
	db := &KeyValueCSV{
		filename: filename,
		data:     make(map[string]dataItem),
	}
	if err := db.loadFromDisk(); err != nil {
		return nil, err
	} else {
		return db, nil
	}
}

// OpenReadOnlyCSV will read a CSV map file, providing a read only view of the data.
func OpenReadOnlyCSV(filename string) (FFMap, error) {
	return OpenCSV(filename)
}

type FFMap interface {
	// Size reports how many entries are stored in the map.
	Size() int
	// Get will set the value for the given key.  The returned bool indicates if the value was found and matches the
	// type, check the error for possible parsing or type errors.
	Get(key string, value interface{}) (bool, error)
	// ContainsKey will return true if the map has an associated value with the provided key.
	ContainsKey(key string) bool
	// KeySet will return all the keys stored within the map.
	KeySet() []string
}

type MutableFFMap interface {
	FFMap
	// Set will set the provided value into the map, when retrieved the same type must be used.  If a value already
	// exists, it will be replaced with the new value.
	Set(key string, value interface{}) error
	// Delete will remove the key from the map (if present).
	Delete(key string)
	// DeleteAll will clear or delete all entries from the map.
	DeleteAll()
	// Commit will update the disk representation to match the in-memory state.  If this is not invoked the disk will
	// never be updated.  This must not be called concurrently, and may be slow as the file format is optimized.
	Commit() error
}

// TypedFFMap provides a similar API to the interface MutableFFMap, however it only functions on a single value type.
type TypedFFMap[T any] struct {
	ffm MutableFFMap
}

// NewTypedFFMap provides a TypedFFMap which will operate with only the specific generic value type provided.
// If the underline map contains values of other types, an error will be returned when the value is attempted to be retrieved.
func NewTypedFFMap[T any](ffm MutableFFMap) *TypedFFMap[T] {
	return &TypedFFMap[T]{ffm}
}

// Size reports how many entries are stored in the map.
func (tfm *TypedFFMap[T]) Size() int {
	return tfm.ffm.Size()
}

// Get will set the value for the given key.  The returned bool indicates if the value was found, if false the returned
// value is nil or invalid for the type.
func (tfm *TypedFFMap[T]) Get(key string) (T, bool) {
	var val T
	ok, err := tfm.ffm.Get(key, &val)
	return val, ok && err == nil
}

// ContainsKey will return true if the map has an associated value with the provided key.
func (tfm *TypedFFMap[T]) ContainsKey(key string) bool {
	return tfm.ffm.ContainsKey(key)
}

// KeySet will return all the keys stored within the map.
func (tfm *TypedFFMap[T]) KeySet() []string {
	return tfm.ffm.KeySet()
}

// Set will set the provided value into the map.  If a value already exists, it will be replaced with the new value.
func (tfm *TypedFFMap[T]) Set(key string, value T) error {
	return tfm.ffm.Set(key, value)
}

// SetAll will iterate the provided map and set all the key values into the TypedFFMap. In the case of error, remaining
// values will still be set, with the returned error being a joined error (if multiple errors occurred).
func (tfm *TypedFFMap[T]) SetAll(m map[string]T) error {
	if kv, ok := tfm.ffm.(*KeyValueCSV); ok {
		return SetAll(kv, m)
	} else { // flexible for other interface implementations if one exists
		var errs []error
		for k, v := range m {
			if err := tfm.ffm.Set(k, v); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}
}

// Delete will remove the key from the map (if present).
func (tfm *TypedFFMap[T]) Delete(key string) {
	tfm.ffm.Delete(key)
}

// DeleteAll will clear or delete all entries from the map.
func (tfm *TypedFFMap[T]) DeleteAll() {
	tfm.ffm.DeleteAll()
}

// Commit will update the disk representation to match the in-memory state.  If this is not invoked the disk will
// never be updated.  This must not be called concurrently, and may be slow as the file format is optimized.
func (tfm *TypedFFMap[T]) Commit() error {
	return tfm.ffm.Commit()
}
