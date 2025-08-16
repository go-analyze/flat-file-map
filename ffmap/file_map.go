package ffmap

import (
	"errors"
)

// EncodingError indicates a value cannot be encoded for storage.
type EncodingError struct {
	// Key provides the key being set during the encoding.
	Key string
	// Value provides the value that failed to encode.
	Value interface{}
	// Message provides the flat file map context of the failure.
	Message string
	// Err provides the underline error if it was sourced from another encoding (e.g. json.Marshal failure).
	Err error
}

func (e *EncodingError) Error() string {
	if e.Key != "" {
		return "encoding error for key \"" + e.Key + "\": " + e.Message
	}
	return "encoding error: " + e.Message
}

func (e *EncodingError) Unwrap() error {
	return e.Err
}

// TypeMismatchError indicates type conversion issues (including overflow).
type TypeMismatchError struct {
	// Key provides the key being retrieved.
	Key string
	// Message provides the context of the failure.
	Message string
}

func (e *TypeMismatchError) Error() string {
	if e.Key != "" {
		return "type mismatch error for key \"" + e.Key + "\": " + e.Message
	}
	return "type mismatch error: " + e.Message
}

// ValidationError indicates invalid input parameters or file format.
type ValidationError struct {
	// Message provides the context of the failure.
	Message string
	// Err provides the underline error if any.
	Err error
}

func (e *ValidationError) Error() string {
	return "validation error: " + e.Message
}

func (e *ValidationError) Unwrap() error {
	return e.Err
}

// OpenCSV will create or read an existing CSV map file.
func OpenCSV(filename string) (*KeyValueCSV, error) {
	db := &KeyValueCSV{
		filename: filename,
		memoryMap: &memoryJsonMap{
			data: make(map[string]dataItem),
		},
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

// NewMemoryMap creates a new in-memory map, this map behaves the same as persistent maps.
// This is primarily intended for testing, all operations are thread-safe and the Commit method is a no-op.
func NewMemoryMap() MutableFFMap {
	return &memoryJsonMap{
		data: make(map[string]dataItem),
	}
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
	// never be updated.  The operation may be slow as the file format is optimized.
	Commit() error
}

// Deprecated: SetAll is deprecated, use SetMapValues.
func SetAll[T any](kv MutableFFMap, m map[string]T) error {
	return SetMapValues(kv, m)
}

// SetMapValues will iterate the provided map and set all the key values into the provided MutableFFMap.
// In the case of error, remaining values will still be set, with the returned error being a joined error
// (if multiple errors occurred).
func SetMapValues[T any](kv MutableFFMap, m map[string]T) error {
	if kv == nil {
		return &ValidationError{Message: "nil MutableFFMap"}
	}

	if csvKV, ok := kv.(*KeyValueCSV); ok {
		return csvSetMapValues(csvKV, m)
	} else { // flexible for other interface implementations if one exists
		var errs []error
		for k, v := range m {
			if err := kv.Set(k, v); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}
}

// SetSliceValues will iterate the provided slice, and using the provided function to derive the key for each slice,
// set the values into the provided MutableFFMap. In the case of error, remaining values will still be set,
// with the returned error being a joined error (if multiple errors occurred).
func SetSliceValues[T any](kv MutableFFMap, s []T, keyProvider func(value T) string) error {
	if kv == nil {
		return &ValidationError{Message: "nil MutableFFMap"}
	} else if keyProvider == nil {
		return &ValidationError{Message: "nil keyProvider function"}
	}

	if csvKV, ok := kv.(*KeyValueCSV); ok {
		return csvSetSliceValues(csvKV, s, keyProvider)
	} else { // flexible for other interface implementations if one exists
		var errs []error
		for _, v := range s {
			if err := kv.Set(keyProvider(v), v); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}
}

// TypedFFMap provides a similar API to the interface MutableFFMap, however it only functions on a single value type.
type TypedFFMap[T any] struct {
	ffm MutableFFMap
}

// NewTypedFFMap provides a TypedFFMap which will operate with only the specific generic value type provided.
// If the underlying map contains values of other types, an error will be returned when the value is attempted to be retrieved.
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

// Deprecated: SetAll is deprecated, use SetMapValues.
func (tfm *TypedFFMap[T]) SetAll(m map[string]T) error {
	return tfm.SetMapValues(m)
}

// SetMapValues will iterate the provided map and set all the key values into the TypedFFMap. In the case of error,
// remaining values will still be set, with the returned error being a joined error (if multiple errors occurred).
func (tfm *TypedFFMap[T]) SetMapValues(m map[string]T) error {
	return SetMapValues(tfm.ffm, m)
}

// SetSliceValues will iterate the provided slice, and using the provided function to derive the key for each slice,
// set the values into the TypedFFMap. In the case of error, remaining values will still be set, with the returned
// error being a joined error (if multiple errors occurred).
func (tfm *TypedFFMap[T]) SetSliceValues(s []T, keyProvider func(value T) string) error {
	return SetSliceValues(tfm.ffm, s, keyProvider)
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
// never be updated.  The operation may be slow as the file format is optimized.
func (tfm *TypedFFMap[T]) Commit() error {
	return tfm.ffm.Commit()
}
