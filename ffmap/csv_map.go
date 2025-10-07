package ffmap

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"syscall"
)

// KeyValueCSV provides a primarily in-memory key-value map with the ability to load and commit the contents to disk.
type KeyValueCSV struct {
	filename  string
	memoryMap *memoryJsonMap
	commitMod int
}

const currentFileVersion = "ver:0"

// loadFromDisk updates the map with data from disk.
func (kv *KeyValueCSV) loadFromDisk() error {
	kv.memoryMap.rwLock.Lock()
	defer kv.memoryMap.rwLock.Unlock()

	file, err := os.OpenFile(kv.filename, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer func() { _ = file.Close() }()

	return kv.loadFromReader(file)
}

// loadFromReader parses CSV data from a reader and loads it into the memory map.
func (kv *KeyValueCSV) loadFromReader(r io.Reader) error {
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1 // disable check, field counts will vary
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	return kv.loadRecords(records)
}

// loadRecords processes parsed CSV records and populates the memory map.
func (kv *KeyValueCSV) loadRecords(records [][]string) error {
	var currStructName string
	var currStructValueNames []string
	for i, record := range records {
		if i == 0 { // header line
			if len(record) < 1 || currentFileVersion != record[0] {
				return &ValidationError{Message: "invalid header line: " + strings.Join(record, ",")}
			}
			continue
		} else if len(record) == 0 {
			continue
		}

		dataType, err := strconv.Atoi(record[0])
		if err != nil {
			return &ValidationError{Message: "unexpected data type: %s" + record[0], Err: err}
		}
		switch dataType {
		case dataStructHeader:
			if len(record) < 3 {
				return &ValidationError{Message: fmt.Sprintf("unexpected csv struct header column count: %v, line: %v", len(record), i+1)}
			}
			currStructName = record[1]
			currStructValueNames = record[2:]
		case dataStructValue:
			if len(record) != 3 {
				return &ValidationError{Message: fmt.Sprintf("unexpected csv struct value column count: %v, line: %v", len(record), i+1)}
			}
			var values []interface{}
			if err := json.Unmarshal([]byte(record[2]), &values); err != nil {
				return err
			} else if len(values) != len(currStructValueNames) {
				return &ValidationError{Message: fmt.Sprintf("unexpected encoded json value count: %v/%v, line: %v", len(values), len(currStructValueNames), i+1)}
			}
			structValue := make(map[string]interface{}, len(currStructValueNames))
			for j, name := range currStructValueNames {
				structValue[name] = values[j]
			}
			encodedStruct, err := json.Marshal(structValue)
			if err != nil {
				return err
			}
			kv.memoryMap.data[record[1]] = dataItem{dataType: dataStructJson, structId: currStructName, value: string(encodedStruct)}
		default:
			if len(record) != 3 {
				return &ValidationError{Message: fmt.Sprintf("unexpected csv db column count: %v, type: %v, line: %v", len(record), dataType, i+1)}
			}
			kv.memoryMap.data[record[1]] = dataItem{dataType: dataType, value: record[2]}
		}
	}
	return nil
}

// Size returns the number of key-value pairs stored in the map.
func (kv *KeyValueCSV) Size() int {
	return kv.memoryMap.Size()
}

// Set stores the provided value under the given key, replacing any existing entry.
func (kv *KeyValueCSV) Set(key string, value interface{}) error {
	return kv.memoryMap.Set(key, value)
}

// csvSetMapValues iterates the provided map and sets all key-value pairs into the provided KeyValueCSV.
// If errors occur, remaining values are still set and a joined error is returned.
func csvSetMapValues[T any](kv *KeyValueCSV, m map[string]T) error {
	// encode before getting lock
	items := make(map[string]*dataItem, len(m))
	var errs []error
	for k, v := range m {
		if item, err := encodeValue(v); err == nil {
			items[k] = item
		} else {
			errs = append(errs, err)
		}
	}

	kv.memoryMap.setItemMap(items)
	return errors.Join(errs...)
}

// csvSetSliceValues iterates the provided slice, using the keyProvider function to derive the key for each element,
// and sets the values into the provided KeyValueCSV. If errors occur, remaining values are still set
// and a joined error is returned.
func csvSetSliceValues[T any](kv *KeyValueCSV, s []T, keyProvider func(value T) string) error {
	// encode before getting lock
	items := make(map[string]*dataItem, len(s))
	var errs []error
	for _, v := range s {
		if item, err := encodeValue(v); err == nil {
			items[keyProvider(v)] = item
		} else {
			errs = append(errs, err)
		}
	}

	kv.memoryMap.setItemMap(items)
	return errors.Join(errs...)
}

// Delete removes the key from the map if it exists.
func (kv *KeyValueCSV) Delete(key string) {
	kv.memoryMap.Delete(key)
}

// DeleteAll removes all entries from the map.
func (kv *KeyValueCSV) DeleteAll() {
	kv.memoryMap.DeleteAll()
}

// Get retrieves the value for the given key into the provided pointer.
// The returned bool indicates whether the key was found.
// A returned error indicates a failure to set into the provided value.
func (kv *KeyValueCSV) Get(key string, value interface{}) (bool, error) {
	return kv.memoryMap.Get(key, value)
}

// ContainsKey reports whether the given key exists in the map.
func (kv *KeyValueCSV) ContainsKey(key string) bool {
	return kv.memoryMap.ContainsKey(key)
}

// KeySet returns all keys stored in the map.
func (kv *KeyValueCSV) KeySet() []string {
	return kv.memoryMap.KeySet()
}

// Commit writes the current state of the map to disk.
func (kv *KeyValueCSV) Commit() error {
	kv.memoryMap.rwLock.Lock()
	defer kv.memoryMap.rwLock.Unlock()

	if kv.memoryMap.modCount == kv.commitMod {
		return nil // no modifications since last commit, ignore
	}
	kv.commitMod = kv.memoryMap.modCount

	file, err := os.OpenFile(kv.filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|syscall.O_NOFOLLOW, 0644)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	return kv.commitTo(file)
}

// commitTo writes all current key-value pairs to the provided writer, sorted by data type then key.
func (kv *KeyValueCSV) commitTo(w io.Writer) error {
	// sort keys so output is in a consistent order
	keys := mapKeys(kv.memoryMap.data)
	slices.SortFunc(keys, func(a, b string) int {
		dataVal1 := kv.memoryMap.data[a]
		dataVal2 := kv.memoryMap.data[b]

		if dataVal1.dataType != dataVal2.dataType {
			return dataVal1.dataType - dataVal2.dataType
		} else if dataVal1.dataType == dataStructJson && dataVal1.structId != dataVal2.structId {
			return strings.Compare(dataVal1.structId, dataVal2.structId)
		} else {
			return strings.Compare(a, b)
		}
	})

	writer := csv.NewWriter(w)
	// write header at start
	if err := writer.Write([]string{currentFileVersion}); err != nil {
		return err
	}
	var lastStructName string
	var structFieldNames []string
	var fieldNameTypes map[string]reflect.Kind
	for i, key := range keys {
		dataVal := kv.memoryMap.data[key]
		if dataVal.dataType == dataStructJson {
			if dataVal.structId != lastStructName && i+1 < len(keys) && kv.memoryMap.data[keys[i+1]].structId == dataVal.structId {
				// we have at least one more value of this type, so encode a header line
				// first we need to inspect all fields of the struct, we need to check all instances
				// because may have omitted default fields in specific instances
				var allStructFieldNames [][]string
				fieldNameTypes = make(map[string]reflect.Kind)
				for i2 := i; i2 < len(keys); i2++ {
					val2 := kv.memoryMap.data[keys[i2]]
					if dataVal.structId != val2.structId {
						break
					}

					var structValue map[string]interface{}
					if err := json.Unmarshal([]byte(val2.value), &structValue); err != nil {
						return err
					}
					allStructFieldNames = append(allStructFieldNames, mapKeys(structValue))
					// track the field type, needed to fill in default values for structs which had default values omitted
					for name := range structValue {
						fieldType := reflect.ValueOf(structValue[name]).Kind()
						if current, ok := fieldNameTypes[name]; ok {
							if zeroValue(current) != zeroValue(fieldType) {
								// on type mismatch we set to pointer to encode a `null` for empty fields
								// This is an interface or any field which accepts multiple types
								fieldNameTypes[name] = reflect.Ptr
							}
						} else {
							fieldNameTypes[name] = fieldType
						}
					}
				}
				structFieldNames = sliceUniqueUnion(allStructFieldNames)
				slices.Sort(structFieldNames) // sort for consistency
				lastStructName = dataVal.structId

				if err := writer.Write(append([]string{strconv.Itoa(dataStructHeader), dataVal.structId}, structFieldNames...)); err != nil {
					return err
				}
			}

			if dataVal.structId == lastStructName { // append value only
				var structValue map[string]interface{}
				if err := json.Unmarshal([]byte(dataVal.value), &structValue); err != nil {
					return err
				}
				values := make([]interface{}, len(structFieldNames))
				for valueIdx, fieldName := range structFieldNames {
					if val, exists := structValue[fieldName]; exists {
						values[valueIdx] = val
					} else { // Substitute with default based on field's type
						values[valueIdx] = zeroValue(fieldNameTypes[fieldName])
					}
				}

				if valueJsonBytes, err := json.Marshal(values); err != nil {
					return err
				} else if err := writer.Write([]string{strconv.Itoa(dataStructValue), key, string(valueJsonBytes)}); err != nil {
					return err
				}
			} else { // no advantage to header encoding, append as single raw json line
				if err := writer.Write([]string{strconv.Itoa(dataStructJson), key, dataVal.value}); err != nil {
					return err
				}
			}
		} else {
			if err := writer.Write([]string{strconv.Itoa(dataVal.dataType), key, dataVal.value}); err != nil {
				return err
			}
		}
	}

	writer.Flush()
	return writer.Error()
}
