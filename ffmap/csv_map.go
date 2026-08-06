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

	"github.com/go-analyze/bulk"
)

// KeyValueCSV provides a primarily in-memory key-value map with the ability to load and commit the contents to disk.
type KeyValueCSV struct {
	filename  string
	memoryMap *memoryJsonMap
	commitMod int
}

const fileVersion0 = "ver:0"
const currentFileVersion = "ver:1"

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

// sliceLoadCtx accumulates type-12 element rows under one open type-11 header,
// flushed into a single dataArraySlice dataItem when the next non-type-12 record (or EOF) arrives.
type sliceLoadCtx struct {
	open     bool
	key      string
	structId string
	fields   []string
	entries  []json.RawMessage
}

func (c *sliceLoadCtx) flush(data map[string]dataItem) error {
	if !c.open {
		return nil
	}
	jsonBytes, err := json.Marshal(c.entries)
	if err != nil {
		return err
	}
	data[c.key] = dataItem{dataType: dataArraySlice, structId: c.structId, value: string(jsonBytes)}
	c.open = false
	c.entries = nil
	return nil
}

// loadRecords processes parsed CSV records and populates the memory map.
func (kv *KeyValueCSV) loadRecords(records [][]string) error {
	var currStructName string
	var currStructValueNames []string
	var sliceCtx sliceLoadCtx
	for i, record := range records {
		if i == 0 { // header line
			if len(record) < 1 || (currentFileVersion != record[0] && fileVersion0 != record[0]) {
				return &ValidationError{Message: "invalid header line: " + strings.Join(record, ",")}
			}
			continue
		} else if len(record) == 0 {
			continue
		}

		dataType, err := strconv.Atoi(record[0])
		if err != nil {
			return &ValidationError{Message: "unexpected data type: " + record[0], Err: err}
		}
		if dataType != dataArraySliceValue {
			// any non-type-12 record terminates the current slice block
			if err := sliceCtx.flush(kv.memoryMap.data); err != nil {
				return err
			}
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
			var values []json.RawMessage
			if err := json.Unmarshal([]byte(record[2]), &values); err != nil {
				return err
			} else if len(values) != len(currStructValueNames) {
				return &ValidationError{Message: fmt.Sprintf("unexpected encoded json value count: %v/%v, line: %v", len(values), len(currStructValueNames), i+1)}
			}
			structValue := make(map[string]json.RawMessage, len(currStructValueNames))
			for j, name := range currStructValueNames {
				structValue[name] = values[j]
			}
			encodedStruct, err := json.Marshal(structValue)
			if err != nil {
				return err
			}
			kv.memoryMap.data[record[1]] = dataItem{dataType: dataStructJson, structId: currStructName, value: string(encodedStruct)}
		case dataArraySliceHeader:
			if len(record) < 3 {
				return &ValidationError{Message: fmt.Sprintf("unexpected csv slice header column count: %v, line: %v", len(record), i+1)}
			}
			sliceCtx = sliceLoadCtx{
				open:     true,
				key:      record[1],
				structId: record[2],
				fields:   slices.Clone(record[3:]),
			}
		case dataArraySliceValue:
			if len(record) != 2 {
				return &ValidationError{Message: fmt.Sprintf("unexpected csv slice value column count: %v, line: %v", len(record), i+1)}
			} else if !sliceCtx.open {
				return &ValidationError{Message: fmt.Sprintf("slice value record without preceding header, line: %v", i+1)}
			}
			if record[1] == "null" {
				sliceCtx.entries = append(sliceCtx.entries, json.RawMessage("null"))
				continue
			}
			var values []json.RawMessage
			if err := json.Unmarshal([]byte(record[1]), &values); err != nil {
				return err
			} else if len(values) != len(sliceCtx.fields) {
				return &ValidationError{Message: fmt.Sprintf("slice value count mismatch %v/%v under header key=%v, line: %v", len(values), len(sliceCtx.fields), sliceCtx.key, i+1)}
			}
			entry := make(map[string]json.RawMessage, len(sliceCtx.fields))
			for j, name := range sliceCtx.fields {
				entry[name] = values[j]
			}
			entryBytes, err := json.Marshal(entry)
			if err != nil {
				return err
			}
			sliceCtx.entries = append(sliceCtx.entries, entryBytes)
		default:
			if len(record) != 3 {
				return &ValidationError{Message: fmt.Sprintf("unexpected csv db column count: %v, type: %v, line: %v", len(record), dataType, i+1)}
			}
			kv.memoryMap.data[record[1]] = dataItem{dataType: dataType, value: record[2]}
		}
	}
	return sliceCtx.flush(kv.memoryMap.data)
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

// structFieldUnion takes unmarshalled struct objects and returns the sorted union of field names
// plus a per-field reflect.Kind used for zero-value substitution when an item omits a field.
// Fields whose kinds differ across items become reflect.Ptr so encoded to null rather than a wrong-type zero.
func structFieldUnion(items []map[string]interface{}) ([]string, map[string]reflect.Kind) {
	fieldKinds := make(map[string]reflect.Kind)
	for _, item := range items {
		for name, v := range item {
			var fieldType reflect.Kind
			if _, ok := v.(json.Number); ok {
				fieldType = reflect.Float64 // treat as numeric so zero substitution produces 0
			} else {
				fieldType = reflect.ValueOf(v).Kind()
			}
			if current, ok := fieldKinds[name]; ok {
				if zeroValue(current) != zeroValue(fieldType) {
					fieldKinds[name] = reflect.Ptr
				}
			} else {
				fieldKinds[name] = fieldType
			}
		}
	}
	sortedFieldNames := bulk.MapKeysSlice(fieldKinds)
	slices.Sort(sortedFieldNames)
	return sortedFieldNames, fieldKinds
}

// projectStructValues projects one unmarshalled struct object onto the header schema produced
// by structFieldUnion, substituting zero values for absent fields.
func projectStructValues(item map[string]interface{}, sortedFieldNames []string, fieldKinds map[string]reflect.Kind) []interface{} {
	values := make([]interface{}, len(sortedFieldNames))
	for i, fieldName := range sortedFieldNames {
		if val, exists := item[fieldName]; exists {
			values[i] = val
		} else {
			values[i] = zeroValue(fieldKinds[fieldName])
		}
	}
	return values
}

// deferredSlice carries the prepared state for one exploded slice block,
// emitted after the main walk so type-11/12 records cluster at the end of the file.
type deferredSlice struct {
	key          string
	structId     string
	sortedFields []string
	fieldKinds   map[string]reflect.Kind
	parsed       []map[string]interface{} // nil entry represents a JSON null element
}

// prepareExplodedSlice evaluates eligibility and builds the projected rows for one slice item.
// Returns ok=false (and a nil bundle) if any eligibility check fails; the caller falls back to type-9.
func prepareExplodedSlice(key string, item dataItem) (*deferredSlice, bool) {
	if item.structId == "" || item.structId == "[]any" {
		return nil, false
	}
	// fast bail, need at least two struct objects to be worth exploding
	if strings.Count(item.value, "{") < 2 {
		return nil, false
	}
	var rawEntries []json.RawMessage
	if err := json.Unmarshal([]byte(item.value), &rawEntries); err != nil || len(rawEntries) < 2 {
		return nil, false
	}
	parsed := make([]map[string]interface{}, len(rawEntries))
	var nonNilCount int
	for i, raw := range rawEntries {
		if string(raw) == "null" {
			continue // parsed[i] stays nil
		}
		var m map[string]interface{}
		if err := unmarshalUseNumber(raw, &m); err != nil || m == nil {
			return nil, false
		}
		parsed[i] = m
		nonNilCount++
	}
	if nonNilCount < 2 {
		return nil, false
	}
	sortedFields, fieldKinds := structFieldUnion(parsed)
	if len(sortedFields) == 0 {
		return nil, false
	}
	return &deferredSlice{
		key:          key,
		structId:     item.structId,
		sortedFields: sortedFields,
		fieldKinds:   fieldKinds,
		parsed:       parsed,
	}, true
}

// commitTo writes all current key-value pairs to the provided writer, sorted by data type then key.
func (kv *KeyValueCSV) commitTo(w io.Writer) error {
	// sort keys so output is in a consistent order
	keys := bulk.MapKeysSlice(kv.memoryMap.data)
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
	var deferred []*deferredSlice
	for i := 0; i < len(keys); i++ {
		key := keys[i]
		dataVal := kv.memoryMap.data[key]

		if dataVal.dataType == dataArraySlice {
			if ds, ok := prepareExplodedSlice(key, dataVal); ok {
				deferred = append(deferred, ds)
				continue
			}
			// ineligible, fall through to inline type-9 encoding
		}

		if dataVal.dataType != dataStructJson || dataVal.structId == "" {
			if err := writer.Write([]string{strconv.Itoa(dataVal.dataType), key, dataVal.value}); err != nil {
				return err
			}
			continue
		}

		// look ahead to find the run of items sharing this structId
		runEnd := i + 1
		for runEnd < len(keys) {
			next := kv.memoryMap.data[keys[runEnd]]
			if next.dataType != dataStructJson || next.structId != dataVal.structId {
				break
			}
			runEnd++
		}

		if runEnd-i == 1 {
			// no advantage to header encoding, append as single raw json line
			if err := writer.Write([]string{strconv.Itoa(dataStructJson), key, dataVal.value}); err != nil {
				return err
			}
			continue
		}

		// unmarshal each item once for both header construction and value projection
		items := make([]map[string]interface{}, runEnd-i)
		for j := i; j < runEnd; j++ {
			if err := unmarshalUseNumber([]byte(kv.memoryMap.data[keys[j]].value), &items[j-i]); err != nil {
				return err
			}
		}
		sortedFieldNames, fieldKinds := structFieldUnion(items)

		if err := writer.Write(append([]string{strconv.Itoa(dataStructHeader), dataVal.structId}, sortedFieldNames...)); err != nil {
			return err
		}
		for j := i; j < runEnd; j++ {
			values := projectStructValues(items[j-i], sortedFieldNames, fieldKinds)
			valueJsonBytes, err := json.Marshal(values)
			if err != nil {
				return err
			}
			if err := writer.Write([]string{strconv.Itoa(dataStructValue), keys[j], string(valueJsonBytes)}); err != nil {
				return err
			}
		}
		i = runEnd - 1
	}

	// emit deferred exploded slices grouped by (structId, key)
	slices.SortFunc(deferred, func(a, b *deferredSlice) int {
		if a.structId != b.structId {
			return strings.Compare(a.structId, b.structId)
		}
		return strings.Compare(a.key, b.key)
	})
	for _, ds := range deferred {
		header := append([]string{strconv.Itoa(dataArraySliceHeader), ds.key, ds.structId}, ds.sortedFields...)
		if err := writer.Write(header); err != nil {
			return err
		}
		for _, m := range ds.parsed {
			if m == nil {
				if err := writer.Write([]string{strconv.Itoa(dataArraySliceValue), "null"}); err != nil {
					return err
				}
				continue
			}
			values := projectStructValues(m, ds.sortedFields, ds.fieldKinds)
			valueJsonBytes, err := json.Marshal(values)
			if err != nil {
				return err
			}
			if err := writer.Write([]string{strconv.Itoa(dataArraySliceValue), string(valueJsonBytes)}); err != nil {
				return err
			}
		}
	}

	writer.Flush()
	return writer.Error()
}
