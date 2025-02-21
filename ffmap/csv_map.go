package ffmap

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// KeyValueCSV provides a primarily in-memory key value map, with the ability to load and commit the contents to disk.
type KeyValueCSV struct {
	filename  string
	rwLock    sync.RWMutex
	data      map[string]dataItem
	modCount  int
	commitMod int
}

type dataItem struct {
	dataType int
	structId string
	value    string
}

const (
	//0,structId,fieldName1,fieldName2...
	dataStructHeader = iota // Used only in CSV, defines a header for struct values (size optimization)
	//1,mapKey,"[jsonValue1, jsonValue2...]" // value encoded as json array with position matching header field positions
	dataStructValue // Used only in CSV, defines a struct instance values (size optimization)
	//2,mapKey,"{jsonObject}" // value encoded directly as json
	dataStructJson
	//3,mapKey,valueString
	dataString
	//4,mapKey,-42
	dataInt
	//5,mapKey,42
	dataUint
	//6,mapKey,3.1
	dataFloat
	//7,mapKey,(3.1-4)
	dataComplexNum
	//8,mapKey,t
	dataBool
	//9,mapKey,"[jsonArray]" // value encoded as json
	dataArraySlice
	//10,mapKey,"{"key":"value"} // value encoded as json
	dataMap
)

const currentFileVersion = "ver:0"

// loadFromDisk updates the map with data from the disk.
func (kv *KeyValueCSV) loadFromDisk() error {
	kv.rwLock.Lock()
	defer kv.rwLock.Unlock()

	file, err := os.Open(kv.filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	return kv.loadFromReader(file)
}

func (kv *KeyValueCSV) loadFromReader(r io.Reader) error {
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1 // disable check, field counts will vary
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	return kv.loadRecords(records)
}

func (kv *KeyValueCSV) loadRecords(records [][]string) error {
	var currStructName string
	var currStructValueNames []string
	for i, record := range records {
		if i == 0 { // header line
			if len(record) < 1 || currentFileVersion != record[0] {
				return fmt.Errorf("invalid header line: %s", strings.Join(record, ","))
			}
			continue
		} else if len(record) == 0 {
			continue
		}

		dataType, err := strconv.Atoi(record[0])
		if err != nil {
			return fmt.Errorf("unexpected data type, %s", record[0])
		}
		switch dataType {
		case dataStructHeader:
			if len(record) < 3 {
				return fmt.Errorf("unexpected csv struct header column count: %v, line: %v", len(record), i+1)
			}
			currStructName = record[1]
			currStructValueNames = record[2:]
		case dataStructValue:
			if len(record) != 3 {
				return fmt.Errorf("unexpected csv struct value column count: %v, line: %v", len(record), i+1)
			}
			var values []interface{}
			if err := json.Unmarshal([]byte(record[2]), &values); err != nil {
				return err
			} else if len(values) != len(currStructValueNames) {
				return fmt.Errorf("unexpected encoded json value count: %v/%v, line: %v",
					len(values), len(currStructValueNames), i+1)
			}
			structValue := make(map[string]interface{}, len(currStructValueNames))
			for j, name := range currStructValueNames {
				structValue[name] = values[j]
			}
			encodedStruct, err := json.Marshal(structValue)
			if err != nil {
				return err
			}
			kv.data[record[1]] = dataItem{dataType: dataStructJson, structId: currStructName, value: string(encodedStruct)}
		default:
			if len(record) != 3 {
				return fmt.Errorf("unexpected csv db column count: %v, type: %v, line: %v", len(record), dataType, i+1)
			}
			kv.data[record[1]] = dataItem{dataType: dataType, value: record[2]}
		}
	}
	return nil
}

func (kv *KeyValueCSV) Size() int {
	kv.rwLock.RLock()
	defer kv.rwLock.RUnlock()

	return len(kv.data)
}

func encodeValue(value interface{}) (*dataItem, error) {
	var dataType int
	var structId string
	var strVal string
	switch v := value.(type) {
	case nil:
		return nil, errors.New("cannot encode nil value")
	case string:
		dataType = dataString
		strVal = v
	case bool:
		dataType = dataBool
		if v {
			strVal = "t"
		} else {
			strVal = "f"
		}
	case float32, float64:
		dataType = dataFloat
		strVal = strconv.FormatFloat(reflect.ValueOf(v).Float(), 'f', -1, 64)
	case int, int8, int16, int32, int64:
		dataType = dataInt
		strVal = strconv.FormatInt(reflect.ValueOf(v).Int(), 10)
	case uint, uint8, uint16, uint32, uint64:
		dataType = dataUint
		strVal = strconv.FormatUint(reflect.ValueOf(v).Uint(), 10)
	case complex64, complex128:
		dataType = dataComplexNum
		strVal = fmt.Sprintf("%v", v)
	default:
		val := reflect.ValueOf(value)
		if val.Kind() == reflect.Ptr {
			if val.IsNil() {
				return nil, errors.New("cannot encode nil pointer")
			}
			val = val.Elem() // get the value the pointer references
		}
		switch val.Kind() {
		case reflect.Slice, reflect.Array:
			dataType = dataArraySlice
		case reflect.Map:
			dataType = dataMap
		default:
			dataType = dataStructJson
			// this id is only used for comparison but must remain consistent for a given file version
			// We have to consider the field names so that don't mix structs which have had field updates between versions
			combinedFieldName := bytes.Buffer{}
			for i := 0; i < val.NumField(); i++ {
				combinedFieldName.WriteString(val.Type().Field(i).Name)
			}
			structId = strings.ReplaceAll(val.Type().String(), " ", "")
			if combinedFieldName.Len() != 0 {
				crc32q := crc32.MakeTable(crc32.Castagnoli)
				structId += "-" + strconv.FormatUint(uint64(crc32.Checksum(combinedFieldName.Bytes(), crc32q)), 36)
			}
		}
		bytes, err := json.Marshal(stripZeroFields(val))
		if err != nil {
			return nil, err
		}
		strVal = string(bytes)
	}
	return &dataItem{dataType: dataType, structId: structId, value: strVal}, nil
}

// stripZeroFields removes default / empty / zero-value fields from a struct while preserving non-nil pointers
// and correctly serializing custom json.Marshaler types. It also preserves []byte so that json.Marshal
// will encode them as base64, and preserves fixed-size byte arrays so that they remain JSON arrays.
//
// This allows us to reduce our stored json to only what is necessary to accurately recreate the struct state on Get.
func stripZeroFields(v reflect.Value) interface{} {
	if !v.IsValid() {
		return nil
	}

	// Special-case: for byte slices and arrays, return as-is so that json.Marshal encodes them as base64
	if (v.Kind() == reflect.Slice || v.Kind() == reflect.Array) && v.Type().Elem().Kind() == reflect.Uint8 {
		return v.Interface()
	} else if v.CanInterface() { // If the type implements json.Marshaler, use that
		if marshaler, ok := v.Interface().(json.Marshaler); ok {
			if rv := reflect.ValueOf(marshaler); rv.Kind() == reflect.Ptr && rv.IsNil() {
				// fall through to normal processing
			} else if jsonBytes, err := marshaler.MarshalJSON(); err == nil {
				var unmarshaled interface{}
				if err := json.Unmarshal(jsonBytes, &unmarshaled); err == nil {
					return unmarshaled
				}
			}
		}
	}

	// Handle pointer: if non-nil, and if its element is considered empty (for non-collection types)
	// then return the pointer (preserving an explicit pointer to an empty or default value),
	// otherwise process its element.
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		elem := v.Elem()
		// For slices and maps we want to process the element normally,
		// so only shortcut if the pointer points to a non-collection empty value.
		if elem.Kind() != reflect.Slice && elem.Kind() != reflect.Map && isZeroValue(elem) {
			return v.Interface()
		}
		return stripZeroFields(elem)
	}

	switch v.Kind() {
	case reflect.Struct:
		out := make(map[string]interface{})
		typ := v.Type()
		for i := 0; i < v.NumField(); i++ {
			fieldType := typ.Field(i)
			if fieldType.PkgPath != "" {
				continue // Skip unexported fields
			}
			fieldVal := v.Field(i)
			jsonKey := fieldType.Name
			if tag, ok := fieldType.Tag.Lookup("json"); ok {
				tagParts := strings.Split(tag, ",")
				if tagParts[0] == "-" {
					continue // omit fields with json:"-"
				} else if tagParts[0] != "" {
					jsonKey = tagParts[0]
				}
			}

			// Process the field recursively.
			strippedValue := stripZeroFields(fieldVal)

			// If the field is anonymous (embedded) and is (or points to) a struct, flatten its fields.
			if fieldType.Anonymous {
				if fieldVal.Kind() == reflect.Struct ||
					(fieldVal.Kind() == reflect.Ptr && !fieldVal.IsNil() && fieldVal.Elem().Kind() == reflect.Struct) {
					if m, ok := strippedValue.(map[string]interface{}); ok {
						for k, v := range m {
							out[k] = v
						}
						continue // Skip adding the struct itself since its fields are now in out
					}
				}
			}

			// For non-anonymous fields: if the field is a slice/map and nil, or is a zero value, omit it.
			if fieldVal.Kind() == reflect.Slice || fieldVal.Kind() == reflect.Map {
				if fieldVal.IsNil() {
					continue
				}
			} else if isZeroValue(fieldVal) {
				continue
			}
			out[jsonKey] = strippedValue
		}
		// Even if all fields were omitted, we want to preserve an empty struct
		// so that a pointer to an empty struct remains non-nil.
		return out
	case reflect.Slice:
		if v.Len() == 0 {
			if v.IsNil() {
				return nil
			}
			return []interface{}{}
		}
		out := make([]interface{}, v.Len())
		for i := 0; i < v.Len(); i++ {
			out[i] = stripZeroFields(v.Index(i))
		}
		return out
	case reflect.Array:
		// Arrays cannot be nil so we always process them.
		if v.Len() == 0 {
			return v.Interface()
		}
		out := make([]interface{}, v.Len())
		for i := 0; i < v.Len(); i++ {
			out[i] = stripZeroFields(v.Index(i))
		}
		return out
	case reflect.Map:
		if v.Len() == 0 {
			if v.IsNil() {
				return nil
			}
			return map[string]interface{}{}
		}
		out := make(map[string]interface{})
		for _, key := range v.MapKeys() {
			// use fmt.Sprint to ensure JSON-compatible keys
			mapKey := fmt.Sprint(key.Interface())
			mapValue := v.MapIndex(key)
			switch mapValue.Kind() {
			case reflect.Ptr, reflect.Interface, reflect.Struct, reflect.Map, reflect.Slice, reflect.Array:
				out[mapKey] = stripZeroFields(mapValue)
			default:
				out[mapKey] = mapValue.Interface()
			}
		}
		return out
	default:
		if isZeroValue(v) {
			return nil
		}
		return v.Interface()
	}
}

// isZeroValue determines whether a reflect.Value is its type's zero value.
// For slices and maps, emptiness is defined by nil rather than Len()==0,
// so that non-nil empty slices/maps (which the user explicitly provided) are preserved.
func isZeroValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Bool:
		return !v.Bool()
	case reflect.String:
		return v.Len() == 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Slice, reflect.Map:
		return v.IsNil() // only consider nil to match prior pointer behavior
	case reflect.Array:
		return v.Len() == 0
	case reflect.Ptr, reflect.Interface:
		return v.IsNil()
	case reflect.Struct:
		// If the struct implements json.Marshaler, check its output.
		if v.CanInterface() {
			if marshaler, ok := v.Interface().(json.Marshaler); ok {
				if rv := reflect.ValueOf(marshaler); rv.Kind() == reflect.Ptr && rv.IsNil() {
					return true
				} else if jsonBytes, err := marshaler.MarshalJSON(); err == nil {
					s := string(jsonBytes)
					return s == `""` || s == `{}`
				}
			}
		}
		for i := 0; i < v.NumField(); i++ {
			if !isZeroValue(v.Field(i)) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// zeroValue returns the default or zero value for the provided reflected field. This is the counterpart to isZeroValue
// used for encoding needed zero values that were omitted.
func zeroValue(t reflect.Kind) interface{} {
	// not all types are possible after json.Unmarshal, but specified for completeness
	switch t {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return uint64(0)
	case reflect.Float32, reflect.Float64:
		return 0.0
	case reflect.Bool:
		return false
	case reflect.String:
		return ""
	default:
		return nil
	}
}

func decodeValue(dataType int, encodedValue string, value interface{}) error {
	v := reflect.ValueOf(value).Elem()
	switch dataType {
	case dataString:
		if v.Kind() != reflect.String {
			return fmt.Errorf("expected string type but got %v", v.Kind())
		}

		v.SetString(encodedValue)
	case dataInt:
		iVal, err := strconv.ParseInt(encodedValue, 10, 64)
		if err != nil {
			return err
		}

		switch v.Kind() {
		case reflect.Int64:
		case reflect.Int:
			if iVal < math.MinInt || iVal > math.MaxInt {
				return errors.New("int overflow")
			}
		case reflect.Int32:
			if iVal < math.MinInt32 || iVal > math.MaxInt32 {
				return errors.New("int32 overflow")
			}
		case reflect.Int16:
			if iVal < math.MinInt16 || iVal > math.MaxInt16 {
				return errors.New("int16 overflow")
			}
		case reflect.Int8:
			if iVal < math.MinInt8 || iVal > math.MaxInt8 {
				return errors.New("int8 overflow")
			}
		default:
			return fmt.Errorf("expected int type but got %v", v.Kind())
		}

		v.SetInt(iVal)
	case dataUint:
		uVal, err := strconv.ParseUint(encodedValue, 10, 64)
		if err != nil {
			return err
		}

		switch v.Kind() {
		case reflect.Uint64:
		case reflect.Uint:
			if uVal > math.MaxUint {
				return errors.New("uint overflow")
			}
		case reflect.Uint32:
			if uVal > math.MaxUint32 {
				return errors.New("uint32 overflow")
			}
		case reflect.Uint16:
			if uVal > math.MaxUint16 {
				return errors.New("uint16 overflow")
			}
		case reflect.Uint8:
			if uVal > math.MaxUint8 {
				return errors.New("uint8 overflow")
			}
		default:
			return fmt.Errorf("expected uint type but got %v", v.Kind())
		}

		v.SetUint(uVal)
	case dataFloat:
		fVal, err := strconv.ParseFloat(encodedValue, 64)
		if err != nil {
			return err
		}

		switch v.Kind() {
		case reflect.Float64:
		case reflect.Float32:
			if isFloat32Overflow(fVal) {
				return errors.New("float32 overflow")
			}
		default:
			return fmt.Errorf("expected float type but got %v", v.Kind())
		}

		v.SetFloat(fVal)
	case dataComplexNum:
		var real, imag float64
		if _, err := fmt.Sscanf(encodedValue, "(%f+%fi)", &real, &imag); err != nil {
			return err
		}

		switch v.Kind() {
		case reflect.Complex128:
		case reflect.Complex64:
			if isFloat32Overflow(real) {
				return errors.New("complex real float32 overflow")
			} else if isFloat32Overflow(imag) {
				return errors.New("complex imaginary float32 overflow")
			}
		default:
			return fmt.Errorf("expected complex type but got %v", v.Kind())
		}

		v.SetComplex(complex(real, imag))
	case dataBool:
		if v.Kind() != reflect.Bool {
			return fmt.Errorf("expected bool type but got %v", v.Kind())
		}
		if encodedValue == "t" {
			v.SetBool(true)
		} else if encodedValue == "f" {
			v.SetBool(false)
		} else {
			return fmt.Errorf("unexpected encoded bool value: %s", encodedValue)
		}
	case dataArraySlice, dataMap, dataStructJson:
		if err := json.Unmarshal([]byte(encodedValue), value); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unexpected error, Get needs to implement data type %v", dataType)
	}
	return nil
}

func isFloat32Overflow(fVal float64) bool {
	f32Val := float32(fVal)
	return math.IsInf(float64(f32Val), 0)
}

func (kv *KeyValueCSV) Set(key string, value interface{}) error {
	item, err := encodeValue(value)
	if err != nil {
		return err
	}

	kv.rwLock.Lock()
	defer kv.rwLock.Unlock()

	kv.modCount++
	kv.data[key] = *item
	return nil
}

// SetAll will iterate the provided map and set all the key values into the provided KeyValueCSV. In the case of error,
// remaining values will still be set, with the returned error being a joined error (if multiple errors occurred).
func SetAll[T any](kv *KeyValueCSV, m map[string]T) error {
	if kv == nil {
		return errors.New("nil KeyValueCSV")
	}

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

	if len(items) != 0 {
		kv.rwLock.Lock()
		defer kv.rwLock.Unlock()

		kv.modCount++
		for k := range items {
			kv.data[k] = *items[k]
		}
	}

	return errors.Join(errs...)
}

func (kv *KeyValueCSV) Delete(key string) {
	kv.rwLock.Lock()
	defer kv.rwLock.Unlock()

	kv.modCount++
	delete(kv.data, key)
}

func (kv *KeyValueCSV) DeleteAll() {
	kv.rwLock.Lock()
	defer kv.rwLock.Unlock()

	kv.modCount++
	kv.data = make(map[string]dataItem)
}

// lockedRead will acquire a read lock before loading the value, use this function whenever you don't
// already hold a lock.  Using this ensures that the read lock is promptly released.
func (kv *KeyValueCSV) lockedRead(key string) (dataItem, bool) {
	kv.rwLock.RLock()
	defer kv.rwLock.RUnlock()

	dataVal, ok := kv.data[key]
	return dataVal, ok
}

func (kv *KeyValueCSV) Get(key string, value interface{}) (bool, error) {
	if dataVal, ok := kv.lockedRead(key); !ok {
		return false, nil
	} else if err := decodeValue(dataVal.dataType, dataVal.value, value); err != nil {
		return false, err
	} else {
		return true, nil
	}
}

func (kv *KeyValueCSV) ContainsKey(key string) bool {
	_, found := kv.lockedRead(key)
	return found
}

func (kv *KeyValueCSV) KeySet() []string {
	kv.rwLock.RLock()
	defer kv.rwLock.RUnlock()

	return mapKeys(kv.data)
}

func (kv *KeyValueCSV) Commit() error {
	kv.rwLock.Lock()
	defer kv.rwLock.Unlock()

	if kv.modCount == kv.commitMod {
		return nil // no modifications since last commit, ignore
	}
	kv.commitMod = kv.modCount

	file, err := os.Create(kv.filename)
	if err != nil {
		return err
	}
	defer file.Close()

	return kv.commitTo(file)
}

// commitTo writes all current key-value pairs to the provided writer, sorted by data type and then key.
func (kv *KeyValueCSV) commitTo(w io.Writer) error {
	// sort keys so output is in a consistent order
	keys := mapKeys(kv.data)
	slices.SortFunc(keys, func(a, b string) int {
		dataVal1 := kv.data[a]
		dataVal2 := kv.data[b]

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
		dataVal := kv.data[key]
		if dataVal.dataType == dataStructJson {
			if dataVal.structId != lastStructName && i+1 < len(keys) && kv.data[keys[i+1]].structId == dataVal.structId {
				// we have at least one more value of this type, so encode a header line
				// first we need to inspect all fields of the struct, we need to check all instances
				// because may have omitted default fields in specific instances
				var allStructFieldNames [][]string
				fieldNameTypes = make(map[string]reflect.Kind)
				for i2 := i; i2 < len(keys); i2++ {
					val2 := kv.data[keys[i2]]
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
