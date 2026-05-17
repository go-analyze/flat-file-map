package ffmap

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/go-analyze/bulk"
	"github.com/go-analyze/encoding/base85"
)

// jsonRawMessageType identifies json.RawMessage so its raw JSON bytes are stored
// verbatim instead of being Z85-encoded as opaque bytes.
var jsonRawMessageType = reflect.TypeOf(json.RawMessage(nil))

// memoryJsonMap provides a primarily in-memory key-value map.
// This is primarily used for testing or building other map types.
type memoryJsonMap struct {
	rwLock   sync.RWMutex
	data     map[string]dataItem
	modCount int
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
	//11,key,structId,fieldName1,fieldName2...
	dataArraySliceHeader // Used only in CSV, defines a header for one slice-of-struct value (size + diff optimization)
	//12,"[jsonValue1, jsonValue2...]" // value encoded as json array with position matching header field positions
	//12,null                          // nil element
	dataArraySliceValue // Used only in CSV, defines one element row under the preceding slice header
	//13,mapKey,z85String
	dataBytes // top-level []byte stored as base85 Z85 (denser than JSON base64; alphabet needs no CSV escaping)
)

// Size returns the number of key-value pairs stored in the map.
func (kv *memoryJsonMap) Size() int {
	kv.rwLock.RLock()
	defer kv.rwLock.RUnlock()

	return len(kv.data)
}

// computeElementStructId returns a namespaced identity for a slice's element type, or
// empty string when no struct identity can be assigned.
// Pointer-to-struct elements are unwrapped one level. Interface element types trigger a
// runtime walk; homogeneous-on-struct contents share the typed slice's id, mixed contents
// return the "[]any" sentinel, empty/all-nil return empty.
func computeElementStructId(val reflect.Value) string {
	elemType := val.Type().Elem()
	switch elemType.Kind() {
	case reflect.Struct:
		return "[]" + computeStructId(elemType)
	case reflect.Ptr:
		if elemType.Elem().Kind() == reflect.Struct {
			return "[]" + computeStructId(elemType.Elem())
		}
		return ""
	case reflect.Interface:
		var concreteType reflect.Type
		var sawNonNil bool
		for i := 0; i < val.Len(); i++ {
			elem := val.Index(i)
			if elem.IsNil() {
				continue
			}
			inner := elem.Elem()
			if inner.Kind() == reflect.Ptr {
				if inner.IsNil() {
					continue
				}
				inner = inner.Elem()
			}
			if inner.Kind() != reflect.Struct {
				return "[]any"
			}
			t := inner.Type()
			if !sawNonNil {
				concreteType = t
				sawNonNil = true
			} else if t != concreteType {
				return "[]any"
			}
		}
		if !sawNonNil {
			return ""
		}
		return "[]" + computeStructId(concreteType)
	default:
		return ""
	}
}

// computeStructId returns a stable identity string for a struct-kind reflect.Type.
// The id combines the type name with a CRC32 over sorted field names so that field renames
// produce a different id while reorderings of the same fields share one.
func computeStructId(t reflect.Type) string {
	// this id is only used for comparison but should remain consistent for a given file version
	id := strings.ReplaceAll(t.String(), " ", "")
	if t.NumField() == 0 {
		return id
	}
	// id grouping based on field names, sorted to be stable over minor refactors
	fieldNames := make([]string, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		fieldNames[i] = t.Field(i).Name
	}
	slices.Sort(fieldNames)
	combinedFieldNames := strings.Join(fieldNames, ";")
	crc32q := crc32.MakeTable(crc32.Castagnoli)
	return id + "-" + strconv.FormatUint(uint64(crc32.Checksum([]byte(combinedFieldNames), crc32q)), 36)
}

// encodeValue converts a Go value into a dataItem for storage.
func encodeValue(value interface{}) (*dataItem, error) {
	var dataType int
	var structId, strVal string
	switch v := value.(type) {
	case nil:
		return nil, &EncodingError{Message: "cannot encode nil value"}
	case []byte:
		if v == nil {
			// nil []byte stays in the JSON slice path so it round-trips as nil (distinct from empty)
			return &dataItem{dataType: dataArraySlice, value: "null"}, nil
		}
		return &dataItem{dataType: dataBytes, value: base85.Z85.EncodeToString(v)}, nil
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
				return nil, &EncodingError{Value: value, Message: "cannot encode nil pointer"}
			}
			val = val.Elem() // get the value the pointer references
		}
		switch val.Kind() {
		case reflect.Slice, reflect.Array:
			// json.RawMessage keeps its raw JSON form so the file stays human-readable
			if val.Type().Elem().Kind() == reflect.Uint8 && val.Type() != jsonRawMessageType {
				if val.Kind() == reflect.Slice && val.IsNil() {
					return &dataItem{dataType: dataArraySlice, value: "null"}, nil
				}
				var b []byte
				if val.Kind() == reflect.Slice {
					b = val.Bytes()
				} else {
					// arrays passed by value are not addressable; copy into a slice
					b = make([]byte, val.Len())
					reflect.Copy(reflect.ValueOf(b), val)
				}
				return &dataItem{dataType: dataBytes, value: base85.Z85.EncodeToString(b)}, nil
			}
			dataType = dataArraySlice
			structId = computeElementStructId(val)
		case reflect.Map:
			dataType = dataMap
		case reflect.String:
			dataType = dataString
			strVal = val.String()
			return &dataItem{dataType: dataType, structId: structId, value: strVal}, nil
		case reflect.Bool:
			dataType = dataBool
			if val.Bool() {
				strVal = "t"
			} else {
				strVal = "f"
			}
			return &dataItem{dataType: dataType, structId: structId, value: strVal}, nil
		case reflect.Float32, reflect.Float64:
			dataType = dataFloat
			strVal = strconv.FormatFloat(val.Float(), 'f', -1, 64)
			return &dataItem{dataType: dataType, structId: structId, value: strVal}, nil
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			dataType = dataInt
			strVal = strconv.FormatInt(val.Int(), 10)
			return &dataItem{dataType: dataType, structId: structId, value: strVal}, nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			dataType = dataUint
			strVal = strconv.FormatUint(val.Uint(), 10)
			return &dataItem{dataType: dataType, structId: structId, value: strVal}, nil
		case reflect.Complex64, reflect.Complex128:
			dataType = dataComplexNum
			strVal = fmt.Sprintf("%v", val.Complex())
			return &dataItem{dataType: dataType, structId: structId, value: strVal}, nil
		case reflect.Struct:
			dataType = dataStructJson
			structId = computeStructId(val.Type())
		default:
			return nil, &EncodingError{Value: value, Message: fmt.Sprintf("unsupported type: %v", val.Kind())}
		}
		jsonBytes, err := json.Marshal(stripZeroFields(val))
		if err != nil {
			return nil, &EncodingError{Value: value, Message: "failed to marshal to JSON", Err: err}
		}
		strVal = string(jsonBytes)
	}
	return &dataItem{dataType: dataType, structId: structId, value: strVal}, nil
}

// stripZeroFields removes default/empty/zero-value fields from a struct while preserving non-nil pointers
// and correctly serializing custom json.Marshaler types. It also preserves []byte so that json.Marshal
// will encode them as base64, and preserves fixed-size byte arrays so that they remain JSON arrays.
//
// This allows reducing stored JSON to only what is necessary to accurately recreate the struct state on Get.
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
		// Shortcut preserves explicit non-nil pointers to zero primitives; struct/slice/map kinds
		// descend so their stripped form is byte-minimal (and round-trips identically)
		if elem.Kind() != reflect.Slice && elem.Kind() != reflect.Map && elem.Kind() != reflect.Struct && isZeroValue(elem) {
			return v.Interface()
		}
		return stripZeroFields(elem)
	}

	// Interface holding a struct, pointer, or collection
	// unwrap and recurse so the contents are processed as if directly-typed
	if v.Kind() == reflect.Interface && !v.IsNil() {
		elem := v.Elem()
		elemKind := elem.Kind()
		if elemKind == reflect.Struct || elemKind == reflect.Ptr ||
			elemKind == reflect.Slice || elemKind == reflect.Map || elemKind == reflect.Array {
			return stripZeroFields(elem)
		}
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

// decodeValue converts a stored dataItem back into a Go value.
func decodeValue(dataType int, encodedValue string, value interface{}) error {
	ve := reflect.ValueOf(value).Elem()
	switch dataType {
	case dataString:
		if ve.Kind() != reflect.String {
			return &TypeMismatchError{Message: fmt.Sprintf("expected string type but got %v", ve.Kind())}
		}

		ve.SetString(encodedValue)
	case dataInt:
		iVal, err := strconv.ParseInt(encodedValue, 10, 64)
		if err != nil {
			return err
		}

		switch ve.Kind() {
		case reflect.Int64:
		case reflect.Int:
			if iVal < math.MinInt || iVal > math.MaxInt {
				return &TypeMismatchError{Message: "int overflow"}
			}
		case reflect.Int32:
			if iVal < math.MinInt32 || iVal > math.MaxInt32 {
				return &TypeMismatchError{Message: "int32 overflow"}
			}
		case reflect.Int16:
			if iVal < math.MinInt16 || iVal > math.MaxInt16 {
				return &TypeMismatchError{Message: "int16 overflow"}
			}
		case reflect.Int8:
			if iVal < math.MinInt8 || iVal > math.MaxInt8 {
				return &TypeMismatchError{Message: "int8 overflow"}
			}
		default:
			return &TypeMismatchError{Message: fmt.Sprintf("expected int type but got %v", ve.Kind())}
		}

		ve.SetInt(iVal)
	case dataUint:
		uVal, err := strconv.ParseUint(encodedValue, 10, 64)
		if err != nil {
			return err
		}

		switch ve.Kind() {
		case reflect.Uint64:
		case reflect.Uint:
			if uVal > math.MaxUint {
				return &TypeMismatchError{Message: "uint overflow"}
			}
		case reflect.Uint32:
			if uVal > math.MaxUint32 {
				return &TypeMismatchError{Message: "uint32 overflow"}
			}
		case reflect.Uint16:
			if uVal > math.MaxUint16 {
				return &TypeMismatchError{Message: "uint16 overflow"}
			}
		case reflect.Uint8:
			if uVal > math.MaxUint8 {
				return &TypeMismatchError{Message: "uint8 overflow"}
			}
		default:
			return &TypeMismatchError{Message: fmt.Sprintf("expected uint type but got %v", ve.Kind())}
		}

		ve.SetUint(uVal)
	case dataFloat:
		fVal, err := strconv.ParseFloat(encodedValue, 64)
		if err != nil {
			return err
		}

		switch ve.Kind() {
		case reflect.Float64:
		case reflect.Float32:
			if isFloat32Overflow(fVal) {
				return &TypeMismatchError{Message: "float32 overflow"}
			}
		default:
			return &TypeMismatchError{Message: fmt.Sprintf("expected float type but got %v", ve.Kind())}
		}

		ve.SetFloat(fVal)
	case dataComplexNum:
		c, err := strconv.ParseComplex(encodedValue, 128)
		if err != nil {
			return err
		}

		realVal, imagVal := real(c), imag(c)
		switch ve.Kind() {
		case reflect.Complex128:
			// value can be set directly, ParseComplex already used float64 precision
		case reflect.Complex64:
			if isFloat32Overflow(realVal) {
				return &TypeMismatchError{Message: "complex real float32 overflow"}
			} else if isFloat32Overflow(imagVal) {
				return &TypeMismatchError{Message: "complex imaginary float32 overflow"}
			}
		default:
			return &TypeMismatchError{Message: fmt.Sprintf("expected complex type but got %v", ve.Kind())}
		}

		ve.SetComplex(complex(realVal, imagVal))
	case dataBool:
		if ve.Kind() != reflect.Bool {
			return &TypeMismatchError{Message: fmt.Sprintf("expected bool type but got %v", ve.Kind())}
		}
		switch encodedValue {
		case "t":
			ve.SetBool(true)
		case "f":
			ve.SetBool(false)
		default:
			return &ValidationError{Message: "unexpected encoded bool value: " + encodedValue}
		}
	case dataBytes:
		decoded, err := base85.Z85.DecodeString(encodedValue)
		if err != nil {
			return err
		}
		switch ve.Kind() {
		case reflect.Slice:
			if ve.Type().Elem().Kind() != reflect.Uint8 {
				return &TypeMismatchError{Message: fmt.Sprintf("expected []byte type but got %v", ve.Type())}
			}
			ve.SetBytes(decoded)
		case reflect.Array:
			if ve.Type().Elem().Kind() != reflect.Uint8 {
				return &TypeMismatchError{Message: fmt.Sprintf("expected [N]byte type but got %v", ve.Type())}
			}
			if ve.Len() != len(decoded) {
				return &TypeMismatchError{Message: fmt.Sprintf("byte array length mismatch: stored %d, target %d", len(decoded), ve.Len())}
			}
			reflect.Copy(ve, reflect.ValueOf(decoded))
		default:
			return &TypeMismatchError{Message: fmt.Sprintf("expected []byte or [N]byte type but got %v", ve.Type())}
		}
	case dataArraySlice, dataMap, dataStructJson:
		// Zero values to ensure that fields not in the json are zeroed out
		ve.Set(reflect.Zero(ve.Type()))

		if err := json.Unmarshal([]byte(encodedValue), value); err != nil {
			return err
		}
	default:
		return &ValidationError{Message: "unexpected error, Get needs to implement data type: " + strconv.Itoa(dataType)}
	}
	return nil
}

// isFloat32Overflow checks if a float64 value would overflow when converted to float32.
func isFloat32Overflow(fVal float64) bool {
	f32Val := float32(fVal)
	return math.IsInf(float64(f32Val), 0)
}

// Set stores the provided value under the given key, replacing any existing entry.
func (kv *memoryJsonMap) Set(key string, value interface{}) error {
	item, err := encodeValue(value)
	if err != nil {
		var encodingErr *EncodingError
		if errors.As(err, &encodingErr) {
			encodingErr.Key = key
			return encodingErr
		}
		return err
	}

	kv.rwLock.Lock()
	defer kv.rwLock.Unlock()

	kv.modCount++
	kv.data[key] = *item
	return nil
}

// setItemMap bulk sets multiple dataItems in the map.
func (kv *memoryJsonMap) setItemMap(items map[string]*dataItem) {
	if len(items) != 0 {
		kv.rwLock.Lock()
		defer kv.rwLock.Unlock()

		kv.modCount++
		for k := range items {
			kv.data[k] = *items[k]
		}
	}
}

// Delete removes the key from the map if it exists.
func (kv *memoryJsonMap) Delete(key string) {
	kv.rwLock.Lock()
	defer kv.rwLock.Unlock()

	kv.modCount++
	delete(kv.data, key)
}

// DeleteAll removes all entries from the map.
func (kv *memoryJsonMap) DeleteAll() {
	kv.rwLock.Lock()
	defer kv.rwLock.Unlock()

	kv.modCount++
	clear(kv.data)
}

// lockedRead acquires a read lock before loading the value.
// Use this function whenever you don't already hold a lock to ensure the read lock is promptly released.
func (kv *memoryJsonMap) lockedRead(key string) (dataItem, bool) {
	kv.rwLock.RLock()
	defer kv.rwLock.RUnlock()

	dataVal, ok := kv.data[key]
	return dataVal, ok
}

// Get retrieves the value for the given key into the provided pointer.
// The returned bool indicates whether the key was found.
// A returned error indicates a failure in setting into the provided value.
func (kv *memoryJsonMap) Get(key string, value interface{}) (bool, error) {
	if dataVal, ok := kv.lockedRead(key); !ok {
		return false, nil
	} else if err := decodeValue(dataVal.dataType, dataVal.value, value); err != nil {
		var typeMismatchErr *TypeMismatchError
		if errors.As(err, &typeMismatchErr) {
			typeMismatchErr.Key = key
			return false, typeMismatchErr
		}
		return false, err
	} else {
		return true, nil
	}
}

// ContainsKey reports whether the given key exists in the map.
func (kv *memoryJsonMap) ContainsKey(key string) bool {
	_, found := kv.lockedRead(key)
	return found
}

// KeySet returns all keys stored in the map.
func (kv *memoryJsonMap) KeySet() []string {
	kv.rwLock.RLock()
	defer kv.rwLock.RUnlock()

	return bulk.MapKeysSlice(kv.data)
}

// Commit is a no-op for memoryJsonMap.
func (kv *memoryJsonMap) Commit() error {
	return nil
}
