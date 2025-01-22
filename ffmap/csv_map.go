package ffmap

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
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

	reader := csv.NewReader(file)
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
			structValue := make(map[string]interface{})
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
		return nil, errors.New("can not encode nil value")
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
		if val.Kind() == reflect.Ptr { // If it's a pointer, get the value it points to
			val = val.Elem()
		}
		if val.Kind() == reflect.Slice || val.Kind() == reflect.Array {
			dataType = dataArraySlice
		} else if val.Kind() == reflect.Map {
			dataType = dataMap
		} else {
			dataType = dataStructJson
			// this id is only used for comparison but must remain consistent for a given file version
			// We have to consider the field names so that don't mix structs which have had field updates between versions
			fieldNames := make([]string, val.NumField())
			for i := range fieldNames {
				fieldNames[i] = val.Type().Field(i).Name
			}

			structId = strings.ReplaceAll(val.Type().String(), " ", "")
			if len(fieldNames) > 0 {
				concatenatedFieldNames := strings.Join(fieldNames, "")
				crc32q := crc32.MakeTable(crc32.Castagnoli)
				structId += "-" + strconv.FormatUint(uint64(crc32.Checksum([]byte(concatenatedFieldNames), crc32q)), 36)
			}
		}
		bytes, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		strVal = string(bytes)
	}
	return &dataItem{dataType: dataType, structId: structId, value: strVal}, nil
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

	return kv.unlockedKeySet()
}

// unlockedKeySet must have lock acquired before invoking, use KeySet if you don't already hold a lock.
func (kv *KeyValueCSV) unlockedKeySet() []string {
	keys := make([]string, 0, len(kv.data))
	for key := range kv.data {
		keys = append(keys, key)
	}
	return keys
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

	// sort keys so output is in a consistent order
	keys := kv.unlockedKeySet()
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

	writer := csv.NewWriter(file)
	// write header at start
	if err = writer.Write([]string{currentFileVersion}); err != nil {
		return err
	}
	var lastStructName string
	var structFieldNames []string
	for i, key := range keys {
		dataVal := kv.data[key]
		if dataVal.dataType == dataStructJson {
			if dataVal.structId != lastStructName && i+1 < len(keys) && kv.data[keys[i+1]].structId == dataVal.structId {
				// we have at least one more value of this type, so encode a header line
				var structValue map[string]interface{}
				if err = json.Unmarshal([]byte(dataVal.value), &structValue); err != nil {
					return err
				}
				lastStructName = dataVal.structId
				structFieldNames = structFieldNames[:0] // reset
				for fieldName := range structValue {
					structFieldNames = append(structFieldNames, fieldName)
				}
				slices.Sort(structFieldNames) // sort for consistency

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
					values[valueIdx] = structValue[fieldName]
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
