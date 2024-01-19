package ffmap

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

// KeyValueCSV provides a primarily in-memory key value map, with the ability to load and commit the contents to disk.
type KeyValueCSV struct {
	filename string
	data     map[string]dataItem
}

type dataItem struct {
	dataType int
	structId string
	value    string
}

const (
	dataStructHeader = iota // Used in CSV, defines a header for struct values (size optimization)
	dataStructValue         // Used in CSV, defines a struct instance values (size optimization)
	dataStructJson
	dataString
	dataInt
	dataUint
	dataFloat
	dataComplexNum
	dataBool
	dataSlice
	dataMap
)

const currentFileVersion = "ver:0"

func dataTypePrefix(dataType int) (string, error) {
	switch dataType {
	case dataString:
		return "s:", nil
	case dataInt:
		return "i:", nil
	case dataUint:
		return "u:", nil
	case dataFloat:
		return "f:", nil
	case dataComplexNum:
		return "c:", nil
	case dataBool:
		return "b:", nil
	case dataSlice:
		return "js:", nil
	case dataMap:
		return "jm:", nil
	case dataStructJson:
		return "j:", nil
	}
	return "", fmt.Errorf("unhandled dataType: %v", dataType)
}

func instanceForPrefix(fieldHeader string) (int, interface{}, string, error) {
	if strings.HasPrefix(fieldHeader, "s:") {
		return dataString, new(string), fieldHeader[2:], nil
	} else if strings.HasPrefix(fieldHeader, "i:") {
		return dataInt, new(int64), fieldHeader[2:], nil
	} else if strings.HasPrefix(fieldHeader, "u:") {
		return dataUint, new(uint64), fieldHeader[2:], nil
	} else if strings.HasPrefix(fieldHeader, "f:") {
		return dataFloat, new(float64), fieldHeader[2:], nil
	} else if strings.HasPrefix(fieldHeader, "c:") {
		return dataComplexNum, new(complex128), fieldHeader[2:], nil
	} else if strings.HasPrefix(fieldHeader, "b:") {
		return dataBool, new(bool), fieldHeader[2:], nil
	} else if strings.HasPrefix(fieldHeader, "js:") {
		return dataSlice, new(map[string]interface{}), fieldHeader[3:], nil
	} else if strings.HasPrefix(fieldHeader, "jm:") {
		return dataMap, new(map[string]interface{}), fieldHeader[3:], nil
	} else if strings.HasPrefix(fieldHeader, "j:") {
		return dataStructJson, new(map[string]interface{}), fieldHeader[2:], nil
	} else {
		return -1, nil, "", fmt.Errorf("unknown prefix in field name: %s", fieldHeader)
	}
}

// loadFromDisk updates the map with data from the disk.
func (kv *KeyValueCSV) loadFromDisk() error {
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
			if len(record) != 2+len(currStructValueNames) {
				return fmt.Errorf("unexpected csv struct value column count: %v, line: %v", len(record), i+1)
			}
			structValue := make(map[string]interface{})
			for i, fieldTypeAndName := range currStructValueNames {
				dataType, value, fieldName, err := instanceForPrefix(fieldTypeAndName)
				if err != nil {
					return err
				}
				if err = decodeValue(dataType, record[i+2], value); err != nil {
					return err
				}
				structValue[fieldName] = value
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
	return len(kv.data)
}

func encodeValue(value interface{}) (*dataItem, error) {
	var dataType int
	var structId string
	var strVal string
	switch v := value.(type) {
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
		if val.Kind() == reflect.Slice {
			dataType = dataSlice
		} else if val.Kind() == reflect.Map {
			dataType = dataMap
		} else {
			dataType = dataStructJson
			if val.IsValid() {
				// this id is only used for comparison but must remain consistent for a given file version
				structId = strings.ReplaceAll(val.Type().String(), " ", "")
			} else {
				structId = "nil"
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
				return fmt.Errorf("int overflow")
			}
		case reflect.Int32:
			if iVal < math.MinInt32 || iVal > math.MaxInt32 {
				return fmt.Errorf("int32 overflow")
			}
		case reflect.Int16:
			if iVal < math.MinInt16 || iVal > math.MaxInt16 {
				return fmt.Errorf("int16 overflow")
			}
		case reflect.Int8:
			if iVal < math.MinInt8 || iVal > math.MaxInt8 {
				return fmt.Errorf("int8 overflow")
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
				return fmt.Errorf("uint overflow")
			}
		case reflect.Uint32:
			if uVal > math.MaxUint32 {
				return fmt.Errorf("uint32 overflow")
			}
		case reflect.Uint16:
			if uVal > math.MaxUint16 {
				return fmt.Errorf("uint16 overflow")
			}
		case reflect.Uint8:
			if uVal > math.MaxUint8 {
				return fmt.Errorf("uint8 overflow")
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
				return fmt.Errorf("float32 overflow")
			}
		default:
			return fmt.Errorf("expected float type but got %v", v.Kind())
		}

		v.SetFloat(fVal)
	case dataComplexNum:
		var real, imag float64
		_, err := fmt.Sscanf(encodedValue, "(%f+%fi)", &real, &imag)
		if err != nil {
			return err
		}

		switch v.Kind() {
		case reflect.Complex128:
		case reflect.Complex64:
			if isFloat32Overflow(real) {
				return fmt.Errorf("complex real float32 overflow")
			} else if isFloat32Overflow(imag) {
				return fmt.Errorf("complex imaginary float32 overflow")
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
	case dataSlice, dataMap, dataStructJson:
		err := json.Unmarshal([]byte(encodedValue), value)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unexpected error, Get needs to implement data type %v", encodedValue)
	}
	return nil
}

func (kv *KeyValueCSV) Set(key string, value interface{}) error {
	item, err := encodeValue(value)
	if err != nil {
		return err
	}
	kv.data[key] = *item
	return nil
}

func (kv *KeyValueCSV) Delete(key string) {
	delete(kv.data, key)
}

func (kv *KeyValueCSV) Get(key string, value interface{}) (bool, error) {
	dataVal, ok := kv.data[key]
	if !ok {
		return false, nil
	} else if err := decodeValue(dataVal.dataType, dataVal.value, value); err != nil {
		return false, err
	} else {
		return true, nil
	}
}

func isFloat32Overflow(fVal float64) bool {
	f32Val := float32(fVal)
	if math.IsInf(float64(f32Val), 0) {
		return true
	}
	return false
}

func (kv *KeyValueCSV) ContainsKey(key string) bool {
	_, found := kv.data[key]
	return found
}

func (kv *KeyValueCSV) KeySet() []string {
	var keys []string
	for key := range kv.data {
		keys = append(keys, key)
	}
	return keys
}

func (kv *KeyValueCSV) Commit() error {
	file, err := os.Create(kv.filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// sort keys so output is in a consistent order
	keys := kv.KeySet()
	slices.Sort(keys)
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
				structFieldNames = []string{}
				for fieldName, _ := range structValue {
					structFieldNames = append(structFieldNames, fieldName)
				}
				slices.Sort(structFieldNames) // sort for consistency
				structHeaders := make([]string, len(structFieldNames))
				for i, fieldName := range structFieldNames {
					fieldValue := structValue[fieldName]
					for j := i + 1; fieldValue == nil && j < len(keys) && kv.data[keys[j]].structId == dataVal.structId; j++ {
						var nextStructValue map[string]interface{}
						if err = json.Unmarshal([]byte(kv.data[keys[j]].value), &nextStructValue); err != nil {
							return err
						}
						fieldValue = nextStructValue[fieldName]
					}
					item, err := encodeValue(fieldValue)
					if err != nil {
						return err
					}
					prefix, err := dataTypePrefix(item.dataType)
					if err != nil {
						return err
					}
					structHeaders[i] = prefix + fieldName
				}

				if err := writer.Write(append([]string{strconv.Itoa(dataStructHeader), dataVal.structId}, structHeaders...)); err != nil {
					return err
				}
			}

			if dataVal.structId == lastStructName { // append value only
				var structValue map[string]interface{}
				if err := json.Unmarshal([]byte(dataVal.value), &structValue); err != nil {
					return err
				}
				var values []string
				for _, fieldName := range structFieldNames {
					item, err := encodeValue(structValue[fieldName])
					if err != nil {
						return err
					}
					values = append(values, item.value)
				}

				if err := writer.Write(append([]string{strconv.Itoa(dataStructValue), key}, values...)); err != nil {
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
