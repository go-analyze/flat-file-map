package ffmap

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
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
	dataFloat
	dataBool
	dataMap
)

const currentFileVersion = "ver:0"

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
			if len(record) != 3 {
				return fmt.Errorf("unexpected csv struct value column count: %v, line: %v", len(record), i+1)
			}
			var values []interface{}
			if err := json.Unmarshal([]byte(record[2]), &values); err != nil {
				return err
			}
			if len(values) != len(currStructValueNames) {
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
			kv.data[record[1]] = dataItem{dataType: dataType, structId: currStructName, value: string(encodedStruct)}
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

func (kv *KeyValueCSV) Set(key string, value interface{}) error {
	var dataType int
	var structId string
	var strVal string
	switch v := value.(type) {
	case string:
		dataType = dataString
		strVal = v
	case bool:
		dataType = dataBool
		strVal = strconv.FormatBool(v)
	case float32, float64:
		dataType = dataFloat
		strVal = strconv.FormatFloat(reflect.ValueOf(v).Float(), 'f', -1, 64)
	case int, int8, int16, int32, int64:
		dataType = dataInt
		strVal = strconv.FormatInt(reflect.ValueOf(v).Int(), 10)
	default:
		val := reflect.ValueOf(value)
		if val.Kind() == reflect.Map {
			dataType = dataMap
		} else {
			dataType = dataStructJson
			// this id is only used for comparison but must remain consistent for a given file version
			structId = strings.ReplaceAll(reflect.ValueOf(value).Type().String(), " ", "")
		}
		bytes, err := json.Marshal(v)
		if err != nil {
			return err
		}
		strVal = string(bytes)
	}
	kv.data[key] = dataItem{dataType: dataType, structId: structId, value: strVal}
	return nil
}

func (kv *KeyValueCSV) Delete(key string) {
	delete(kv.data, key)
}

func (kv *KeyValueCSV) Get(key string, value interface{}) (bool, error) {
	dataVal, ok := kv.data[key]
	if !ok {
		return false, nil
	}

	// TODO - validate dataVal type matches the type provided here
	v := reflect.ValueOf(value).Elem()
	switch v.Kind() {
	case reflect.String:
		v.SetString(dataVal.value)
	case reflect.Bool:
		bVal, err := strconv.ParseBool(dataVal.value)
		if err != nil {
			return false, err
		}
		v.SetBool(bVal)
	case reflect.Float32, reflect.Float64:
		fVal, err := strconv.ParseFloat(dataVal.value, 64)
		if err != nil {
			return false, err
		}
		v.SetFloat(fVal)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		iVal, err := strconv.ParseInt(dataVal.value, 10, 64)
		if err != nil {
			return false, err
		}
		v.SetInt(iVal)
	default:
		// map and json types are handled uniformly at this point
		err := json.Unmarshal([]byte(dataVal.value), value)
		if err != nil {
			return false, err
		}
	}
	return true, nil
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
	var structHeaders []string
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
				structHeaders = []string{}
				for fieldName := range structValue {
					structHeaders = append(structHeaders, fieldName)
				}
				slices.Sort(structHeaders) // sort for consistency

				writer.Write(append([]string{strconv.Itoa(dataStructHeader), dataVal.structId}, structHeaders...))
			}

			if dataVal.structId == lastStructName { // append value only
				var structValue map[string]interface{}
				if err := json.Unmarshal([]byte(dataVal.value), &structValue); err != nil {
					return err
				}
				var values []interface{}
				for _, fieldName := range structHeaders {
					values = append(values, structValue[fieldName])
				}

				valueJsonBytes, err := json.Marshal(values)
				if err != nil {
					return err
				}
				if err := writer.Write([]string{strconv.Itoa(dataStructValue), key, string(valueJsonBytes)}); err != nil {
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
