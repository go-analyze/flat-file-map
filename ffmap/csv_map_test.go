package ffmap

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeTestMap(t *testing.T) (string, *KeyValueCSV) {
	if t != nil {
		t.Helper()
	}

	tmpfile, err := os.CreateTemp("", "testm.*.csv")
	if t == nil {
		if err != nil {
			panic(err)
		}
	} else {
		require.NoError(t, err)
	}
	m, err := OpenCSV(tmpfile.Name())
	if t == nil {
		if err != nil {
			panic(err)
		}
	} else {
		require.NoError(t, err)
	}
	return tmpfile.Name(), m
}

type TestNamedStruct struct {
	Value     string
	ID        int
	Float     float64
	Bool      bool
	Map       map[string]TestNamedStruct
	MapIntKey map[int]string
	Time      time.Time
	Bytes     []byte
	IntSlice  []int
}

type TestAnyStruct struct {
	Value string
	Any   any
}

type TestPointerStruct struct {
	T *time.Time
	I *int
	S *string
	B *bool
	F *float64
}

type TestStructEmbedded struct {
	TestNamedStruct

	DirectStr string
}

type TestNestedStruct struct {
	Inner TestNamedStruct
	Ptr   *TestNamedStruct
}

type TestStructWithDeepNesting struct {
	Level1 struct {
		Level2 struct {
			Level3 struct {
				Value string
				Num   int
			}
		}
	}
}

type TestCustomJsonStruct struct {
	Value    string  `json:"v"`
	EmptyStr string  `json:"emptyStr,omitempty"`
	EmptyInt int     `json:"emptyInt,omitempty"`
	NilPtr   *string `json:"nilPtr,omitempty"`
	ZValue   string  `json:"zv"`
}

type TestStructWithSlice struct {
	Values []TestNamedStruct
}

type TestStructWithEmbeddedAnonymous struct {
	Embedded struct {
		Name string
		ID   int
	}
	Extra string
}

type TestStructWithPointerToAnonymous struct {
	Ptr *struct {
		Name string
		ID   int
	}
}

type TestStructWithPointerToEmpty struct {
	Ptr *struct{}
}

type TestCustomMarshaler struct {
	Value   string
	Encoded json.RawMessage
}

type TestStructWithEmbeddedCustomMarshaler struct {
	Embedded TestCustomMarshaler
	Extra    string
}

type TestStructWithMultiPointer struct {
	PPP ***int
}

type TestStructWithDeeplyNestedMap struct {
	Data map[string]interface{}
}

var (
	defaultStr = ""
	defaultI   = 0
	defaultT   = time.Time{}
	defaultB   = false
	defaultF   = 0.0
)

var stringReturnFunc = func(str string) string { // used in test cases where the key and value match
	return str
}

func TestKeyValueCSV_OpenAndCommit(t *testing.T) {
	t.Run("OpenEmpty", func(t *testing.T) {
		t.Parallel()
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		assert.Equal(t, 0, m.Size())
	})
	t.Run("OpenMissing", func(t *testing.T) {
		t.Parallel()
		tmpfile, err := os.CreateTemp("", "testm.*.csv")
		defer os.Remove(tmpfile.Name())
		require.NoError(t, err)
		require.NoError(t, os.Remove(tmpfile.Name()))
		m, err := OpenCSV(tmpfile.Name())
		require.NoError(t, err)

		assert.Equal(t, 0, m.Size())
	})
	t.Run("SaveAndLoadMaps", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)
		defer os.Remove(tmpFile)

		values := make(map[string]string)
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key%d", i)
			value := fmt.Sprintf("value%d", i)
			values[key] = value
			require.NoError(t, mOrig.Set(key, value))
		}
		require.NoError(t, mOrig.Commit())

		mNew, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)

		for key, expectedValue := range values {
			var actualValue string
			found, err := mNew.Get(key, &actualValue)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, expectedValue, actualValue)
		}
	})
	t.Run("SaveAndLoadNamedStruct", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)
		defer os.Remove(tmpFile)

		values := make(map[string]TestNamedStruct)
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key%d", i)
			value := TestNamedStruct{Value: fmt.Sprintf("value%d", i), ID: i}
			values[key] = value
			require.NoError(t, mOrig.Set(key, value))
		}
		require.NoError(t, mOrig.Commit())

		mNew, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)

		for key, expectedValue := range values {
			var actualValue TestNamedStruct
			found, err := mNew.Get(key, &actualValue)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, expectedValue, actualValue)
		}
	})
	t.Run("SaveAndLoadEmbeddedStruct", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)
		defer os.Remove(tmpFile)

		values := make(map[string]TestStructEmbedded)
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key%d", i)
			valueStr := fmt.Sprintf("value%d", i)
			value := TestStructEmbedded{
				TestNamedStruct: TestNamedStruct{Value: valueStr, ID: i},
				DirectStr:       valueStr,
			}
			values[key] = value
			require.NoError(t, mOrig.Set(key, value))
		}
		require.NoError(t, mOrig.Commit())

		mNew, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)

		for key, expectedValue := range values {
			var actualValue TestStructEmbedded
			found, err := mNew.Get(key, &actualValue)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, expectedValue, actualValue)
		}
	})
	t.Run("SaveAndLoadNestedStruct", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)
		defer os.Remove(tmpFile)

		values := make(map[string]TestNestedStruct)
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key%d", i)
			valueStr := fmt.Sprintf("value%d", i)
			value := TestNestedStruct{
				Inner: TestNamedStruct{Value: "i-" + valueStr, ID: i},
				Ptr:   &TestNamedStruct{Value: "p-" + valueStr, ID: i},
			}
			values[key] = value
			require.NoError(t, mOrig.Set(key, value))
		}
		require.NoError(t, mOrig.Commit())

		mNew, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)

		for key, expectedValue := range values {
			var actualValue TestNestedStruct
			found, err := mNew.Get(key, &actualValue)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, expectedValue, actualValue)
		}
	})
	t.Run("SaveAndLoadDeepNestedStruct", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)
		defer os.Remove(tmpFile)

		values := make(map[string]TestStructWithDeepNesting)
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key%d", i)
			value := TestStructWithDeepNesting{
				Level1: struct {
					Level2 struct {
						Level3 struct {
							Value string
							Num   int
						}
					}
				}{
					Level2: struct {
						Level3 struct {
							Value string
							Num   int
						}
					}{
						Level3: struct {
							Value string
							Num   int
						}{Value: fmt.Sprintf("value%d", i), Num: i},
					},
				},
			}
			values[key] = value
			require.NoError(t, mOrig.Set(key, value))
		}
		require.NoError(t, mOrig.Commit())

		mNew, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)

		for key, expectedValue := range values {
			var actualValue TestStructWithDeepNesting
			found, err := mNew.Get(key, &actualValue)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, expectedValue, actualValue)
		}
	})
	t.Run("SaveAndLoadAnyStructMixedTypes", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		values := map[string]TestAnyStruct{
			"float64": {Value: "float64", Any: 1.1},
			"str":     {Value: "str", Any: "str"},
			"nil":     {Value: "nil", Any: nil},
		}

		require.NoError(t, SetAll(m, values))
		require.NoError(t, m.Commit())

		mNew, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)

		for key, expectedValue := range values {
			var actualValue TestAnyStruct
			found, err := mNew.Get(key, &actualValue)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, expectedValue, actualValue)
		}
	})
	t.Run("SaveAndLoadCustomJsonStruct", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)
		defer os.Remove(tmpFile)

		var values []TestCustomJsonStruct
		for i := 0; i < 200; i++ {
			value := TestCustomJsonStruct{Value: fmt.Sprintf("value%d", i), ZValue: "z"}
			if i%11 == 0 {
				value.EmptyInt = 10
			} else if i%7 == 0 {
				value.EmptyStr = "foo"
			}
			values = append(values, value)
			require.NoError(t, mOrig.Set(value.Value, value))
		}
		require.NoError(t, mOrig.Commit())

		mNew, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)

		for _, expectedValue := range values {
			var actualValue TestCustomJsonStruct
			found, err := mNew.Get(expectedValue.Value, &actualValue)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, expectedValue, actualValue)
		}
	})
	t.Run("SaveAndLoadDeeplyNestedMapStruct", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)
		defer os.Remove(tmpFile)

		values := make(map[string]TestStructWithDeeplyNestedMap)
		for i := 0; i < 200; i++ {
			key := fmt.Sprintf("key%d", i)
			value := TestStructWithDeeplyNestedMap{map[string]interface{}{
				"level1": map[string]interface{}{
					"level2": map[string]interface{}{
						"level3": map[string]interface{}{"value": fmt.Sprintf("value%d", i)},
					},
				},
			}}
			values[key] = value
			require.NoError(t, mOrig.Set(key, value))
		}
		require.NoError(t, mOrig.Commit())

		mNew, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)

		for key, expectedValue := range values {
			var actualValue TestStructWithDeeplyNestedMap
			found, err := mNew.Get(key, &actualValue)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, expectedValue, actualValue)
		}
	})
	t.Run("CommitOrderString", func(t *testing.T) {
		t.Parallel()
		tmpFile1, m1 := makeTestMap(t)
		defer os.Remove(tmpFile1)
		tmpFile2, m2 := makeTestMap(t)
		defer os.Remove(tmpFile2)

		for a, b := range map[string]string{"foo1": "bar1", "foo2": "bar2", "foo3": "bar3", "foo4": "bar4"} {
			require.NoError(t, m1.Set(a, b))
			require.NoError(t, m1.Set(b, a))
			// m2 insertion order swapped
			require.NoError(t, m2.Set(b, a))
			require.NoError(t, m2.Set(a, b))
		}
		require.NoError(t, m1.Commit())
		require.NoError(t, m2.Commit())

		file1Content, err := os.ReadFile(tmpFile1)
		require.NoError(t, err)
		file2Content, err := os.ReadFile(tmpFile2)
		require.NoError(t, err)
		assert.Equal(t, string(file1Content), string(file2Content))
	})
	t.Run("CommitOrderMixed", func(t *testing.T) {
		t.Parallel()
		tmpFile1, m1 := makeTestMap(t)
		defer os.Remove(tmpFile1)
		tmpFile2, m2 := makeTestMap(t)
		defer os.Remove(tmpFile2)

		for a, b := range map[string]string{"foo1": "bar1", "foo2": "bar2", "foo3": "bar3", "foo4": "bar4"} {
			i := len(m1.KeySet())
			require.NoError(t, m1.Set(a, b))
			require.NoError(t, m1.Set(b, a))
			require.NoError(t, m1.Set("int-"+strconv.Itoa(i), i))
			require.NoError(t, m1.Set("map-"+strconv.Itoa(i), map[string]string{a: b}))
			require.NoError(t, m1.Set("struct-"+a, TestNamedStruct{Value: a}))
			require.NoError(t, m1.Set("struct-"+b, TestNamedStruct{Value: b}))
			// m2 insertion order swapped
			require.NoError(t, m2.Set("struct-"+b, TestNamedStruct{Value: b}))
			require.NoError(t, m2.Set("struct-"+a, TestNamedStruct{Value: a}))
			require.NoError(t, m2.Set("map-"+strconv.Itoa(i), map[string]string{a: b}))
			require.NoError(t, m2.Set("int-"+strconv.Itoa(i), i))
			require.NoError(t, m2.Set(b, a))
			require.NoError(t, m2.Set(a, b))
		}

		require.NoError(t, m1.Commit())
		require.NoError(t, m2.Commit())

		file1Content, err := os.ReadFile(tmpFile1)
		require.NoError(t, err)
		file2Content, err := os.ReadFile(tmpFile2)
		require.NoError(t, err)
		assert.Equal(t, string(file1Content), string(file2Content))
	})
	t.Run("SaveAndLoadAllTypes", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)
		defer os.Remove(tmpFile)

		testData := map[string]interface{}{
			"string":             "foo",
			"bool":               true,
			"float32":            float32(3.14),
			"float64":            3.1415,
			"int":                42,
			"int8":               int8(8),
			"int16":              int16(16),
			"int32":              int32(32),
			"int64":              int64(64),
			"uint":               uint(1),
			"uint8":              uint8(8),
			"uint16":             uint16(16),
			"uint32":             uint32(32),
			"uint64":             uint64(64),
			"complex64":          complex64(complex(5, 6)),
			"complex128":         complex(5, 6),
			"namedStruct":        TestNamedStruct{Value: "foo", ID: 1},
			"customJStruct":      TestCustomJsonStruct{Value: "foo", ZValue: "z"},
			"embeddedStruct":     TestStructEmbedded{TestNamedStruct: TestNamedStruct{Value: "foo", ID: 1}, DirectStr: "str"},
			"intSlice":           []int{1, 2, 3, 4},
			"int64Slice":         []int64{1000, 2000, 3000, 4000},
			"stringSlice":        []string{"a", "b", "c"},
			"byteSlice":          []byte{0x01, 0x02, 0x03, 0x04},
			"namedStructSlice":   []TestNamedStruct{{Value: "foo", ID: 1}, {Value: "bar", ID: 2}},
			"customJStructSlice": []TestCustomJsonStruct{{Value: "bar", ZValue: "z"}, {Value: "foo", ZValue: "z"}},
			"embeddedStructSlice": []TestStructEmbedded{
				{TestNamedStruct: TestNamedStruct{Value: "foo1", ID: 1}, DirectStr: "str1"},
				{TestNamedStruct: TestNamedStruct{Value: "foo2", ID: 2}, DirectStr: "str2"},
			},
			"mapStringString": map[string]string{"key1": "value1", "key2": "value2"},
		}

		require.NoError(t, SetAll(mOrig, testData))
		require.NoError(t, mOrig.Commit())

		mNew, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)

		for key, expectedValue := range testData {
			valPtr := reflect.New(reflect.TypeOf(expectedValue))
			found, err := mNew.Get(key, valPtr.Interface())
			require.NoError(t, err)
			assert.True(t, found)

			actualValue := valPtr.Elem().Interface()
			assert.Equal(t, expectedValue, actualValue)
		}
	})
	t.Run("CommitModTracking", func(t *testing.T) {
		t.Parallel()
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		for i, s := range []string{"foo1", "bar1", "foo2", "bar2", "foo3", "bar3", "foo4", "bar4"} {
			assert.Equal(t, i, m.memoryMap.modCount)
			require.NoError(t, m.Set(s, s))
		}
		assert.Zero(t, m.commitMod)
		require.NoError(t, m.Commit())
		assert.Equal(t, m.memoryMap.modCount, m.commitMod)
	})
	t.Run("CommitIgnored", func(t *testing.T) {
		t.Parallel()
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		require.NoError(t, SetSliceValues(m,
			[]string{"foo1", "bar1", "foo2", "bar2", "foo3", "bar3", "foo4", "bar4"}, stringReturnFunc))
		require.NoError(t, m.Commit())
		require.NoError(t, os.Remove(tmpFile)) // remove file as hack to verify commit does not apply

		require.NoError(t, m.Commit()) // no-op commit
		assert.NoFileExists(t, tmpFile)
	})
	t.Run("SaveAndLoadAllZeroStruct", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)
		defer os.Remove(tmpFile)

		value := TestNamedStruct{}
		require.NoError(t, mOrig.Set("allzero1", value))
		require.NoError(t, mOrig.Set("allzero2", value))
		require.NoError(t, mOrig.Commit())

		mNew, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)

		var actualValue TestNamedStruct
		found, err := mNew.Get("allzero1", &actualValue)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, value, actualValue)
	})
	t.Run("SaveAndLoadCustomMarshaler", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)
		defer os.Remove(tmpFile)

		values := map[string]TestStructWithEmbeddedCustomMarshaler{
			"key1": {Embedded: TestCustomMarshaler{Value: "value1", Encoded: json.RawMessage(`"encoded1"`)}, Extra: "extra1"},
		}

		for key, value := range values {
			require.NoError(t, mOrig.Set(key, value))
		}
		require.NoError(t, mOrig.Commit())

		mNew, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)

		for key, expectedValue := range values {
			var actualValue TestStructWithEmbeddedCustomMarshaler
			found, err := mNew.Get(key, &actualValue)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, expectedValue.Embedded.Value, actualValue.Embedded.Value)
			assert.Equal(t, string(expectedValue.Embedded.Encoded), string(actualValue.Embedded.Encoded))
			assert.Equal(t, expectedValue.Extra, actualValue.Extra)
		}
	})
}

func TestKeyValueCSV_Size(t *testing.T) {
	t.Parallel()
	tmpFile, m := makeTestMap(t)
	defer os.Remove(tmpFile)

	for i := 0; i < 10; i++ {
		require.NoError(t, m.Set(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)))
		assert.Equal(t, i+1, m.Size())
	}
}

func TestKeyValueCSV_EncodeValueType(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name             string
		value            interface{}
		expectedDataType int
	}{
		{
			name:             "String",
			value:            "testString",
			expectedDataType: dataString,
		},
		{
			name:             "Bool",
			value:            true,
			expectedDataType: dataBool,
		},
		{
			name:             "Float32",
			value:            float32(3.14),
			expectedDataType: dataFloat,
		},
		{
			name:             "Float64",
			value:            float64(3.1415),
			expectedDataType: dataFloat,
		},
		{
			name:             "Int",
			value:            int(42),
			expectedDataType: dataInt,
		},
		{
			name:             "Int8",
			value:            int8(8),
			expectedDataType: dataInt,
		},
		{
			name:             "Int16",
			value:            int16(16),
			expectedDataType: dataInt,
		},
		{
			name:             "Int32",
			value:            int32(32),
			expectedDataType: dataInt,
		},
		{
			name:             "Int64",
			value:            int64(64),
			expectedDataType: dataInt,
		},
		{
			name:             "Uint",
			value:            uint(1),
			expectedDataType: dataUint,
		},
		{
			name:             "Uint8",
			value:            uint8(8),
			expectedDataType: dataUint,
		},
		{
			name:             "Uint16",
			value:            uint16(16),
			expectedDataType: dataUint,
		},
		{
			name:             "Uint32",
			value:            uint32(32),
			expectedDataType: dataUint,
		},
		{
			name:             "Uint64",
			value:            uint64(64),
			expectedDataType: dataUint,
		},
		{
			name:             "Complex64",
			value:            complex64(complex(5, 6)),
			expectedDataType: dataComplexNum,
		},
		{
			name:             "Complex128",
			value:            complex128(complex(7, 8)),
			expectedDataType: dataComplexNum,
		},
		{
			name:             "CustomStruct",
			value:            struct{ Name string }{"Test"},
			expectedDataType: dataStructJson,
		},
		{
			name: "NamedStruct",
			value: TestNamedStruct{
				Value: "foo",
				ID:    123,
				Map:   map[string]TestNamedStruct{"bar": {Value: "bar", ID: 987, Bool: true}},
			},
			expectedDataType: dataStructJson,
		},
		{
			name: "PointerStruct",
			value: TestPointerStruct{
				S: &defaultStr,
				I: &defaultI,
				T: &defaultT,
			},
			expectedDataType: dataStructJson,
		},
		{
			name: "CustomJsonStruct",
			value: TestCustomJsonStruct{
				Value: "foo",
			},
			expectedDataType: dataStructJson,
		},
		{
			name:             "Map",
			value:            map[string]string{"foo1": "bar1", "foo2": "bar2"},
			expectedDataType: dataMap,
		},
		{
			name:             "ByteArray",
			value:            [4]byte{1, 2, 3, 4},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "ByteSlice",
			value:            []byte{1, 2, 3, 4},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "IntArray",
			value:            [4]int{1, 2, 3, 4},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "IntSlice",
			value:            []int{1, 2, 3, 4},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "Int64Array",
			value:            [4]int64{1000, 2000, 3000, 4000},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "Int64Slice",
			value:            []int64{1000, 2000, 3000, 4000},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "StringArray",
			value:            [2]string{"foo", "bar"},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "StringSlice",
			value:            []string{"foo", "bar"},
			expectedDataType: dataArraySlice,
		},
		{
			name: "StructSlice",
			value: []TestNamedStruct{
				{Value: "foo", ID: 123},
				{Value: "bar", ID: 456},
			},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "Time",
			value:            time.Now(),
			expectedDataType: dataStructJson,
		},
		{
			name:             "MapPointer",
			value:            &map[string]string{"foo": "bar", "key": "value"},
			expectedDataType: dataMap,
		},
		{
			name:             "StructPointer",
			value:            &TestNamedStruct{Value: "foo", ID: 123},
			expectedDataType: dataStructJson,
		},
		{
			name:             "Float32Slice",
			value:            []float32{1.1, 2.2, 3.3},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "Float64Slice",
			value:            []float64{1.1, 2.2, 3.3},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "UintSlice",
			value:            []uint{1, 2, 3, 4},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "NestedIntSlice",
			value:            [][]int{{1, 2}, {3, 4}},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "NestedStringSlice",
			value:            [][]string{{"a", "b"}, {"c", "d"}},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "EmptySlice",
			value:            []int{},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "EmptyArray",
			value:            [0]int{},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "PointerSlice",
			value:            []*int{new(int), new(int)},
			expectedDataType: dataArraySlice,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dataItem, err := encodeValue(tc.value)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedDataType, dataItem.dataType)
		})
	}
}

func TestKeyValueCSV_SetAndGet(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		setValue interface{}
		getValue interface{}
	}{
		{
			name:     "String",
			setValue: "testString",
			getValue: new(string),
		},
		{
			name:     "Bool",
			setValue: true,
			getValue: new(bool),
		},
		{
			name:     "Float32",
			setValue: float32(3.14),
			getValue: new(float32),
		},
		{
			name:     "Float64",
			setValue: float64(3.1415),
			getValue: new(float64),
		},
		{
			name:     "Int",
			setValue: int(42),
			getValue: new(int),
		},
		{
			name:     "Int8",
			setValue: int8(8),
			getValue: new(int8),
		},
		{
			name:     "Int16",
			setValue: int16(16),
			getValue: new(int16),
		},
		{
			name:     "Int32",
			setValue: int32(32),
			getValue: new(int32),
		},
		{
			name:     "Int64",
			setValue: int64(64),
			getValue: new(int64),
		},
		{
			name:     "Uint",
			setValue: uint(1),
			getValue: new(uint),
		},
		{
			name:     "Uint8",
			setValue: uint8(8),
			getValue: new(uint8),
		},
		{
			name:     "Uint16",
			setValue: uint16(16),
			getValue: new(uint16),
		},
		{
			name:     "Uint32",
			setValue: uint32(32),
			getValue: new(uint32),
		},
		{
			name:     "Uint64",
			setValue: uint64(64),
			getValue: new(uint64),
		},
		{
			name:     "Complex64",
			setValue: complex64(complex(5, 6)),
			getValue: new(complex64),
		},
		{
			name:     "Complex128",
			setValue: complex128(complex(7, 8)),
			getValue: new(complex128),
		},
		{
			name:     "Complex64NegImag",
			setValue: complex64(complex(5, -6)),
			getValue: new(complex64),
		},
		{
			name:     "Complex128Neg",
			setValue: complex128(complex(-7, -8)),
			getValue: new(complex128),
		},
		{
			name:     "CustomStruct",
			setValue: struct{ Name string }{"Test"},
			getValue: new(struct{ Name string }),
		},
		{
			name: "NamedStruct",
			setValue: TestNamedStruct{
				Value:     "foo",
				ID:        123,
				Map:       map[string]TestNamedStruct{"bar": {Value: "bar", ID: 987, Bool: true}},
				MapIntKey: map[int]string{1: "one", 2: "two"},
				Time:      time.Date(2025, 2, 16, 10, 20, 40, 20, time.UTC),
				IntSlice:  []int{0, 0, 0, 0},
			},
			getValue: new(TestNamedStruct),
		},
		{
			name: "MapValueWithZeroKey",
			setValue: TestNamedStruct{
				Value:     "foo",
				MapIntKey: map[int]string{0: "zero", 1: "one"},
			},
			getValue: new(TestNamedStruct),
		},
		{
			name: "MapValueWithZeroValue",
			setValue: TestNamedStruct{
				Value:     "foo",
				MapIntKey: map[int]string{1: "", 2: ""},
			},
			getValue: new(TestNamedStruct),
		},
		{
			name:     "NamedStructEmpty",
			setValue: TestNamedStruct{},
			getValue: new(TestNamedStruct),
		},
		{
			name:     "NamedStructPointer",
			setValue: &TestNamedStruct{},
			getValue: new(*TestNamedStruct),
		},
		{
			name: "PointerStruct",
			setValue: TestPointerStruct{
				S: &defaultStr,
				I: &defaultI,
				T: &defaultT,
			},
			getValue: new(TestPointerStruct),
		},
		{
			name:     "EmbeddedStruct",
			setValue: TestStructEmbedded{TestNamedStruct: TestNamedStruct{Value: "foo", ID: 1}, DirectStr: "str"},
			getValue: new(TestStructEmbedded),
		},
		{
			name: "CustomJsonStructEmpty",
			setValue: TestCustomJsonStruct{
				Value: "foo",
			},
			getValue: new(TestCustomJsonStruct),
		},
		{
			name: "CustomJsonStructFilled",
			setValue: TestCustomJsonStruct{
				Value:    "foo",
				EmptyStr: "str",
				EmptyInt: -1,
				ZValue:   "z",
			},
			getValue: new(TestCustomJsonStruct),
		},
		{
			name:     "Map",
			setValue: map[string]string{"foo1": "bar1", "foo2": "bar2"},
			getValue: new(map[string]string),
		},
		{
			name:     "MapWithZeroKey",
			setValue: map[string]string{"": "foo"},
			getValue: new(map[string]string),
		},
		{
			name:     "MapWithZeroValue",
			setValue: map[string]string{"foo": ""},
			getValue: new(map[string]string),
		},
		{
			name:     "ByteArray",
			setValue: [4]byte{1, 2, 3, 4},
			getValue: new([4]byte),
		},
		{
			name:     "ByteSlice",
			setValue: []byte{1, 2, 3, 4},
			getValue: new([]byte),
		},
		{
			name:     "IntArray",
			setValue: [4]int{1, 2, 3, 4},
			getValue: new([4]int),
		},
		{
			name:     "IntSlice",
			setValue: []int{1, 2, 3, 4},
			getValue: new([]int),
		},
		{
			name:     "Int64Array",
			setValue: [4]int64{1000, 2000, 3000, 4000},
			getValue: new([4]int64),
		},
		{
			name:     "Int64Slice",
			setValue: []int64{1000, 2000, 3000, 4000},
			getValue: new([]int64),
		},
		{
			name:     "StringArray",
			setValue: [2]string{"foo", "bar"},
			getValue: new([2]string),
		},
		{
			name:     "StringSlice",
			setValue: []string{"foo", "bar"},
			getValue: new([]string),
		},
		{
			name: "StructSlice",
			setValue: []TestNamedStruct{
				{Value: "foo", ID: 123},
				{Value: "bar", ID: 456},
			},
			getValue: new([]TestNamedStruct),
		},
		{
			name:     "MapPointer",
			setValue: &map[string]string{"foo": "bar", "key": "value"},
			getValue: new(*map[string]string),
		},
		{
			name:     "StructPointer",
			setValue: &TestNamedStruct{Value: "foo", ID: 123},
			getValue: new(*TestNamedStruct),
		},
		{
			name:     "NestedMap",
			setValue: map[string]map[string]int{"outer": {"inner": 42}},
			getValue: new(map[string]map[string]int),
		},
		{
			name:     "MixedTypeSlice",
			setValue: []interface{}{"two", 3.0},
			getValue: new([]interface{}),
		},
		{
			name:     "PointerSlice",
			setValue: []*int{new(int), new(int)},
			getValue: new([]*int),
		},
		{
			name:     "UintSlice",
			setValue: []uint{10, 20, 30},
			getValue: new([]uint),
		},
		{
			name:     "Uint8Slice",
			setValue: []uint8{1, 2, 3, 4},
			getValue: new([]uint8),
		},
		{
			name:     "Uint16Slice",
			setValue: []uint16{100, 200, 300},
			getValue: new([]uint16),
		},
		{
			name:     "Uint32Slice",
			setValue: []uint32{1000, 2000, 3000},
			getValue: new([]uint32),
		},
		{
			name:     "Uint64Slice",
			setValue: []uint64{10000, 20000, 30000},
			getValue: new([]uint64),
		},
		{
			name:     "Float32Slice",
			setValue: []float32{3.14, 6.28},
			getValue: new([]float32),
		},
		{
			name:     "Float64Slice",
			setValue: []float64{3.1415, 2.718},
			getValue: new([]float64),
		},
		{
			name:     "BoolSlice",
			setValue: []bool{true, false, true},
			getValue: new([]bool),
		},
		{
			name:     "UintArray",
			setValue: [4]uint{10, 20, 30, 40},
			getValue: new([4]uint),
		},
		{
			name:     "Uint8Array",
			setValue: [4]uint8{1, 2, 3, 4},
			getValue: new([4]uint8),
		},
		{
			name:     "Uint16Array",
			setValue: [4]uint16{100, 200, 300, 400},
			getValue: new([4]uint16),
		},
		{
			name:     "Uint32Array",
			setValue: [4]uint32{1000, 2000, 3000, 4000},
			getValue: new([4]uint32),
		},
		{
			name:     "Uint64Array",
			setValue: [4]uint64{10000, 20000, 30000, 40000},
			getValue: new([4]uint64),
		},
		{
			name:     "Float32Array",
			setValue: [4]float32{3.14, 6.28, 9.42, 12.56},
			getValue: new([4]float32),
		},
		{
			name:     "Float64Array",
			setValue: [4]float64{3.1415, 2.718, 1.618, 0.577},
			getValue: new([4]float64),
		},
		{
			name:     "BoolArray",
			setValue: [4]bool{true, false, true, false},
			getValue: new([4]bool),
		},
		{
			name: "NestedStruct",
			setValue: TestNestedStruct{
				Inner: TestNamedStruct{
					Value: "nested",
					ID:    999,
				},
				Ptr: &TestNamedStruct{
					Value: "pointer",
					ID:    888,
				},
			},
			getValue: new(TestNestedStruct),
		},
		{
			name: "StructWithSlice",
			setValue: TestStructWithSlice{
				Values: []TestNamedStruct{
					{Value: "first", ID: 1},
					{Value: "second", ID: 2},
				},
			},
			getValue: new(TestStructWithSlice),
		},
		{
			name: "PointerStructExpanded",
			setValue: TestPointerStruct{
				S: &defaultStr,
				I: &defaultI,
				T: &defaultT,
				B: &defaultB,
				F: &defaultF,
			},
			getValue: new(TestPointerStruct),
		},
		{
			name:     "StructSliceAnonymous",
			setValue: []struct{ Name string }{{Name: "Alice"}, {Name: "Bob"}},
			getValue: new([]struct{ Name string }),
		},
		{
			name:     "PointerStructSlice",
			setValue: []*TestPointerStruct{{S: &defaultStr, I: &defaultI}, {T: &defaultT, F: &defaultF}},
			getValue: new([]*TestPointerStruct),
		},
		{
			name:     "EmptyStruct",
			setValue: struct{}{},
			getValue: new(struct{}),
		},
		{
			name: "StructWithMixedFields",
			setValue: struct {
				Str  string
				Ptr  *string
				Bool bool
				Num  int
			}{Str: "test", Ptr: &defaultStr, Bool: true, Num: 42},
			getValue: new(struct {
				Str  string
				Ptr  *string
				Bool bool
				Num  int
			}),
		},
		{
			name: "StructWithEmbeddedStruct",
			setValue: struct {
				Embedded TestNamedStruct
				Extra    string
			}{Embedded: TestNamedStruct{Value: "embed", ID: 999}, Extra: "extra"},
			getValue: new(struct {
				Embedded TestNamedStruct
				Extra    string
			}),
		},
		{
			name: "PointerToEmptyStruct",
			setValue: &struct {
				Name string
			}{},
			getValue: new(*struct {
				Name string
			}),
		},
		{
			name:     "RuneSlice",
			setValue: []rune{'a', 'b', 'c'},
			getValue: new([]rune),
		},
		{
			name:     "ByteSliceWithZeros",
			setValue: []byte{0, 1, 2, 3, 0},
			getValue: new([]byte),
		},
		{
			name:     "EmptyByteSlice",
			setValue: []byte{},
			getValue: new([]byte),
		},
		{
			name:     "NilByteSlice",
			setValue: ([]byte)(nil),
			getValue: new([]byte),
		},
		{
			name:     "StructPointerSlice",
			setValue: []*TestNamedStruct{{Value: "first", ID: 1}, nil, {Value: "second", ID: 2}},
			getValue: new([]*TestNamedStruct),
		},
		{
			name: "StructWithEmbeddedAnonymousStruct",
			setValue: TestStructWithEmbeddedAnonymous{
				Embedded: struct {
					Name string
					ID   int
				}{Name: "embedded", ID: 777},
				Extra: "extra",
			},
			getValue: new(TestStructWithEmbeddedAnonymous),
		},
		{
			name: "StructWithPointerToAnonymousStruct",
			setValue: TestStructWithPointerToAnonymous{
				Ptr: &struct {
					Name string
					ID   int
				}{Name: "ptrAnon", ID: 888},
			},
			getValue: new(TestStructWithPointerToAnonymous),
		},
		{
			name: "StructWithPointerToEmptyStruct",
			setValue: TestStructWithPointerToEmpty{
				Ptr: &struct{}{},
			},
			getValue: new(TestStructWithPointerToEmpty),
		},
		{
			name: "MapWithEmptyValues",
			setValue: map[string]interface{}{
				"emptyStr":   "",
				"emptyFloat": 0.0,
				"emptyBool":  false,
			},
			getValue: new(map[string]interface{}),
		},
		{
			name: "NestedPointerMap",
			setValue: map[string]*TestNamedStruct{
				"key1": {Value: "nested1", ID: 111},
				"key2": nil,
			},
			getValue: new(map[string]*TestNamedStruct),
		},
		{
			name: "StructWithInterfaceField",
			setValue: struct {
				Value interface{}
			}{Value: "interfaceString"},
			getValue: new(struct {
				Value interface{}
			}),
		},
		{
			name: "StructWithEmbeddedAnonymousPointer",
			setValue: struct {
				Ptr *struct {
					Name string
					ID   int
				}
			}{Ptr: &struct {
				Name string
				ID   int
			}{Name: "anonymous", ID: 789}},
			getValue: new(struct {
				Ptr *struct {
					Name string
					ID   int
				}
			}),
		},
		{
			name: "DeeplyNestedStruct",
			setValue: struct {
				Level1 struct {
					Level2 struct {
						Level3 struct {
							Value string
							Num   int
						}
					}
				}
			}{
				Level1: struct {
					Level2 struct {
						Level3 struct {
							Value string
							Num   int
						}
					}
				}{
					Level2: struct {
						Level3 struct {
							Value string
							Num   int
						}
					}{
						Level3: struct {
							Value string
							Num   int
						}{Value: "deep", Num: 100},
					},
				},
			},
			getValue: new(struct {
				Level1 struct {
					Level2 struct {
						Level3 struct {
							Value string
							Num   int
						}
					}
				}
			}),
		},
		{
			name: "LargeIntSlice",
			setValue: []int{
				1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
			},
			getValue: new([]int),
		},
		{
			name: "MapWithMixedTypes",
			setValue: map[string]interface{}{
				"string": "foo",
				"float":  42.0,
				"bool":   true,
			},
			getValue: new(map[string]interface{}),
		},
		{
			name:     "CustomMarshalerStruct",
			setValue: TestCustomMarshaler{Value: "test", Encoded: json.RawMessage(`"custom"`)},
			getValue: new(TestCustomMarshaler),
		},
		{
			name: "StructWithEmbeddedCustomMarshaler",
			setValue: TestStructWithEmbeddedCustomMarshaler{
				Embedded: TestCustomMarshaler{Value: "inner", Encoded: json.RawMessage(`"encoded"`)},
				Extra:    "extra",
			},
			getValue: new(TestStructWithEmbeddedCustomMarshaler),
		},
		{
			name: "StructWithMultiPointer",
			setValue: TestStructWithMultiPointer{
				PPP: func() ***int {
					p := &defaultI
					pp := &p
					ppp := &pp
					return ppp
				}(),
			},
			getValue: new(TestStructWithMultiPointer),
		},
		{
			name: "StructWithDeepNesting",
			setValue: TestStructWithDeepNesting{
				Level1: struct {
					Level2 struct {
						Level3 struct {
							Value string
							Num   int
						}
					}
				}{
					Level2: struct {
						Level3 struct {
							Value string
							Num   int
						}
					}{
						Level3: struct {
							Value string
							Num   int
						}{Value: "deep", Num: 100},
					},
				},
			},
			getValue: new(TestStructWithDeepNesting),
		},
		{
			name: "StructWithDeeplyNestedMap",
			setValue: TestStructWithDeeplyNestedMap{
				Data: map[string]interface{}{
					"level1": map[string]interface{}{
						"level2": map[string]interface{}{
							"level3": map[string]interface{}{"value": "deep"},
						},
					},
				},
			},
			getValue: new(TestStructWithDeeplyNestedMap),
		},
		{
			name:     "LargeByteSlice",
			setValue: make([]byte, 1024),
			getValue: new([]byte),
		},
		{
			name:     "MixedByteArrayAndSlice",
			setValue: struct{ Data [4]byte }{Data: [4]byte{1, 2, 3, 4}},
			getValue: new(struct{ Data [4]byte }),
		},
		{
			name: "StructWithEmptyMap",
			setValue: struct {
				EmptyMap map[string]int
			}{EmptyMap: map[string]int{}},
			getValue: new(struct {
				EmptyMap map[string]int
			}),
		},
		{
			name: "StructWithNilMap",
			setValue: struct {
				NilMap map[string]int
			}{NilMap: nil},
			getValue: new(struct {
				NilMap map[string]int
			}),
		},
		{
			name: "StructWithEmptySlice",
			setValue: struct {
				EmptySlice []string
			}{EmptySlice: []string{}},
			getValue: new(struct {
				EmptySlice []string
			}),
		},
		{
			name: "StructWithNilSlice",
			setValue: struct {
				NilSlice []string
			}{NilSlice: nil},
			getValue: new(struct {
				NilSlice []string
			}),
		},
		{
			name: "StructWithPointerToEmptyStruct",
			setValue: struct {
				Ptr *struct{}
			}{Ptr: &struct{}{}},
			getValue: new(struct {
				Ptr *struct{}
			}),
		},
		{
			name: "SliceOfStructsWithDefaultValues",
			setValue: []TestNamedStruct{
				{Value: "", ID: 0},
				{Value: "filled", ID: 123},
			},
			getValue: new([]TestNamedStruct),
		},
		{
			name: "StructWithInterfaceNil",
			setValue: struct {
				Field interface{}
			}{Field: nil},
			getValue: new(struct{ Field interface{} }),
		},
		{
			name: "StructWithPointerToSlice",
			setValue: struct {
				PtrSlice *[]int
			}{PtrSlice: &[]int{1, 2, 3}},
			getValue: new(struct{ PtrSlice *[]int }),
		},
		{
			name: "EmptyStructPointerSlice",
			setValue: []*struct {
				Name string
			}{},
			getValue: new([]*struct {
				Name string
			}),
		},
		{
			name: "NilStructPointerSlice",
			setValue: []*struct {
				Name string
			}(nil),
			getValue: new([]*struct {
				Name string
			}),
		},
		{
			name: "EmptyMapWithNonEmptyValues",
			setValue: map[string]interface{}{
				"emptyMap":  map[string]interface{}{},
				"filledMap": map[string]interface{}{"a": 1.0},
			},
			getValue: new(map[string]interface{}),
		},
		{
			name:     "SliceWithNilValues",
			setValue: []interface{}{nil, "non-nil", nil},
			getValue: new([]interface{}),
		},
		{
			name: "StructWithDoublePointer",
			setValue: struct {
				Ptr **int
			}{
				Ptr: func() **int {
					p := &defaultI
					pp := &p
					return pp
				}(),
			},
			getValue: new(struct {
				Ptr **int
			}),
		},
		{
			name: "StructWithTriplePointerLevels",
			setValue: struct {
				PP ***int
			}{
				PP: func() ***int {
					p := &defaultI
					pp := &p
					ppp := &pp
					return ppp
				}(),
			},
			getValue: new(struct {
				PP ***int
			}),
		},
		{
			name: "StructWithNestedPointerSlice",
			setValue: struct {
				PtrSlice *[]*int
			}{
				PtrSlice: &[]*int{new(int), nil},
			},
			getValue: new(struct {
				PtrSlice *[]*int
			}),
		},
		{
			name: "MixedTypeMapWithNilValues",
			setValue: map[string]interface{}{
				"string": "hello",
				"float":  3.14,
				"bool":   true,
				"slice":  []interface{}{"a", "b"},
				"map":    map[string]interface{}{"a": 1.0, "b": 2.0},
				"nil":    nil,
			},
			getValue: new(map[string]interface{}),
		},
		{
			name: "NestedMixedMap",
			setValue: map[string]interface{}{
				"outer": map[string]interface{}{
					"innerString": "value",
					"innerMap":    map[string]interface{}{"key": 123.0},
					"innerNil":    nil,
				},
			},
			getValue: new(map[string]interface{}),
		},
		{
			name: "StructWithRawMessage",
			setValue: struct {
				Raw json.RawMessage
			}{
				Raw: json.RawMessage(`{"key":"value"}`),
			},
			getValue: new(struct {
				Raw json.RawMessage
			}),
		},
		{
			name: "StructWithNilPtrInt",
			setValue: struct {
				Field *int
			}{Field: nil},
			getValue: new(struct {
				Field *int
			}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpFile, m := makeTestMap(t)
			defer os.Remove(tmpFile)

			key := "testKey-" + tc.name
			require.NoError(t, m.Set(key, tc.setValue))

			found, err := m.Get(key, tc.getValue)
			require.NoError(t, err)
			assert.True(t, found)

			assert.Equal(t, reflect.ValueOf(tc.setValue).Interface(), reflect.ValueOf(tc.getValue).Elem().Interface())
			assert.True(t, reflect.DeepEqual(reflect.ValueOf(tc.setValue).Interface(), reflect.ValueOf(tc.getValue).Elem().Interface()))
		})
	}

	// extra tests
	t.Run("PointerToEmptySlice", func(t *testing.T) {
		type TestPointerSlice struct {
			Slice *[]int
		}
		emptySlice := make([]int, 0)
		orig := TestPointerSlice{Slice: &emptySlice}
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		key := "pointerEmptySlice"
		require.NoError(t, m.Set(key, orig))

		var got TestPointerSlice
		found, err := m.Get(key, &got)
		require.NoError(t, err)
		assert.True(t, found)
		assert.NotNil(t, got.Slice)
		assert.Equal(t, []int{}, *got.Slice)
	})
}

func TestKeyValueCSV_SetMapValues(t *testing.T) {
	t.Run("nil_kv", func(t *testing.T) {
		require.Error(t, SetMapValues(nil, make(map[string]string)))
	})
	t.Run("nil_map", func(t *testing.T) {
		t.Parallel()
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		require.NoError(t, SetMapValues[string](m, nil))
		require.Equal(t, 0, m.Size())
	})
	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		values := map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
			"d": "4",
		}

		require.NoError(t, SetMapValues[string](m, values))
		require.Equal(t, 4, m.Size())
		for k, v1 := range values {
			var v2 string
			ok, err := m.Get(k, &v2)
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, v1, v2)
		}
	})
	t.Run("struct", func(t *testing.T) {
		t.Parallel()
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		values := map[string]TestNamedStruct{
			"a": {
				Value: "1",
				ID:    1,
			},
			"b": {
				Value: "2",
				ID:    2,
			},
			"c": {
				Value: "3",
				ID:    3,
			},
			"d": {
				Value: "4",
				ID:    4,
			},
		}

		require.NoError(t, SetMapValues(m, values))
		require.Equal(t, 4, m.Size())
		for k, v1 := range values {
			var v2 TestNamedStruct
			ok, err := m.Get(k, &v2)
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, v1, v2)
		}
	})
	t.Run("ptr", func(t *testing.T) {
		t.Parallel()
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		values := map[string]*TestNamedStruct{
			"a": {
				Value: "1",
				ID:    1,
			},
			"b": {
				Value: "2",
				ID:    2,
			},
			"c": {
				Value: "3",
				ID:    3,
			},
			"d": {
				Value: "4",
				ID:    4,
			},
		}

		require.NoError(t, SetMapValues(m, values))
		require.Equal(t, 4, m.Size())
		for k, v1 := range values {
			v2 := &TestNamedStruct{}
			ok, err := m.Get(k, v2)
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, v1, v2)
		}
	})
	t.Run("middle_error", func(t *testing.T) {
		t.Parallel()
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		values := map[string]*TestNamedStruct{
			"a": {
				Value: "1",
				ID:    1,
			},
			"b": nil,
			"c": {
				Value: "3",
				ID:    3,
			},
			"d": {
				Value: "4",
				ID:    4,
			},
		}

		require.Error(t, SetMapValues(m, values))
		require.Equal(t, 3, m.Size())
	})
}

func TestKeyValueCSV_SetSliceValues(t *testing.T) {
	t.Run("nil_kv", func(t *testing.T) {
		require.Error(t, SetSliceValues(nil, make([]int, 0), strconv.Itoa))
	})
	t.Run("nil_slice", func(t *testing.T) {
		t.Parallel()
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		require.NoError(t, SetSliceValues(m, make([]int, 0), strconv.Itoa))
		require.Equal(t, 0, m.Size())
	})
	t.Run("nil_func", func(t *testing.T) {
		t.Parallel()
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		require.Error(t, SetSliceValues(m, make([]int, 1), nil))
		require.Equal(t, 0, m.Size())
	})
	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		values := []int{0, 1, 2, 3, 4}

		require.NoError(t, SetSliceValues(m, values, strconv.Itoa))
		require.Equal(t, 5, m.Size())
		for _, v1 := range values {
			var v2 int
			ok, err := m.Get(strconv.Itoa(v1), &v2)
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, v1, v2)
		}
	})
	t.Run("struct", func(t *testing.T) {
		t.Parallel()
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		values := []TestNamedStruct{
			{
				Value: "1",
				ID:    1,
			},
			{
				Value: "2",
				ID:    2,
			},
			{
				Value: "3",
				ID:    3,
			},
			{
				Value: "4",
				ID:    4,
			},
		}

		require.NoError(t, SetSliceValues(m, values, func(value TestNamedStruct) string {
			return value.Value
		}))
		require.Equal(t, 4, m.Size())
		for _, v1 := range values {
			var v2 TestNamedStruct
			ok, err := m.Get(v1.Value, &v2)
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, v1, v2)
		}
	})
	t.Run("ptr", func(t *testing.T) {
		t.Parallel()
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		values := []*TestNamedStruct{
			{
				Value: "1",
				ID:    1,
			},
			{
				Value: "2",
				ID:    2,
			},
			{
				Value: "3",
				ID:    3,
			},
			{
				Value: "4",
				ID:    4,
			},
		}

		require.NoError(t, SetSliceValues(m, values, func(value *TestNamedStruct) string {
			return value.Value
		}))
		require.Equal(t, 4, m.Size())
		for _, v1 := range values {
			v2 := &TestNamedStruct{}
			ok, err := m.Get(v1.Value, v2)
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, v1, v2)
		}
	})
	t.Run("middle_error", func(t *testing.T) {
		t.Parallel()
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		values := []*TestNamedStruct{
			{
				Value: "1",
				ID:    1,
			},
			nil,
			{
				Value: "3",
				ID:    3,
			},
			{
				Value: "4",
				ID:    4,
			},
		}

		require.Error(t, SetSliceValues(m, values, func(value *TestNamedStruct) string {
			if value == nil {
				return ""
			}
			return value.Value
		}))
		require.Equal(t, 3, m.Size())
		for _, v1 := range values {
			if v1 == nil {
				continue
			}
			v2 := &TestNamedStruct{}
			ok, err := m.Get(v1.Value, v2)
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, v1, v2)
		}
	})
}

func TestKeyValueCSV_SetError(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		setValue interface{}
	}{
		{
			name:     "NilValue",
			setValue: nil,
		},
		{
			name: "StructWithFunc",
			setValue: struct {
				FuncField func() int
				Value     int
			}{FuncField: func() int { return 42 }, Value: 100},
		},
		{
			name:     "NilPointerToEmptySlice",
			setValue: (*[]int)(nil),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpFile, m := makeTestMap(t)
			defer os.Remove(tmpFile)

			require.Error(t, m.Set(tc.name, tc.setValue))
			assert.Equal(t, 0, m.Size())
		})
	}
}

func TestKeyValueCSV_GetOverflowError(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		setValue interface{}
		getValue interface{}
	}{
		{
			name:     "Float32Over",
			setValue: math.MaxFloat32 * 2,
			getValue: new(float32),
		},
		{
			name:     "Float32Under",
			setValue: -math.MaxFloat32 * 2,
			getValue: new(float32),
		},
		{
			name:     "Int8Over",
			setValue: int16(math.MaxInt8 + 1),
			getValue: new(int8),
		},
		{
			name:     "Int8Under",
			setValue: int16(math.MinInt8 - 1),
			getValue: new(int8),
		},
		{
			name:     "Int16Over",
			setValue: int32(math.MaxInt16 + 1),
			getValue: new(int16),
		},
		{
			name:     "Int16Under",
			setValue: int32(math.MinInt16 - 1),
			getValue: new(int16),
		},
		{
			name:     "Int32Over",
			setValue: int64(math.MaxInt32 + 1),
			getValue: new(int32),
		},
		{
			name:     "Int32Under",
			setValue: int64(math.MinInt32 - 1),
			getValue: new(int32),
		},
		{
			name:     "Uint8Over",
			setValue: uint16(math.MaxUint8 + 1),
			getValue: new(uint8),
		},
		{
			name:     "Uint16Over",
			setValue: uint32(math.MaxUint16 + 1),
			getValue: new(uint16),
		},
		{
			name:     "Uint32Over",
			setValue: uint64(math.MaxUint32 + 1),
			getValue: new(uint32),
		},
		{
			name:     "Complex64RealOver",
			setValue: complex(math.MaxFloat32*2, 6),
			getValue: new(complex64),
		},
		{
			name:     "Complex64RealUnder",
			setValue: complex(-math.MaxFloat32*2, 6),
			getValue: new(complex64),
		},
		{
			name:     "Complex64ImagOver",
			setValue: complex(5, math.MaxFloat32*2),
			getValue: new(complex64),
		},
		{
			name:     "Complex64ImagUnder",
			setValue: complex(5, -math.MaxFloat32*2),
			getValue: new(complex64),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpFile, m := makeTestMap(t)
			defer os.Remove(tmpFile)

			key := "testKey-" + tc.name
			require.NoError(t, m.Set(key, tc.setValue))

			found, err := m.Get(key, tc.getValue)
			require.Error(t, err)
			assert.False(t, found)
		})
	}
}

func TestKeyValueCSV_GetWithZeroFieldsSet(t *testing.T) {
	t.Parallel()
	tmpFile, m := makeTestMap(t)
	defer os.Remove(tmpFile)

	zeroStruct := TestNamedStruct{}
	err := m.Set("zero", zeroStruct)
	require.NoError(t, err)

	valueStruct := &TestNamedStruct{ // pre-filled with values that should be cleared
		Value: "value",
		ID:    1,
		Float: 1.0,
		Bool:  true,
		Time:  time.Now(),
		Bytes: []byte("foo"),
	}
	_, err = m.Get("zero", valueStruct)
	require.NoError(t, err)

	assert.Equal(t, zeroStruct, *valueStruct)
}

func TestKeyValueCSV_GetInvalidType(t *testing.T) {
	t.Parallel()
	tmpFile, m := makeTestMap(t)
	defer os.Remove(tmpFile)

	testData := map[string]interface{}{
		"string":           "foo",
		"bool":             true,
		"float":            3.1415,
		"int":              42,
		"uint":             uint(16),
		"complex":          complex(5, 6),
		"intSlice":         []int{1, 2, 3, 4},
		"stringSlice":      []string{"a", "b", "c"},
		"namedStructSlice": []TestNamedStruct{{Value: "foo", ID: 1}, {Value: "bar", ID: 2}},
		"mapStringString":  map[string]string{"key1": "value1", "key2": "value2"},
	}

	require.NoError(t, SetAll(m, testData))

	for key := range testData {
		for mismatchKey, mismatchValue := range testData {
			if key == mismatchKey {
				continue
			}

			valPtr := reflect.New(reflect.TypeOf(mismatchValue))
			found, err := m.Get(key, valPtr.Interface())
			require.Errorf(t, err, "error expected looking up %s with type %s", key, mismatchKey)
			assert.False(t, found)
		}
	}
}

func TestKeyValueCSV_EncodingSize(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name                string
		value               interface{}
		expectedStrSize     int
		expectedFileSizeOne int64
		expectedFileSizeTwo int64
	}{
		{
			name:                "String",
			value:               "testString",
			expectedStrSize:     10,
			expectedFileSizeOne: 35,
			expectedFileSizeTwo: 64,
		},
		{
			name:                "Bool",
			value:               true,
			expectedStrSize:     1,
			expectedFileSizeOne: 24,
			expectedFileSizeTwo: 42,
		},
		{
			name:                "Float32",
			value:               float32(3.1414999961853027),
			expectedStrSize:     18,
			expectedFileSizeOne: 44,
			expectedFileSizeTwo: 82,
		},
		{
			name:                "Float64",
			value:               float64(3.1415),
			expectedStrSize:     6,
			expectedFileSizeOne: 32,
			expectedFileSizeTwo: 58,
		},
		{
			name:                "Int",
			value:               int(42),
			expectedStrSize:     2,
			expectedFileSizeOne: 24,
			expectedFileSizeTwo: 42,
		},
		{
			name:                "Int8",
			value:               int8(8),
			expectedStrSize:     1,
			expectedFileSizeOne: 24,
			expectedFileSizeTwo: 42,
		},
		{
			name:                "Int16",
			value:               int16(16),
			expectedStrSize:     2,
			expectedFileSizeOne: 26,
			expectedFileSizeTwo: 46,
		},
		{
			name:                "Int32",
			value:               int32(32),
			expectedStrSize:     2,
			expectedFileSizeOne: 26,
			expectedFileSizeTwo: 46,
		},
		{
			name:                "Int64",
			value:               int64(64),
			expectedStrSize:     2,
			expectedFileSizeOne: 26,
			expectedFileSizeTwo: 46,
		},
		{
			name:                "Uint",
			value:               uint(1),
			expectedStrSize:     1,
			expectedFileSizeOne: 24,
			expectedFileSizeTwo: 42,
		},
		{
			name:                "Uint8",
			value:               uint8(8),
			expectedStrSize:     1,
			expectedFileSizeOne: 25,
			expectedFileSizeTwo: 44,
		},
		{
			name:                "Uint16",
			value:               uint16(16),
			expectedStrSize:     2,
			expectedFileSizeOne: 27,
			expectedFileSizeTwo: 48,
		},
		{
			name:                "Uint32",
			value:               uint32(32),
			expectedStrSize:     2,
			expectedFileSizeOne: 27,
			expectedFileSizeTwo: 48,
		},
		{
			name:                "Uint64",
			value:               uint64(64),
			expectedStrSize:     2,
			expectedFileSizeOne: 27,
			expectedFileSizeTwo: 48,
		},
		{
			name:                "Complex64",
			value:               complex64(complex(5, 6)),
			expectedStrSize:     6,
			expectedFileSizeOne: 34,
			expectedFileSizeTwo: 62,
		},
		{
			name:                "Complex128",
			value:               complex128(complex(7, 8)),
			expectedStrSize:     6,
			expectedFileSizeOne: 35,
			expectedFileSizeTwo: 64,
		},
		{
			name:                "CustomStruct",
			value:               struct{ Name string }{"Test"},
			expectedStrSize:     15,
			expectedFileSizeOne: 52,
			expectedFileSizeTwo: 113,
		},
		{
			name: "CustomStructEmptyField",
			value: struct {
				Name     string
				EmptyStr string
			}{Name: "Test"},
			expectedStrSize:     15,
			expectedFileSizeOne: 62,
			expectedFileSizeTwo: 149,
		},
		{
			name: "NamedStruct",
			value: TestNamedStruct{
				Value: "foo",
				ID:    123,
				Map:   map[string]TestNamedStruct{"bar": {Value: "bar", ID: 987, Bool: true}},
			},
			expectedStrSize:     135,
			expectedFileSizeOne: 193,
			expectedFileSizeTwo: 362,
		},
		{
			name: "PointerStruct",
			value: TestPointerStruct{
				S: &defaultStr,
				I: &defaultI,
				T: &defaultT,
			},
			expectedStrSize:     41,
			expectedFileSizeOne: 85,
			expectedFileSizeTwo: 167,
		},
		{
			name: "StructEmbedded",
			value: TestStructEmbedded{
				TestNamedStruct: TestNamedStruct{
					Value: "foo",
					ID:    123,
					Bool:  true,
					Map:   map[string]TestNamedStruct{"bar": {Value: "bar", ID: 987, Bool: true}},
				},
				DirectStr: "str",
			},
			expectedStrSize:     165,
			expectedFileSizeOne: 232,
			expectedFileSizeTwo: 412,
		},
		{
			name: "CustomJsonStructEmpty",
			value: TestCustomJsonStruct{
				Value: "foo",
			},
			expectedStrSize:     11,
			expectedFileSizeOne: 57,
			expectedFileSizeTwo: 135,
		},
		{
			name: "StructWithDeeplyNestedMap",
			value: TestStructWithDeeplyNestedMap{
				Data: map[string]interface{}{
					"level1": map[string]interface{}{
						"level2": map[string]interface{}{
							"level3": map[string]interface{}{"value": "deep"},
						},
					},
				},
			},
			expectedStrSize:     58,
			expectedFileSizeOne: 116,
			expectedFileSizeTwo: 258,
		},
		{
			name:                "CustomMarshalerStruct",
			value:               TestCustomMarshaler{Value: "test", Encoded: json.RawMessage(`"custom"`)},
			expectedStrSize:     35,
			expectedFileSizeOne: 85,
			expectedFileSizeTwo: 170,
		},
		{
			name: "StructWithEmbeddedCustomMarshaler",
			value: TestStructWithEmbeddedCustomMarshaler{
				Embedded: TestCustomMarshaler{Value: "inner", Encoded: json.RawMessage(`"encoded"`)},
				Extra:    "extra",
			},
			expectedStrSize:     66,
			expectedFileSizeOne: 134,
			expectedFileSizeTwo: 284,
		},
		{
			name:                "Map",
			value:               map[string]string{"foo1": "bar1", "foo2": "bar2"},
			expectedStrSize:     29,
			expectedFileSizeOne: 62,
			expectedFileSizeTwo: 118,
		},
		{
			name:                "MapZeroKey",
			value:               map[string]string{"": "foo"},
			expectedStrSize:     10,
			expectedFileSizeOne: 46,
			expectedFileSizeTwo: 86,
		},
		{
			name:                "MapZeroValue",
			value:               map[string]string{"foo": ""},
			expectedStrSize:     10,
			expectedFileSizeOne: 48,
			expectedFileSizeTwo: 90,
		},
		{
			name:                "ByteSlice",
			value:               []byte{1, 2, 3, 4},
			expectedStrSize:     10,
			expectedFileSizeOne: 42,
			expectedFileSizeTwo: 78,
		},
		{
			name:                "IntSlice",
			value:               []int{1, 2, 3, 4},
			expectedStrSize:     9,
			expectedFileSizeOne: 38,
			expectedFileSizeTwo: 70,
		},
		{
			name:                "Int64Slice",
			value:               []int64{1000, 2000, 3000, 4000},
			expectedStrSize:     21,
			expectedFileSizeOne: 52,
			expectedFileSizeTwo: 98,
		},
		{
			name:                "StringSlice",
			value:               []string{"foo", "bar"},
			expectedStrSize:     13,
			expectedFileSizeOne: 49,
			expectedFileSizeTwo: 92,
		},
		{
			name: "StructSlice",
			value: []TestNamedStruct{
				{Value: "foo", ID: 123},
				{Value: "bar", ID: 456},
			},
			expectedStrSize:     111,
			expectedFileSizeOne: 163,
			expectedFileSizeTwo: 320,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpFile, m := makeTestMap(t)
			defer os.Remove(tmpFile)

			key1 := "testKey1-" + tc.name
			require.NoError(t, m.Set(key1, tc.value))

			valueHolder, found := m.memoryMap.data[key1]
			assert.True(t, found)
			value := valueHolder.value
			assert.Lenf(t, value, tc.expectedStrSize, "unexpected encoded value size: %s", value)

			require.NoError(t, m.Commit())
			verifyFileSize(t, tmpFile, tc.expectedFileSizeOne)

			require.NoError(t, m.Set("testKey2-"+tc.name, tc.value))
			require.NoError(t, m.Commit())
			verifyFileSize(t, tmpFile, tc.expectedFileSizeTwo)
		})
	}

	// extra tests
	t.Run("MixedFieldsSize", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		values := map[string]TestNamedStruct{
			"key1":  {Value: "value", ID: 1},
			"str":   {Value: "str"},
			"int":   {ID: 100},
			"bool":  {Bool: true},
			"map":   {Map: map[string]TestNamedStruct{"foo": {Value: "bar", ID: 123, Bool: true}, "nestedEmpty": {}}},
			"empty": {},
			"full": {
				Value: "foo",
				ID:    123,
				Bool:  true,
				Map:   map[string]TestNamedStruct{"bar": {Value: "bar", ID: 987, Bool: true}},
			},
		}

		require.NoError(t, SetAll(m, values))

		require.NoError(t, m.Commit())
		verifyFileSize(t, tmpFile, 670)
	})
	t.Run("MixedFieldTypes", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		values := map[string]TestAnyStruct{
			"int": {Value: "int", Any: 1},
			"str": {Value: "str", Any: "str"},
			"nil": {Value: "nil", Any: nil},
		}

		require.NoError(t, SetAll(m, values))

		require.NoError(t, m.Commit())
		verifyFileSize(t, tmpFile, 114)
	})
}

func verifyFileSize(t *testing.T, fileStr string, expectedSize int64) {
	t.Helper()

	file, err := os.Open(fileStr)
	require.NoError(t, err)
	defer file.Close()
	fileInfo, err := file.Stat()
	require.NoError(t, err)

	if fileInfo.Size() != expectedSize {
		// Read the contents of the file for debugging
		fileContents, err := os.ReadFile(fileStr)
		require.NoError(t, err)

		// Use require.Equal for the assertion and provide file contents as additional info
		assert.Equal(t, expectedSize, fileInfo.Size(),
			"Unexpected file size. File contents: \n%s", string(fileContents))
	}
}

func TestKeyValueCSV_ContainsKey(t *testing.T) {
	t.Parallel()
	tmpFile, m := makeTestMap(t)
	defer os.Remove(tmpFile)

	for _, k := range []string{"foo1", "bar1", "foo2", "bar2"} {
		require.NoError(t, m.Set(k, k))

		assert.True(t, m.ContainsKey(k))
		assert.False(t, m.ContainsKey("-"+k))
	}
}

func TestKeyValueCSV_KeySet(t *testing.T) {
	t.Parallel()
	tmpFile, m := makeTestMap(t)
	defer os.Remove(tmpFile)

	keys := []string{"foo1", "bar1", "foo2", "bar2", "foo3", "bar3", "foo4", "bar4"}
	require.NoError(t, SetSliceValues(m, keys, stringReturnFunc))

	keySet := m.KeySet()
	assert.Len(t, keySet, len(keys))
	for _, k := range keys {
		assert.Contains(t, keySet, k)
	}
}

func TestKeyValueCSV_Delete(t *testing.T) {
	t.Parallel()
	tmpFile, m := makeTestMap(t)
	defer os.Remove(tmpFile)

	key := "keyDelete"
	value := "testValue"
	require.NoError(t, m.Set(key, value))

	m.Delete(key)

	verifyEmpty(t, m, key)
}

func TestKeyValueCSV_DeleteAll(t *testing.T) {
	t.Parallel()
	tmpFile, m := makeTestMap(t)
	defer os.Remove(tmpFile)

	key := "keyDeleteAll"
	require.NoError(t, m.Set(key, "value"))
	require.NoError(t, m.Set("foo"+key, "value"))

	m.DeleteAll()

	verifyEmpty(t, m, key)
}

func verifyEmpty(t *testing.T, m *KeyValueCSV, oldKey string) {
	t.Helper()

	var result string
	found, err := m.Get(oldKey, &result)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, 0, m.Size())
	assert.Empty(t, m.KeySet())
}

func TestKeyValueCSV_IsFloat32Overflow(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		value    float64
		overflow bool
	}{
		{name: "Zero", value: 0, overflow: false},
		{name: "Small", value: 123.456, overflow: false},
		{name: "MaxFloat32", value: math.MaxFloat32, overflow: false},
		{name: "NegMaxFloat32", value: -math.MaxFloat32, overflow: false},
		{name: "OverPos", value: math.MaxFloat32 * 2, overflow: true},
		{name: "OverNeg", value: -math.MaxFloat32 * 2, overflow: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.overflow, isFloat32Overflow(tc.value))
		})
	}
}

func FuzzLoadRecords(f *testing.F) {
	f.Add("3,AAAA,BBBB")
	f.Add("4,AAAA,11")
	f.Add("5,AAAA,11")
	f.Add("6,AAAA,1.1")
	f.Add("7,AAAA,(5+6i)")
	f.Add("8,AAAA,t")
	f.Add("8,AAAA,f")
	f.Add("9,AAAA,\"AQIDBA==\"")
	f.Add("9,AAAA,[1,1,1,1]")
	f.Add("9,AAAA,[\"Z\",\"Z\",\"Z\"]")
	f.Add("10,AAAA,{\"KK1\":\"VV1\",\"KK2\":\"VV2\"}")
	f.Add("5,AAAA1,11\n5,AAAA2,22")
	f.Add("0,struct{Namestring},Name\n1,AAAA1,[\"Test\"]\n1,AAAA2,[\"Test\"]")
	f.Add("9,AAAA1,[1,1,1,1]\n9,AAAA2,[2,2,2,2]")
	f.Add("9,AAAA1,[\"ZZ\",\"XX\"]\n9,AAAA2,[\"ZZ\",\"XX\"]")

	f.Fuzz(func(t *testing.T, encodedLines string) {
		var records [][]string
		records = append(records, []string{currentFileVersion})
		for _, line := range strings.Split(encodedLines, "\n") {
			lineSlice := strings.Split(line, ",")
			if !strings.HasPrefix(line, "0,") { // only three values expected
				var recombinedLine []string
				recombinedLine = append(recombinedLine, lineSlice[0])
				if len(lineSlice) > 1 {
					recombinedLine = append(recombinedLine, lineSlice[1])
				}
				if len(lineSlice) > 2 {
					recombinedLine = append(recombinedLine, strings.Join(lineSlice[2:], ","))
				}
				lineSlice = recombinedLine
			}
			records = append(records, lineSlice)
		}

		require.NotPanics(t, func() {
			kvMap := &KeyValueCSV{
				memoryMap: &memoryJsonMap{
					data: make(map[string]dataItem),
				},
			}

			_ = kvMap.loadRecords(records)
		})
	})
}

func FuzzDecodeValue(f *testing.F) {
	f.Add("AAAA")
	f.Add("[1000,2000,3000,4000]")
	f.Add("[\"A\",\"A\",\"A\"]")
	f.Add("1111")
	f.Add("(5+6i)")
	f.Add("{\"KK1\":\"VV1\",\"KK2\":\"VV2\"}")
	f.Add("2.11")
	f.Add("\"AQIDBA==\"")
	f.Add("t")
	f.Add("f")

	f.Fuzz(func(t *testing.T, encodedValue string) {
		require.NotPanics(t, func() {
			_ = decodeValue(dataStructJson, encodedValue, new(map[interface{}]interface{}))
		})
		require.NotPanics(t, func() {
			_ = decodeValue(dataString, encodedValue, new(string))
		})
		require.NotPanics(t, func() {
			_ = decodeValue(dataInt, encodedValue, new(int64))
		})
		require.NotPanics(t, func() {
			_ = decodeValue(dataUint, encodedValue, new(uint64))
		})
		require.NotPanics(t, func() {
			_ = decodeValue(dataFloat, encodedValue, new(float64))
		})
		require.NotPanics(t, func() {
			_ = decodeValue(dataComplexNum, encodedValue, new(complex128))
		})
		require.NotPanics(t, func() {
			_ = decodeValue(dataBool, encodedValue, new(bool))
		})
		require.NotPanics(t, func() {
			_ = decodeValue(dataArraySlice, encodedValue, new([]interface{}))
		})
		require.NotPanics(t, func() {
			_ = decodeValue(dataMap, encodedValue, new(map[interface{}]interface{}))
		})
	})
}

func TestKeyValueCSV_ErrorTypes(t *testing.T) {
	t.Parallel()

	t.Run("ValidationError_NilMap", func(t *testing.T) {
		err := SetMapValues(nil, map[string]string{"key": "value"})
		require.Error(t, err)

		var validationErr *ValidationError
		require.ErrorAs(t, err, &validationErr)
		assert.Contains(t, validationErr.Message, "nil MutableFFMap")
	})

	t.Run("ValidationError_NilKeyProvider", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		err := SetSliceValues(m, []string{"value"}, nil)
		require.Error(t, err)

		var validationErr *ValidationError
		require.ErrorAs(t, err, &validationErr)
		assert.Contains(t, validationErr.Message, "nil keyProvider function")
	})

	t.Run("ValidationError_InvalidFileFormat", func(t *testing.T) {
		// Create a file with invalid format
		tmpFile, err := os.CreateTemp("", "invalid.*.csv")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		// Write invalid header
		_, err = tmpFile.WriteString("invalid_version\ngarbage_data\n")
		require.NoError(t, err)
		tmpFile.Close()

		_, err = OpenCSV(tmpFile.Name())
		require.Error(t, err)

		var validationErr *ValidationError
		require.ErrorAs(t, err, &validationErr)
		assert.Contains(t, validationErr.Message, "invalid header line")
	})

	t.Run("EncodingError_PropagatedFromMemoryMap", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		// Test that encoding errors from the underlying memory map are propagated
		err := m.Set("nil_key", nil)
		require.Error(t, err)

		var encodingErr *EncodingError
		require.ErrorAs(t, err, &encodingErr)
		assert.Equal(t, "nil_key", encodingErr.Key)
		assert.Contains(t, encodingErr.Message, "cannot encode nil value")
	})

	t.Run("TypeMismatchError_PropagatedFromMemoryMap", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		require.NoError(t, m.Set("string_key", "string_value"))

		var intResult int
		found, err := m.Get("string_key", &intResult)
		assert.False(t, found)
		require.Error(t, err)

		var typeMismatchErr *TypeMismatchError
		require.ErrorAs(t, err, &typeMismatchErr)
		assert.Equal(t, "string_key", typeMismatchErr.Key)
		assert.Contains(t, typeMismatchErr.Message, "expected")
	})

	t.Run("ErrorTypes_DistinctInCSVContext", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		// Test encoding error
		encodingErr := m.Set("key1", nil)
		var encErr *EncodingError
		var typeErr1 *TypeMismatchError
		var valErr1 *ValidationError

		require.ErrorAs(t, encodingErr, &encErr)
		assert.False(t, errors.As(encodingErr, &typeErr1))
		assert.False(t, errors.As(encodingErr, &valErr1))

		// Test validation error
		validationErr := SetMapValues(nil, map[string]string{"key": "value"})
		var encErr2 *EncodingError
		var typeErr2 *TypeMismatchError
		var valErr2 *ValidationError

		require.ErrorAs(t, validationErr, &valErr2)
		assert.False(t, errors.As(validationErr, &encErr2))
		assert.False(t, errors.As(validationErr, &typeErr2))
	})
}
