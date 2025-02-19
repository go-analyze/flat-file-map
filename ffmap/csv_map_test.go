package ffmap

import (
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

func TestOpenAndCommit(t *testing.T) {
	t.Run("OpenEmpty", func(t *testing.T) {
		t.Parallel()
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		assert.Equal(t, 0, m.Size())
	})
	t.Run("OpenMissing", func(t *testing.T) {
		t.Parallel()
		tmpfile, err := os.CreateTemp("", "testm.*.csv")
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

		mNew, err := OpenCSV(tmpFile)
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

		mNew, err := OpenCSV(tmpFile)
		require.NoError(t, err)

		for key, expectedValue := range values {
			var actualValue TestNamedStruct
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

		mNew, err := OpenCSV(tmpFile)
		require.NoError(t, err)

		for _, expectedValue := range values {
			var actualValue TestCustomJsonStruct
			found, err := mNew.Get(expectedValue.Value, &actualValue)
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
			"intSlice":           []int{1, 2, 3, 4},
			"int64Slice":         []int64{1000, 2000, 3000, 4000},
			"stringSlice":        []string{"a", "b", "c"},
			"byteSlice":          []byte{0x01, 0x02, 0x03, 0x04},
			"namedStructSlice":   []TestNamedStruct{{Value: "foo", ID: 1}, {Value: "bar", ID: 2}},
			"customJStructSlice": []TestCustomJsonStruct{{Value: "bar", ZValue: "z"}, {Value: "foo", ZValue: "z"}},
			"mapStringString":    map[string]string{"key1": "value1", "key2": "value2"},
		}

		for key, value := range testData {
			require.NoError(t, mOrig.Set(key, value))
		}
		require.NoError(t, mOrig.Commit())

		mNew, err := OpenCSV(tmpFile)
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
			assert.Equal(t, i, m.modCount)
			require.NoError(t, m.Set(s, s))
		}
		assert.Zero(t, m.commitMod)
		require.NoError(t, m.Commit())
		assert.Equal(t, m.modCount, m.commitMod)
	})
	t.Run("CommitIgnored", func(t *testing.T) {
		t.Parallel()
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		for _, s := range []string{"foo1", "bar1", "foo2", "bar2", "foo3", "bar3", "foo4", "bar4"} {
			require.NoError(t, m.Set(s, s))
		}
		require.NoError(t, m.Commit())
		require.NoError(t, os.Remove(tmpFile)) // remove file as hack to verify commit does not apply

		require.NoError(t, m.Commit()) // no-op commit
		assert.NoFileExists(t, tmpFile)
	})
}

func TestSize(t *testing.T) {
	t.Parallel()
	tmpFile, m := makeTestMap(t)
	defer os.Remove(tmpFile)

	for i := 0; i < 10; i++ {
		require.NoError(t, m.Set(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)))
		assert.Equal(t, i+1, m.Size())
	}
}

type TestNamedStruct struct {
	Value string
	ID    int
	Float float64
	Bool  bool
	Map   map[string]TestNamedStruct
	Time  time.Time
	Bytes []byte
}

type TestCustomJsonStruct struct {
	Value    string  `json:"v"`
	EmptyStr string  `json:"emptyStr,omitempty"`
	EmptyInt int     `json:"emptyInt,omitempty"`
	NilPtr   *string `json:"nilPtr,omitempty"`
	ZValue   string  `json:"zv"`
}

func TestEncodeValueType(t *testing.T) {
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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dataItem, err := encodeValue(tc.value)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedDataType, dataItem.dataType)
		})
	}
}

func TestSetAndGet(t *testing.T) {
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
			name:     "CustomStruct",
			setValue: struct{ Name string }{"Test"},
			getValue: new(struct{ Name string }),
		},
		{
			name: "NamedStruct",
			setValue: TestNamedStruct{
				Value: "foo",
				ID:    123,
				Map:   map[string]TestNamedStruct{"bar": {Value: "bar", ID: 987, Bool: true}},
			},
			getValue: new(TestNamedStruct),
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
}

func TestSetError(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		setValue interface{}
	}{
		{
			name:     "NilValue",
			setValue: nil,
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

func TestGetOverflowError(t *testing.T) {
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

func TestGetInvalidType(t *testing.T) {
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

	for key, value := range testData {
		require.NoError(t, m.Set(key, value))
	}

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

func TestEncodingSize(t *testing.T) {
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
			name: "NamedStruct",
			value: TestNamedStruct{
				Value: "foo",
				ID:    123,
				Map:   map[string]TestNamedStruct{"bar": {Value: "bar", ID: 987, Bool: true}},
			},
			expectedStrSize:     205,
			expectedFileSizeOne: 275,
			expectedFileSizeTwo: 484,
		},
		{
			name: "CustomJsonStructEmpty",
			value: TestCustomJsonStruct{
				Value: "foo",
			},
			expectedStrSize:     19,
			expectedFileSizeOne: 69,
			expectedFileSizeTwo: 148,
		},
		{
			name:                "Map",
			value:               map[string]string{"foo1": "bar1", "foo2": "bar2"},
			expectedStrSize:     29,
			expectedFileSizeOne: 62,
			expectedFileSizeTwo: 118,
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
			expectedStrSize:     205,
			expectedFileSizeOne: 273,
			expectedFileSizeTwo: 540,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpFile, m := makeTestMap(t)
			defer os.Remove(tmpFile)

			key1 := "testKey1-" + tc.name
			require.NoError(t, m.Set(key1, tc.value))

			valueHolder, found := m.data[key1]
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

func TestContainsKey(t *testing.T) {
	t.Parallel()
	tmpFile, m := makeTestMap(t)
	defer os.Remove(tmpFile)

	for _, k := range []string{"foo1", "bar1", "foo2", "bar2"} {
		require.NoError(t, m.Set(k, k))

		assert.True(t, m.ContainsKey(k))
		assert.False(t, m.ContainsKey("-"+k))
	}
}

func TestKeySet(t *testing.T) {
	t.Parallel()
	tmpFile, m := makeTestMap(t)
	defer os.Remove(tmpFile)

	keys := []string{"foo1", "bar1", "foo2", "bar2", "foo3", "bar3", "foo4", "bar4"}
	for _, k := range keys {
		require.NoError(t, m.Set(k, k))
	}

	keySet := m.KeySet()
	assert.Len(t, keySet, len(keys))
	for _, k := range keys {
		assert.Contains(t, keySet, k)
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()
	tmpFile, m := makeTestMap(t)
	defer os.Remove(tmpFile)

	key := "keyDelete"
	value := "testValue"
	require.NoError(t, m.Set(key, value))

	m.Delete(key)

	verifyEmpty(t, m, key)
}

func TestDeleteAll(t *testing.T) {
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
			kvMap := KeyValueCSV{
				data: make(map[string]dataItem),
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
