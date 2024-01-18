package ffmap

import (
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func makeTestMap(t *testing.T) (string, *KeyValueCSV) {
	tmpfile, err := os.CreateTemp("", "testm.*.csv")
	require.NoError(t, err)
	m, err := OpenCSV(tmpfile.Name())
	require.NoError(t, err)
	return tmpfile.Name(), m
}

func TestOpenAndCommit(t *testing.T) {
	t.Run("OpenEmpty", func(t *testing.T) {
		t.Parallel()
		tmpFile, m := makeTestMap(t)
		defer os.Remove(tmpFile)

		require.Equal(t, 0, m.Size())
	})
	t.Run("OpenMissing", func(t *testing.T) {
		t.Parallel()
		tmpfile, err := os.CreateTemp("", "testm.*.csv")
		require.NoError(t, err)
		require.NoError(t, os.Remove(tmpfile.Name()))
		m, err := OpenCSV(tmpfile.Name())
		require.NoError(t, err)

		require.Equal(t, 0, m.Size())
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
		mOrig.Commit()

		mNew, err := OpenCSV(tmpFile)
		require.NoError(t, err)

		for key, expectedValue := range values {
			var actualValue string
			found, err := mNew.Get(key, &actualValue)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, expectedValue, actualValue)
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
		mOrig.Commit()

		mNew, err := OpenCSV(tmpFile)
		require.NoError(t, err)

		for key, expectedValue := range values {
			var actualValue TestNamedStruct
			found, err := mNew.Get(key, &actualValue)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, expectedValue, actualValue)
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
		require.Equal(t, string(file1Content), string(file2Content))
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
		require.Equal(t, string(file1Content), string(file2Content))
	})
	t.Run("SaveAndLoadAllTypes", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)
		defer os.Remove(tmpFile)

		testData := map[string]interface{}{
			"string":           "foo",
			"bool":             true,
			"float32":          float32(3.14),
			"float64":          3.1415,
			"int":              42,
			"int8":             int8(8),
			"int16":            int16(16),
			"int32":            int32(32),
			"int64":            int64(64),
			"uint":             uint(1),
			"uint8":            uint8(8),
			"uint16":           uint16(16),
			"uint32":           uint32(32),
			"uint64":           uint64(64),
			"complex64":        complex64(complex(5, 6)),
			"complex128":       complex(5, 6),
			"intSlice":         []int{1, 2, 3, 4},
			"int64Slice":       []int64{1000, 2000, 3000, 4000},
			"stringSlice":      []string{"a", "b", "c"},
			"byteSlice":        []byte{0x01, 0x02, 0x03, 0x04},
			"namedStructSlice": []TestNamedStruct{{Value: "foo", ID: 1}, {Value: "bar", ID: 2}},
			"mapStringString":  map[string]string{"key1": "value1", "key2": "value2"},
		}

		for key, value := range testData {
			require.NoError(t, mOrig.Set(key, value))
		}
		mOrig.Commit()

		mNew, err := OpenCSV(tmpFile)
		require.NoError(t, err)

		for key, expectedValue := range testData {
			valPtr := reflect.New(reflect.TypeOf(expectedValue))
			found, err := mNew.Get(key, valPtr.Interface())
			require.NoError(t, err)
			require.True(t, found)

			actualValue := valPtr.Elem().Interface()
			require.Equal(t, expectedValue, actualValue)
		}
	})
}

func TestSize(t *testing.T) {
	t.Parallel()
	tmpFile, m := makeTestMap(t)
	defer os.Remove(tmpFile)

	for i := 0; i < 10; i++ {
		require.NoError(t, m.Set(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)))
		require.Equal(t, i+1, m.Size())
	}
}

type TestNamedStruct struct {
	Value string
	ID    int
	Float float64
	Bool  bool
	Map   map[string]TestNamedStruct
}

func TestSetAndGet(t *testing.T) {
	t.Parallel()
	typesToTest := []struct {
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
			name:     "Map",
			setValue: map[string]string{"foo1": "bar1", "foo2": "bar2"},
			getValue: new(map[string]string),
		},
		{
			name:     "ByteSlice",
			setValue: []byte{1, 2, 3, 4},
			getValue: new([]byte),
		},
		{
			name:     "IntSlice",
			setValue: []int{1, 2, 3, 4},
			getValue: new([]int),
		},
		{
			name:     "Int64Slice",
			setValue: []int64{1000, 2000, 3000, 4000},
			getValue: new([]int64),
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
	}

	for _, tc := range typesToTest {
		t.Run(tc.name, func(t *testing.T) {
			tmpFile, m := makeTestMap(t)
			defer os.Remove(tmpFile)

			key := "testKey-" + tc.name
			require.NoError(t, m.Set(key, tc.setValue))

			found, err := m.Get(key, tc.getValue)
			require.NoError(t, err)
			require.True(t, found)

			require.Equal(t, reflect.ValueOf(tc.setValue).Interface(), reflect.ValueOf(tc.getValue).Elem().Interface())
			require.True(t, reflect.DeepEqual(reflect.ValueOf(tc.setValue).Interface(), reflect.ValueOf(tc.getValue).Elem().Interface()))
		})
	}
}

func TestGetOverflowError(t *testing.T) {
	t.Parallel()
	typesToTest := []struct {
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

	for _, tc := range typesToTest {
		t.Run(tc.name, func(t *testing.T) {
			tmpFile, m := makeTestMap(t)
			defer os.Remove(tmpFile)

			key := "testKey-" + tc.name
			require.NoError(t, m.Set(key, tc.setValue))

			found, err := m.Get(key, tc.getValue)
			require.Error(t, err)
			require.False(t, found)
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

	for key, _ := range testData {
		for mismatchKey, mismatchValue := range testData {
			if key == mismatchKey {
				continue
			}

			valPtr := reflect.New(reflect.TypeOf(mismatchValue))
			found, err := m.Get(key, valPtr.Interface())
			require.Errorf(t, err, "error expected looking up %s with type %s", key, mismatchKey)
			require.False(t, found)
		}
	}
}

func TestEncodingSize(t *testing.T) {
	t.Parallel()
	typesToTest := []struct {
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
			expectedFileSizeTwo: 92,
		},
		{
			name: "NamedStruct",
			value: TestNamedStruct{
				Value: "foo",
				ID:    123,
				Map:   map[string]TestNamedStruct{"bar": {Value: "bar", ID: 987, Bool: true}},
			},
			expectedStrSize:     119,
			expectedFileSizeOne: 177,
			expectedFileSizeTwo: 299,
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
			expectedStrSize:     119,
			expectedFileSizeOne: 175,
			expectedFileSizeTwo: 344,
		},
	}

	for _, tc := range typesToTest {
		t.Run(tc.name, func(t *testing.T) {
			tmpFile, m := makeTestMap(t)
			defer os.Remove(tmpFile)

			key1 := "testKey1-" + tc.name
			require.NoError(t, m.Set(key1, tc.value))

			valueHolder, found := m.data[key1]
			require.True(t, found)
			value := valueHolder.value
			require.Equalf(t, tc.expectedStrSize, len(value), "unexpected encoded value: %s", value)

			m.Commit()
			verifyFileSize(t, tmpFile, tc.expectedFileSizeOne)

			require.NoError(t, m.Set("testKey2-"+tc.name, tc.value))
			m.Commit()
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
		require.Equal(t, expectedSize, fileInfo.Size(),
			"Unexpected file size. File contents: \n%s", string(fileContents))
	}
}

func TestContainsKey(t *testing.T) {
	t.Parallel()
	tmpFile, m := makeTestMap(t)
	defer os.Remove(tmpFile)

	for _, k := range []string{"foo1", "bar1", "foo2", "bar2"} {
		require.NoError(t, m.Set(k, k))

		require.True(t, m.ContainsKey(k))
		require.False(t, m.ContainsKey("-"+k))
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
	require.Len(t, keySet, len(keys))
	for _, k := range keys {
		require.Contains(t, keySet, k)
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()
	tmpFile, m := makeTestMap(t)
	defer os.Remove(tmpFile)

	key := "testKey"
	value := "testValue"
	require.NoError(t, m.Set(key, value))

	m.Delete(key)

	var result string
	found, err := m.Get(key, &result)
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, 0, m.Size())
	require.Len(t, m.KeySet(), 0)
}
