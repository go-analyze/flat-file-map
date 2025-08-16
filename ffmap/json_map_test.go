package ffmap

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryJsonMap_Size(t *testing.T) {
	t.Parallel()
	m := NewMemoryMap()

	assert.Equal(t, 0, m.Size())

	require.NoError(t, m.Set("key1", "value1"))
	assert.Equal(t, 1, m.Size())

	require.NoError(t, m.Set("key2", "value2"))
	assert.Equal(t, 2, m.Size())

	m.Delete("key1")
	assert.Equal(t, 1, m.Size())

	m.DeleteAll()
	assert.Equal(t, 0, m.Size())
}

func TestMemoryJsonMap_Set(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		key       string
		value     interface{}
		expectErr bool
		errMsg    string
	}{
		{name: "ValidString", key: "key1", value: "value1", expectErr: false},
		{name: "ValidInt", key: "key2", value: 42, expectErr: false},
		{name: "ValidStruct", key: "key3", value: TestNamedStruct{Value: "test", ID: 123}, expectErr: false},
		{name: "NilValue", key: "key4", value: nil, expectErr: true, errMsg: "cannot encode nil value"},
		{name: "NilPointer", key: "key5", value: (*string)(nil), expectErr: true, errMsg: "cannot encode nil pointer"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMemoryMap()
			err := m.Set(tt.key, tt.value)

			if tt.expectErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestMemoryJsonMap_Get(t *testing.T) {
	t.Parallel()

	t.Run("ExistingKey", func(t *testing.T) {
		m := NewMemoryMap()
		require.NoError(t, m.Set("key1", "value1"))

		var result string
		found, err := m.Get("key1", &result)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "value1", result)
	})

	t.Run("NonExistentKey", func(t *testing.T) {
		m := NewMemoryMap()

		var result string
		found, err := m.Get("nonexistent", &result)
		require.NoError(t, err)
		assert.False(t, found)
		assert.Equal(t, "", result) // should remain unchanged
	})

	t.Run("TypeMismatch", func(t *testing.T) {
		m := NewMemoryMap()
		require.NoError(t, m.Set("key", "string_value"))

		var intResult int
		found, err := m.Get("key", &intResult)
		assert.False(t, found)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected")
		assert.Contains(t, err.Error(), "type but got")
	})

	t.Run("Overflow", func(t *testing.T) {
		m := NewMemoryMap()

		tests := []struct {
			name     string
			setValue interface{}
			getPtr   interface{}
			errMsg   string
		}{
			{"Int8Overflow", int64(math.MaxInt8 + 1), new(int8), "int8 overflow"},
			{"Uint32Overflow", uint64(math.MaxUint32 + 1), new(uint32), "uint32 overflow"},
			{"Uint16Overflow", uint64(math.MaxUint16 + 1), new(uint16), "uint16 overflow"},
			{"Uint8Overflow", uint64(math.MaxUint8 + 1), new(uint8), "uint8 overflow"},
			{"Float32Overflow", math.MaxFloat64, new(float32), "float32 overflow"},
			{"Complex64Overflow", complex128(complex(math.MaxFloat64, math.MaxFloat64)), new(complex64), "complex"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				require.NoError(t, m.Set("overflow_key", tt.setValue))
				found, err := m.Get("overflow_key", tt.getPtr)
				assert.False(t, found)
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			})
		}
	})
}

func TestMemoryJsonMap_ContainsKey(t *testing.T) {
	t.Parallel()
	m := NewMemoryMap()

	assert.False(t, m.ContainsKey("key1"))

	require.NoError(t, m.Set("key1", "value1"))
	assert.True(t, m.ContainsKey("key1"))

	m.Delete("key1")
	assert.False(t, m.ContainsKey("key1"))
}

func TestMemoryJsonMap_KeySet(t *testing.T) {
	t.Parallel()
	m := NewMemoryMap()

	assert.Empty(t, m.KeySet())

	require.NoError(t, m.Set("key2", "value2"))
	require.NoError(t, m.Set("key1", "value1"))
	require.NoError(t, m.Set("key3", "value3"))

	keys := m.KeySet()
	assert.Len(t, keys, 3)
	assert.Contains(t, keys, "key1")
	assert.Contains(t, keys, "key2")
	assert.Contains(t, keys, "key3")
}

func TestMemoryJsonMap_Delete(t *testing.T) {
	t.Parallel()
	m := NewMemoryMap()

	require.NoError(t, m.Set("key1", "value1"))
	assert.True(t, m.ContainsKey("key1"))

	m.Delete("key1")
	assert.False(t, m.ContainsKey("key1"))

	// Delete non-existent key should be safe
	m.Delete("nonexistent")
}

func TestMemoryJsonMap_DeleteAll(t *testing.T) {
	t.Parallel()
	m := NewMemoryMap()

	require.NoError(t, m.Set("key1", "value1"))
	require.NoError(t, m.Set("key2", "value2"))
	require.NoError(t, m.Set("key3", "value3"))
	assert.Equal(t, 3, m.Size())

	m.DeleteAll()
	assert.Equal(t, 0, m.Size())
	assert.Empty(t, m.KeySet())
}

func TestMemoryJsonMap_Commit(t *testing.T) {
	t.Parallel()
	m := NewMemoryMap()

	require.NoError(t, m.Commit())
}

func TestMemoryJsonMap_DataTypes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		value interface{}
		check func(t *testing.T, retrieved interface{})
	}{
		{
			name:  "String",
			value: "test string",
			check: func(t *testing.T, retrieved interface{}) {
				assert.Equal(t, "test string", retrieved)
			},
		},
		{
			name:  "Int",
			value: 42,
			check: func(t *testing.T, retrieved interface{}) {
				assert.Equal(t, 42, retrieved)
			},
		},
		{
			name:  "Int8",
			value: int8(127),
			check: func(t *testing.T, retrieved interface{}) {
				assert.Equal(t, int8(127), retrieved)
			},
		},
		{
			name:  "Int16",
			value: int16(32767),
			check: func(t *testing.T, retrieved interface{}) {
				assert.Equal(t, int16(32767), retrieved)
			},
		},
		{
			name:  "Int32",
			value: int32(2147483647),
			check: func(t *testing.T, retrieved interface{}) {
				assert.Equal(t, int32(2147483647), retrieved)
			},
		},
		{
			name:  "Int64",
			value: int64(9223372036854775807),
			check: func(t *testing.T, retrieved interface{}) {
				assert.Equal(t, int64(9223372036854775807), retrieved)
			},
		},
		{
			name:  "Uint",
			value: uint(42),
			check: func(t *testing.T, retrieved interface{}) {
				assert.Equal(t, uint(42), retrieved)
			},
		},
		{
			name:  "Uint8",
			value: uint8(255),
			check: func(t *testing.T, retrieved interface{}) {
				assert.Equal(t, uint8(255), retrieved)
			},
		},
		{
			name:  "Uint16",
			value: uint16(65535),
			check: func(t *testing.T, retrieved interface{}) {
				assert.Equal(t, uint16(65535), retrieved)
			},
		},
		{
			name:  "Uint32",
			value: uint32(4294967295),
			check: func(t *testing.T, retrieved interface{}) {
				assert.Equal(t, uint32(4294967295), retrieved)
			},
		},
		{
			name:  "Uint64",
			value: uint64(18446744073709551615),
			check: func(t *testing.T, retrieved interface{}) {
				assert.Equal(t, uint64(18446744073709551615), retrieved)
			},
		},
		{
			name:  "Float32",
			value: float32(3.14159),
			check: func(t *testing.T, retrieved interface{}) {
				assert.InDelta(t, float32(3.14159), retrieved, 0.0001)
			},
		},
		{
			name:  "Float64",
			value: 3.141592653589793,
			check: func(t *testing.T, retrieved interface{}) {
				assert.InDelta(t, 3.141592653589793, retrieved, 1e-8)
			},
		},
		{
			name:  "Complex64",
			value: complex64(3.14 + 2.71i),
			check: func(t *testing.T, retrieved interface{}) {
				assert.Equal(t, complex64(3.14+2.71i), retrieved, 1e-8)
			},
		},
		{
			name:  "Complex128",
			value: complex128(3.141592653589793 + 2.718281828459045i),
			check: func(t *testing.T, retrieved interface{}) {
				assert.Equal(t, complex128(3.141592653589793+2.718281828459045i), retrieved)
			},
		},
		{
			name:  "Bool_True",
			value: true,
			check: func(t *testing.T, retrieved interface{}) {
				assert.Equal(t, true, retrieved)
			},
		},
		{
			name:  "Bool_False",
			value: false,
			check: func(t *testing.T, retrieved interface{}) {
				assert.Equal(t, false, retrieved)
			},
		},
		{
			name:  "IntSlice",
			value: []int{1, 2, 3, 4, 5},
			check: func(t *testing.T, retrieved interface{}) {
				assert.Equal(t, []int{1, 2, 3, 4, 5}, retrieved)
			},
		},
		{
			name:  "StringSlice",
			value: []string{"a", "b", "c"},
			check: func(t *testing.T, retrieved interface{}) {
				assert.Equal(t, []string{"a", "b", "c"}, retrieved)
			},
		},
		{
			name:  "ByteSlice",
			value: []byte{1, 2, 3, 4, 5},
			check: func(t *testing.T, retrieved interface{}) {
				assert.Equal(t, []byte{1, 2, 3, 4, 5}, retrieved)
			},
		},
		{
			name:  "StringMap",
			value: map[string]string{"key1": "value1", "key2": "value2"},
			check: func(t *testing.T, retrieved interface{}) {
				expected := map[string]string{"key1": "value1", "key2": "value2"}
				assert.Equal(t, expected, retrieved)
			},
		},
		{
			name:  "IntMap",
			value: map[string]int{"key1": 1, "key2": 2},
			check: func(t *testing.T, retrieved interface{}) {
				expected := map[string]int{"key1": 1, "key2": 2}
				assert.Equal(t, expected, retrieved)
			},
		},
		{
			name:  "Struct",
			value: TestNamedStruct{Value: "test", ID: 123, Float: 3.14, Bool: true},
			check: func(t *testing.T, retrieved interface{}) {
				expected := TestNamedStruct{Value: "test", ID: 123, Float: 3.14, Bool: true}
				assert.Equal(t, expected, retrieved)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m := NewMemoryMap()

			// Test storing and retrieving the value
			key := "test_key"
			require.NoError(t, m.Set(key, tc.value))

			// Create a variable of the same type to retrieve into
			retrievePtr := reflect.New(reflect.TypeOf(tc.value)).Interface()
			found, err := m.Get(key, retrievePtr)
			require.NoError(t, err)
			assert.True(t, found)

			retrieved := reflect.ValueOf(retrievePtr).Elem().Interface()
			tc.check(t, retrieved)
		})
	}
}

func TestMemoryJsonMap_ComplexStruct(t *testing.T) {
	t.Parallel()
	m := NewMemoryMap()

	original := TestNamedStruct{
		Value:     "test",
		ID:        42,
		Float:     3.14159,
		Bool:      true,
		Map:       map[string]TestNamedStruct{"nested": {Value: "nested_val", ID: 99}},
		MapIntKey: map[int]string{1: "one", 2: "two"},
		Time:      time.Date(2023, 1, 15, 12, 30, 45, 0, time.UTC),
		Bytes:     []byte{1, 2, 3, 4, 5},
		IntSlice:  []int{10, 20, 30},
	}

	require.NoError(t, m.Set("complex", original))

	var retrieved TestNamedStruct
	found, err := m.Get("complex", &retrieved)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, original, retrieved)
}

func TestMemoryJsonMap_PointerStruct(t *testing.T) {
	t.Parallel()
	m := NewMemoryMap()

	timeVal := time.Date(2023, 1, 15, 12, 30, 45, 0, time.UTC)
	intVal := 42
	strVal := "test"
	boolVal := true
	floatVal := 3.14

	original := TestPointerStruct{
		T: &timeVal,
		I: &intVal,
		S: &strVal,
		B: &boolVal,
		F: &floatVal,
	}

	require.NoError(t, m.Set("pointers", original))

	var retrieved TestPointerStruct
	found, err := m.Get("pointers", &retrieved)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, original, retrieved)
}

func TestMemoryJsonMap_EmbeddedStruct(t *testing.T) {
	t.Parallel()
	m := NewMemoryMap()

	original := TestStructEmbedded{
		TestNamedStruct: TestNamedStruct{Value: "embedded", ID: 123},
		DirectStr:       "direct",
	}

	require.NoError(t, m.Set("embedded", original))

	var retrieved TestStructEmbedded
	found, err := m.Get("embedded", &retrieved)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, original, retrieved)
}

func TestMemoryJsonMap_NestedStruct(t *testing.T) {
	t.Parallel()
	m := NewMemoryMap()

	ptrStruct := &TestNamedStruct{Value: "ptr_nested", ID: 456}
	original := TestNestedStruct{
		Inner: TestNamedStruct{Value: "inner", ID: 123},
		Ptr:   ptrStruct,
	}

	require.NoError(t, m.Set("nested", original))

	var retrieved TestNestedStruct
	found, err := m.Get("nested", &retrieved)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, original, retrieved)
}

func TestMemoryJsonMap_ZeroValueStruct(t *testing.T) {
	t.Parallel()
	m := NewMemoryMap()

	original := TestNamedStruct{} // all zero values

	require.NoError(t, m.Set("zero", original))

	var retrieved TestNamedStruct
	found, err := m.Get("zero", &retrieved)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, original, retrieved)
}

func TestMemoryJsonMap_EmptySlice(t *testing.T) {
	t.Parallel()
	m := NewMemoryMap()

	original := []int{}
	require.NoError(t, m.Set("empty_slice", original))

	var retrieved []int
	found, err := m.Get("empty_slice", &retrieved)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, original, retrieved)
}

func TestMemoryJsonMap_NilSlice(t *testing.T) {
	t.Parallel()
	m := NewMemoryMap()

	var original []int // nil slice
	require.NoError(t, m.Set("nil_slice", original))

	var retrieved []int
	found, err := m.Get("nil_slice", &retrieved)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Nil(t, retrieved)
}

func TestMemoryJsonMap_ConcurrentReadWrite(t *testing.T) {
	t.Parallel()
	m := NewMemoryMap()

	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2) // readers and writers
	errChan := make(chan error, numGoroutines*numOperations)

	// Writers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key_%d_%d", id, j)
				value := fmt.Sprintf("value_%d_%d", id, j)
				if err := m.Set(key, value); err != nil {
					errChan <- err
				}
			}
		}(i)
	}

	// Readers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key_%d_%d", id, j)
				var result string
				// Don't assert on found since timing with writers is unpredictable
				_, _ = m.Get(key, &result)
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	// Check for any errors from writers
	for err := range errChan {
		t.Errorf("Unexpected error during concurrent write: %v", err)
	}

	// Final size should be predictable
	assert.Equal(t, numGoroutines*numOperations, m.Size())
}

func TestMemoryJsonMap_ConcurrentDelete(t *testing.T) {
	t.Parallel()
	m := NewMemoryMap()

	numKeys := 100
	for i := 0; i < numKeys; i++ {
		require.NoError(t, m.Set(fmt.Sprintf("key_%d", i), fmt.Sprintf("value_%d", i)))
	}

	var wg sync.WaitGroup
	const numGoroutines = 10
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			start := id * (numKeys / numGoroutines)
			end := start + (numKeys / numGoroutines)
			for j := start; j < end; j++ {
				m.Delete(fmt.Sprintf("key_%d", j))
			}
		}(i)
	}

	wg.Wait()
	assert.Equal(t, 0, m.Size())
}

func TestMemoryJsonMap_ReplaceValue(t *testing.T) {
	t.Parallel()
	m := NewMemoryMap()

	// Set initial value
	require.NoError(t, m.Set("key", "initial"))

	var result string
	found, err := m.Get("key", &result)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "initial", result)

	// Replace with new value
	require.NoError(t, m.Set("key", "replaced"))

	found, err = m.Get("key", &result)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "replaced", result)

	assert.Equal(t, 1, m.Size()) // Size should remain 1
}

func TestMemoryJsonMap_LargeDataSet(t *testing.T) {
	t.Parallel()
	m := NewMemoryMap()

	numItems := 1000
	for i := 0; i < numItems; i++ {
		key := fmt.Sprintf("key_%05d", i)
		value := TestNamedStruct{
			Value:    fmt.Sprintf("value_%d", i),
			ID:       i,
			Float:    float64(i) * 3.14,
			Bool:     i%2 == 0,
			IntSlice: []int{i, i + 1, i + 2},
		}
		require.NoError(t, m.Set(key, value))
	}

	assert.Equal(t, numItems, m.Size())

	// Verify random samples
	for i := 0; i < 10; i++ {
		idx := i * (numItems / 10)
		key := fmt.Sprintf("key_%05d", idx)
		var result TestNamedStruct
		found, err := m.Get(key, &result)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, fmt.Sprintf("value_%d", idx), result.Value)
		assert.Equal(t, idx, result.ID)
	}
}

func TestMemoryJsonMap_SetItemMap(t *testing.T) {
	t.Parallel()
	m := &memoryJsonMap{
		data: make(map[string]dataItem),
	}

	// Test setItemMap function (used by CSV batch operations)
	items := make(map[string]*dataItem)
	items["key1"] = &dataItem{dataType: dataString, value: "value1"}
	items["key2"] = &dataItem{dataType: dataInt, value: "42"}

	m.setItemMap(items)

	// Verify items were set correctly
	var str string
	found, err := m.Get("key1", &str)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value1", str)

	var intVal int
	found, err = m.Get("key2", &intVal)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, 42, intVal)

	// Test with empty map (should be no-op)
	emptyItems := make(map[string]*dataItem)
	initialSize := m.Size()
	m.setItemMap(emptyItems)
	assert.Equal(t, initialSize, m.Size())
}

func TestMemoryJsonMap_DecodeValue(t *testing.T) {
	t.Run("InvalidBoolEncoding", func(t *testing.T) {
		t.Parallel()
		m := &memoryJsonMap{
			data: make(map[string]dataItem),
		}

		// Test invalid bool encoding (direct data manipulation to test decode error path)
		m.rwLock.Lock()
		m.data["invalid_bool"] = dataItem{dataType: dataBool, value: "invalid"}
		m.rwLock.Unlock()

		var boolResult bool
		found, err := m.Get("invalid_bool", &boolResult)
		assert.False(t, found)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected encoded bool value")
	})
}

func TestMemoryJsonMap_EncodeValue(t *testing.T) {
	t.Parallel()
	m := NewMemoryMap()

	t.Run("Array", func(t *testing.T) {
		// Test array (not slice)
		arr := [3]int{1, 2, 3}
		require.NoError(t, m.Set("array", arr))

		var retrieved [3]int
		found, err := m.Get("array", &retrieved)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, arr, retrieved)
	})

	t.Run("EmptyArray", func(t *testing.T) {
		// Test empty array
		emptyArr := [0]int{}
		require.NoError(t, m.Set("empty_array", emptyArr))

		var retrievedEmpty [0]int
		found, err := m.Get("empty_array", &retrievedEmpty)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, emptyArr, retrievedEmpty)
	})
}

func TestMemoryJsonMap_StripZeroFields(t *testing.T) {
	t.Parallel()
	m := NewMemoryMap()

	t.Run("NilVsEmptySlice", func(t *testing.T) {
		// Test with nil slice vs empty slice
		type SliceStruct struct {
			NilSlice   []string
			EmptySlice []string
		}

		original := SliceStruct{
			NilSlice:   nil,
			EmptySlice: []string{},
		}

		require.NoError(t, m.Set("slice_struct", original))

		var retrieved SliceStruct
		found, err := m.Get("slice_struct", &retrieved)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Nil(t, retrieved.NilSlice)
		assert.NotNil(t, retrieved.EmptySlice)
		assert.Empty(t, retrieved.EmptySlice)
	})

	t.Run("NilVsEmptyMap", func(t *testing.T) {
		// Test with nil map vs empty map
		type MapStruct struct {
			NilMap   map[string]int
			EmptyMap map[string]int
		}

		mapOriginal := MapStruct{
			NilMap:   nil,
			EmptyMap: make(map[string]int),
		}

		require.NoError(t, m.Set("map_struct", mapOriginal))

		var mapRetrieved MapStruct
		found, err := m.Get("map_struct", &mapRetrieved)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Nil(t, mapRetrieved.NilMap)
		assert.NotNil(t, mapRetrieved.EmptyMap)
		assert.Empty(t, mapRetrieved.EmptyMap)
	})
}

func TestMemoryJsonMap_ErrorTypes(t *testing.T) {
	t.Run("EncodingError_NilValue", func(t *testing.T) {
		t.Parallel()
		m := NewMemoryMap()

		err := m.Set("test_key", nil)
		require.Error(t, err)

		var encodingErr *EncodingError
		require.ErrorAs(t, err, &encodingErr)
		assert.Equal(t, "test_key", encodingErr.Key)
		assert.Contains(t, encodingErr.Message, "cannot encode nil value")
		assert.Contains(t, encodingErr.Error(), "test_key")
	})

	t.Run("EncodingError_NilPointer", func(t *testing.T) {
		t.Parallel()
		m := NewMemoryMap()

		var nilPtr *string
		err := m.Set("nil_ptr_key", nilPtr)
		require.Error(t, err)

		var encodingErr *EncodingError
		require.ErrorAs(t, err, &encodingErr)
		assert.Equal(t, "nil_ptr_key", encodingErr.Key)
		assert.Contains(t, encodingErr.Message, "cannot encode nil pointer")
	})

	t.Run("TypeMismatchError_WrongType", func(t *testing.T) {
		t.Parallel()
		m := NewMemoryMap()

		require.NoError(t, m.Set("string_key", "string_value"))

		var intResult int
		found, err := m.Get("string_key", &intResult)
		assert.False(t, found)
		require.Error(t, err)

		var typeMismatchErr *TypeMismatchError
		require.ErrorAs(t, err, &typeMismatchErr)
		assert.Equal(t, "string_key", typeMismatchErr.Key)
		assert.Contains(t, typeMismatchErr.Message, "expected")
		assert.Contains(t, typeMismatchErr.Error(), "string_key")
	})

	t.Run("TypeMismatchError_Overflow", func(t *testing.T) {
		t.Parallel()
		m := NewMemoryMap()

		require.NoError(t, m.Set("big_int", int64(math.MaxInt8+1)))

		var int8Result int8
		found, err := m.Get("big_int", &int8Result)
		assert.False(t, found)
		require.Error(t, err)

		var typeMismatchErr *TypeMismatchError
		require.ErrorAs(t, err, &typeMismatchErr)
		assert.Equal(t, "big_int", typeMismatchErr.Key)
		assert.Contains(t, typeMismatchErr.Message, "overflow")
	})

	t.Run("ValidationError_InvalidBoolValue", func(t *testing.T) {
		t.Parallel()
		m := &memoryJsonMap{
			data: make(map[string]dataItem),
		}

		// Directly manipulate internal data to test validation error
		m.rwLock.Lock()
		m.data["invalid_bool"] = dataItem{dataType: dataBool, value: "invalid"}
		m.rwLock.Unlock()

		var boolResult bool
		found, err := m.Get("invalid_bool", &boolResult)
		assert.False(t, found)
		require.Error(t, err)

		var validationErr *ValidationError
		require.ErrorAs(t, err, &validationErr)
		assert.Contains(t, validationErr.Message, "unexpected encoded bool value")
	})

	t.Run("ErrorTypes_DistinctChecking", func(t *testing.T) {
		t.Parallel()
		m := NewMemoryMap()

		// Test that errors.As works correctly for checking error types
		nilErr := m.Set("key1", nil)
		require.NoError(t, m.Set("key2", "string"))

		var intVal int
		_, typeMismatchErr := m.Get("key2", &intVal)

		// Check encoding error type
		var encodingErr *EncodingError
		var typeMismatchErr1 *TypeMismatchError
		var validationErr1 *ValidationError

		require.ErrorAs(t, nilErr, &encodingErr)
		assert.False(t, errors.As(nilErr, &typeMismatchErr1))
		assert.False(t, errors.As(nilErr, &validationErr1))

		// Check type mismatch error type
		var encodingErr2 *EncodingError
		var typeMismatchErr2 *TypeMismatchError
		var validationErr2 *ValidationError

		require.ErrorAs(t, typeMismatchErr, &typeMismatchErr2)
		assert.False(t, errors.As(typeMismatchErr, &encodingErr2))
		assert.False(t, errors.As(typeMismatchErr, &validationErr2))
	})
}
