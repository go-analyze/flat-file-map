package ffmap

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type memoryFFMap struct {
	data map[string]interface{}
}

func newMemoryFFMap() *memoryFFMap {
	return &memoryFFMap{
		data: make(map[string]interface{}),
	}
}

func (m *memoryFFMap) Size() int {
	return len(m.data)
}

func (m *memoryFFMap) Get(key string, value interface{}) (bool, error) {
	storedValue, exists := m.data[key]
	if !exists {
		return false, nil
	}

	valType := reflect.TypeOf(value)
	if reflect.TypeOf(storedValue) != valType.Elem() {
		return false, fmt.Errorf("type mismatch: expected %v, got %v", valType.Elem(), reflect.TypeOf(storedValue))
	}

	reflect.ValueOf(value).Elem().Set(reflect.ValueOf(storedValue))
	return true, nil
}

func (m *memoryFFMap) ContainsKey(key string) bool {
	_, exists := m.data[key]
	return exists
}

func (m *memoryFFMap) KeySet() []string {
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys
}

func (m *memoryFFMap) Set(key string, value interface{}) error {
	m.data[key] = value
	return nil
}

func (m *memoryFFMap) Delete(key string) {
	delete(m.data, key)
}

func (m *memoryFFMap) DeleteAll() {
	m.data = make(map[string]interface{})
}

func (m *memoryFFMap) Commit() error {
	return nil
}

func TestTypedFFMap(t *testing.T) {
	t.Run("Size", func(t *testing.T) {
		t.Parallel()
		tfm := NewTypedFFMap[string](newMemoryFFMap())

		assert.Equal(t, 0, tfm.Size())

		err := tfm.Set("keySize", "value")
		require.NoError(t, err)

		assert.Equal(t, 1, tfm.Size())
	})
	t.Run("GetMissing", func(t *testing.T) {
		t.Parallel()
		tfm := NewTypedFFMap[string](newMemoryFFMap())

		_, ok := tfm.Get("foo")
		assert.False(t, ok)
	})
	t.Run("SetAndGet", func(t *testing.T) {
		t.Parallel()
		tfm := NewTypedFFMap[string](newMemoryFFMap())

		key := "keySetAndGet"
		value := "value"
		err := tfm.Set(key, value)
		require.NoError(t, err)

		result, ok := tfm.Get(key)
		assert.True(t, ok)
		require.NoError(t, err)
		assert.Equal(t, value, result)
	})
	t.Run("ContainsKey", func(t *testing.T) {
		t.Parallel()
		tfm := NewTypedFFMap[string](newMemoryFFMap())
		key := "ContainsKey"

		assert.False(t, tfm.ContainsKey(key))

		err := tfm.Set(key, "value")
		require.NoError(t, err)

		assert.True(t, tfm.ContainsKey(key))
	})
	t.Run("KeySet", func(t *testing.T) {
		t.Parallel()
		tfm := NewTypedFFMap[string](newMemoryFFMap())
		key := "key"

		assert.Empty(t, tfm.KeySet())

		err := tfm.Set(key, "value")
		require.NoError(t, err)
		keyset := tfm.KeySet()

		require.Len(t, keyset, 1)
		assert.Equal(t, key, keyset[0])
	})
	t.Run("Delete", func(t *testing.T) {
		t.Parallel()
		tfm := NewTypedFFMap[string](newMemoryFFMap())

		key := "keyDelete"
		err := tfm.Set(key, "value")
		require.NoError(t, err)
		tfm.Delete(key)

		assert.False(t, tfm.ContainsKey(key))
		assert.Equal(t, 0, tfm.Size())
	})
	t.Run("DeleteAll", func(t *testing.T) {
		t.Parallel()
		tfm := NewTypedFFMap[string](newMemoryFFMap())

		key := "keyDeleteAll"
		require.NoError(t, tfm.Set(key, "value"))
		require.NoError(t, tfm.Set("foo"+key, "value"))
		tfm.DeleteAll()

		assert.False(t, tfm.ContainsKey(key))
		assert.Equal(t, 0, tfm.Size())
	})
}
