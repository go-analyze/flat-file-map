package ffmap

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypedFFMap(t *testing.T) {
	t.Run("Size", func(t *testing.T) {
		t.Parallel()
		tfm := NewTypedFFMap[string](NewMemoryMap())

		assert.Equal(t, 0, tfm.Size())

		err := tfm.Set("keySize", "value")
		require.NoError(t, err)

		assert.Equal(t, 1, tfm.Size())
	})
	t.Run("GetMissing", func(t *testing.T) {
		t.Parallel()
		tfm := NewTypedFFMap[string](NewMemoryMap())

		_, ok := tfm.Get("foo")
		assert.False(t, ok)
	})
	t.Run("SetAndGet", func(t *testing.T) {
		t.Parallel()
		tfm := NewTypedFFMap[string](NewMemoryMap())

		key := "keySetAndGet"
		value := "value"
		err := tfm.Set(key, value)
		require.NoError(t, err)

		result, ok := tfm.Get(key)
		assert.True(t, ok)
		require.NoError(t, err)
		assert.Equal(t, value, result)
	})
	t.Run("SetMapValues", func(t *testing.T) {
		t.Parallel()
		tfm := NewTypedFFMap[string](NewMemoryMap())

		values := map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
			"d": "4",
		}

		require.NoError(t, tfm.SetMapValues(values))
		require.Equal(t, 4, tfm.Size())
		for k, v1 := range values {
			v2, ok := tfm.Get(k)
			assert.True(t, ok)
			assert.Equal(t, v1, v2)
		}
	})
	t.Run("SetSliceValues", func(t *testing.T) {
		t.Parallel()
		tfm := NewTypedFFMap[int](NewMemoryMap())

		values := []int{0, 1, 2, 3, 4}

		require.NoError(t, tfm.SetSliceValues(values, strconv.Itoa))
		require.Equal(t, 5, tfm.Size())
		for k, v1 := range values {
			v2, ok := tfm.Get(strconv.Itoa(k))
			assert.True(t, ok)
			assert.Equal(t, v1, v2)
		}
	})
	t.Run("ContainsKey", func(t *testing.T) {
		t.Parallel()
		tfm := NewTypedFFMap[string](NewMemoryMap())
		key := "ContainsKey"

		assert.False(t, tfm.ContainsKey(key))

		err := tfm.Set(key, "value")
		require.NoError(t, err)

		assert.True(t, tfm.ContainsKey(key))
	})
	t.Run("KeySet", func(t *testing.T) {
		t.Parallel()
		tfm := NewTypedFFMap[string](NewMemoryMap())
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
		tfm := NewTypedFFMap[string](NewMemoryMap())

		key := "keyDelete"
		err := tfm.Set(key, "value")
		require.NoError(t, err)
		tfm.Delete(key)

		assert.False(t, tfm.ContainsKey(key))
		assert.Equal(t, 0, tfm.Size())
	})
	t.Run("DeleteAll", func(t *testing.T) {
		t.Parallel()
		tfm := NewTypedFFMap[string](NewMemoryMap())

		key := "keyDeleteAll"
		require.NoError(t, tfm.Set(key, "value"))
		require.NoError(t, tfm.Set("foo"+key, "value"))
		tfm.DeleteAll()

		assert.False(t, tfm.ContainsKey(key))
		assert.Equal(t, 0, tfm.Size())
	})
}
