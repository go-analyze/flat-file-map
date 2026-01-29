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
	t.Run("All", func(t *testing.T) {
		t.Parallel()

		t.Run("empty_map", func(t *testing.T) {
			tfm := NewTypedFFMap[string](NewMemoryMap())

			var count int
			for range tfm.All() {
				count++
			}
			assert.Equal(t, 0, count)
		})
		t.Run("iterates_all_pairs", func(t *testing.T) {
			tfm := NewTypedFFMap[string](NewMemoryMap())

			expected := map[string]string{"a": "1", "b": "2", "c": "3"}
			require.NoError(t, tfm.SetMapValues(expected))

			result := make(map[string]string)
			for k, v := range tfm.All() {
				result[k] = v
			}
			assert.Equal(t, expected, result)
		})
		t.Run("early_termination", func(t *testing.T) {
			tfm := NewTypedFFMap[string](NewMemoryMap())

			require.NoError(t, tfm.SetMapValues(map[string]string{"a": "1", "b": "2", "c": "3"}))

			var count int
			for range tfm.All() {
				count++
				break
			}
			assert.Equal(t, 1, count)
		})
		t.Run("delete_during_iteration", func(t *testing.T) {
			tfm := NewTypedFFMap[int](NewMemoryMap())
			require.NoError(t, tfm.SetMapValues(map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}))

			var count int
			for k := range tfm.All() {
				tfm.Delete(k)
				tfm.Delete("b") // ensure b is not considered as iterated

				if k == "b" {
					if count == 0 {
						continue // first record, just ignore so not counted
					} else {
						assert.Fail(t, "expected b to be deleted")
					}
				}

				count++
			}
			assert.Equal(t, 3, count)
			assert.Equal(t, 0, tfm.Size())
		})
	})
	t.Run("Values", func(t *testing.T) {
		t.Parallel()

		t.Run("empty_map", func(t *testing.T) {
			tfm := NewTypedFFMap[int](NewMemoryMap())

			var count int
			for range tfm.Values() {
				count++
			}
			assert.Equal(t, 0, count)
		})
		t.Run("iterates_all_values", func(t *testing.T) {
			tfm := NewTypedFFMap[int](NewMemoryMap())

			require.NoError(t, tfm.SetMapValues(map[string]int{"a": 1, "b": 2, "c": 3, "d": 1}))

			var sum int
			for v := range tfm.Values() {
				sum += v
			}
			assert.Equal(t, 7, sum)
		})
		t.Run("early_termination", func(t *testing.T) {
			tfm := NewTypedFFMap[int](NewMemoryMap())

			require.NoError(t, tfm.SetMapValues(map[string]int{"a": 1, "b": 2, "c": 3}))

			var count int
			for range tfm.Values() {
				count++
				break
			}
			assert.Equal(t, 1, count)
		})
		t.Run("delete_during_iteration", func(t *testing.T) {
			tfm := NewTypedFFMap[int](NewMemoryMap())
			const skipValue = 2
			require.NoError(t, tfm.SetMapValues(map[string]int{"a": 1, "b": skipValue, "c": skipValue, "d": 4}))

			var count int
			for val := range tfm.Values() {
				if count == 0 {
					// delete records that we don't want to iterate, 2 needed in case we already are on one of them
					tfm.Delete("b")
					tfm.Delete("c")
					count = 2 // record deleted to capture as skipped
					if val == skipValue {
						continue // got skip record to start
					}
				}
				count++
				assert.NotEqual(t, skipValue, val)
			}
			assert.Equal(t, 4, count)
			assert.Equal(t, 2, tfm.Size())
		})
	})
}
