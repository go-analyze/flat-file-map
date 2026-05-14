package ffmap

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:thelper // t can be nil when called from benchmarks
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
		t.Cleanup(func() { _ = os.Remove(tmpfile.Name()) })
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
	t.Run("open_empty", func(t *testing.T) {
		t.Parallel()
		_, m := makeTestMap(t)

		assert.Equal(t, 0, m.Size())
	})

	t.Run("open_missing", func(t *testing.T) {
		t.Parallel()
		tmpfile, err := os.CreateTemp("", "testm.*.csv")
		require.NoError(t, err)
		require.NoError(t, os.Remove(tmpfile.Name()))
		m, err := OpenCSV(tmpfile.Name())
		require.NoError(t, err)

		assert.Equal(t, 0, m.Size())
	})

	t.Run("save_load_strings", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)

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

	t.Run("save_load_named_struct", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)

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

	t.Run("save_load_embedded_struct", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)

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

	t.Run("save_load_nested_struct", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)

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

	t.Run("save_load_deep_nested", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)

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

	t.Run("save_load_any_struct_mixed", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)

		values := map[string]TestAnyStruct{
			"float64": {Value: "float64", Any: 1.1},
			"str":     {Value: "str", Any: "str"},
			"nil":     {Value: "nil", Any: nil},
		}

		require.NoError(t, SetMapValues(m, values))
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

	t.Run("save_load_custom_json", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)

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

	t.Run("save_load_deep_nested_map", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)

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

	t.Run("commit_order_string", func(t *testing.T) {
		t.Parallel()
		tmpFile1, m1 := makeTestMap(t)
		tmpFile2, m2 := makeTestMap(t)

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

	t.Run("commit_order_mixed", func(t *testing.T) {
		t.Parallel()
		tmpFile1, m1 := makeTestMap(t)
		tmpFile2, m2 := makeTestMap(t)

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

	t.Run("save_load_all_types", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)

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

		require.NoError(t, SetMapValues(mOrig, testData))
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

	t.Run("commit_mod_tracking", func(t *testing.T) {
		t.Parallel()
		_, m := makeTestMap(t)

		for i, s := range []string{"foo1", "bar1", "foo2", "bar2", "foo3", "bar3", "foo4", "bar4"} {
			assert.Equal(t, i, m.memoryMap.modCount)
			require.NoError(t, m.Set(s, s))
		}
		assert.Zero(t, m.commitMod)
		require.NoError(t, m.Commit())
		assert.Equal(t, m.memoryMap.modCount, m.commitMod)
	})

	t.Run("commit_no_op", func(t *testing.T) {
		t.Parallel()
		tmpFile, m := makeTestMap(t)

		require.NoError(t, SetSliceValues(m,
			[]string{"foo1", "bar1", "foo2", "bar2", "foo3", "bar3", "foo4", "bar4"}, stringReturnFunc))
		require.NoError(t, m.Commit())
		require.NoError(t, os.Remove(tmpFile)) // remove file as hack to verify commit does not apply

		require.NoError(t, m.Commit()) // no-op commit
		assert.NoFileExists(t, tmpFile)
	})

	t.Run("save_load_zero_struct", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)

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

	t.Run("reload_preserves_single_struct", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)

		// single-instance struct of one type plus multiple-instance of another type
		singleValue := TestNamedStruct{Value: "single", ID: 42, Float: 3.14, Bool: true}
		multi1 := TestStructWithSlice{Values: []TestNamedStruct{{Value: "m1", ID: 1}}}
		multi2 := TestStructWithSlice{Values: []TestNamedStruct{{Value: "m2", ID: 2}}}
		require.NoError(t, mOrig.Set("solo", singleValue))
		require.NoError(t, mOrig.Set("multi1", multi1))
		require.NoError(t, mOrig.Set("multi2", multi2))
		require.NoError(t, mOrig.Commit())

		// reload from disk (loses structId on the single-instance struct)
		reloaded, err := OpenCSV(tmpFile)
		require.NoError(t, err)

		var afterLoad TestNamedStruct
		found, err := reloaded.Get("solo", &afterLoad)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, singleValue, afterLoad)

		// mutate the multi entries only, then commit (no Set on "solo")
		require.NoError(t, reloaded.Set("multi1", TestStructWithSlice{Values: []TestNamedStruct{{Value: "m1b", ID: 10}}}))
		require.NoError(t, reloaded.Set("multi2", TestStructWithSlice{Values: []TestNamedStruct{{Value: "m2b", ID: 20}}}))
		require.NoError(t, reloaded.Commit())

		// reload and verify the unmodified single-instance entry is intact
		final, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)

		var afterCommit TestNamedStruct
		found, err = final.Get("solo", &afterCommit)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, singleValue, afterCommit)
	})

	t.Run("reload_preserves_multiple_singles", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)

		// two single-instance struct values of different types
		named := TestNamedStruct{Value: "named", ID: 7}
		nested := TestNestedStruct{Inner: TestNamedStruct{Value: "inner", ID: 8}}
		require.NoError(t, mOrig.Set("k:named", named))
		require.NoError(t, mOrig.Set("k:nested", nested))
		require.NoError(t, mOrig.Set("k:str", "primitive"))
		require.NoError(t, mOrig.Commit())

		reloaded, err := OpenCSV(tmpFile)
		require.NoError(t, err)

		// mutate only the primitive and commit
		require.NoError(t, reloaded.Set("k:str", "primitive-2"))
		require.NoError(t, reloaded.Commit())

		final, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)

		var gotNamed TestNamedStruct
		found, err := final.Get("k:named", &gotNamed)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, named, gotNamed)

		var gotNested TestNestedStruct
		found, err = final.Get("k:nested", &gotNested)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, nested, gotNested)
	})

	t.Run("reload_preserves_empty_struct", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)

		// struct whose stripped JSON is {} exercises the empty-field-set synthesized-id path
		empty := TestNamedStruct{}
		require.NoError(t, mOrig.Set("k:empty", empty))
		require.NoError(t, mOrig.Set("k:str", "primitive"))
		require.NoError(t, mOrig.Commit())

		reloaded, err := OpenCSV(tmpFile)
		require.NoError(t, err)

		require.NoError(t, reloaded.Set("k:str", "primitive-2"))
		require.NoError(t, reloaded.Commit())

		final, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)

		var got TestNamedStruct
		found, err := final.Get("k:empty", &got)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, empty, got)
	})

	t.Run("reload_byte_stable_unchanged", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)

		singleton := TestNamedStruct{Value: "stable", ID: 7, Bool: true}
		require.NoError(t, mOrig.Set("k:struct", singleton))
		require.NoError(t, mOrig.Set("k:str", "primitive"))
		require.NoError(t, mOrig.Set("k:int", 42))
		require.NoError(t, mOrig.Commit())

		originalBytes, err := os.ReadFile(tmpFile)
		require.NoError(t, err)
		// locate the singleton's type-2 line so we can assert it is preserved verbatim
		var singletonLine string
		for _, line := range strings.Split(string(originalBytes), "\n") {
			if strings.HasPrefix(line, "2,k:struct,") {
				singletonLine = line
				break
			}
		}
		require.NotEmpty(t, singletonLine, "expected a type-2 line for the singleton in: %s", string(originalBytes))

		reloaded, err := OpenCSV(tmpFile)
		require.NoError(t, err)

		require.NoError(t, reloaded.Set("k:str", "primitive-changed"))
		require.NoError(t, reloaded.Commit())

		updatedBytes, err := os.ReadFile(tmpFile)
		require.NoError(t, err)
		assert.Contains(t, string(updatedBytes), singletonLine,
			"unchanged singleton line should appear verbatim after reload+commit. before:\n%s\nafter:\n%s",
			string(originalBytes), string(updatedBytes))

		final, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)
		var got TestNamedStruct
		found, err := final.Get("k:struct", &got)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, singleton, got)
	})

	t.Run("mixed_singleton_and_slice", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)

		singleton := TestNamedStruct{Value: "solo", ID: 1}
		slice := []TestNamedStruct{
			{Value: "a", ID: 10},
			{Value: "b", ID: 20},
		}
		require.NoError(t, mOrig.Set("k:singleton", singleton))
		require.NoError(t, mOrig.Set("k:slice", slice))
		require.NoError(t, mOrig.Commit())

		// singleton (type 2) + exploded slice (type 11/12) layout
		verifyFileSize(t, tmpFile, 224)

		reloaded, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)

		var gotSingleton TestNamedStruct
		found, err := reloaded.Get("k:singleton", &gotSingleton)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, singleton, gotSingleton)

		var gotSlice []TestNamedStruct
		found, err = reloaded.Get("k:slice", &gotSlice)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, slice, gotSlice)
	})

	t.Run("two_slices_different_values", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)

		sliceA := []TestNamedStruct{
			{Value: "a0", ID: 1, Bool: true},
			{Value: "a1", ID: 2},
			{Value: "a2", ID: 3, Float: 1.5},
		}
		sliceB := []TestNamedStruct{
			{Value: "b0", ID: 100},
			{Value: "b1", ID: 200, Bool: true},
		}
		require.NoError(t, mOrig.Set("k:sliceA", sliceA))
		require.NoError(t, mOrig.Set("k:sliceB", sliceB))
		require.NoError(t, mOrig.Commit())

		reloaded, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)

		var gotA []TestNamedStruct
		found, err := reloaded.Get("k:sliceA", &gotA)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, sliceA, gotA)

		var gotB []TestNamedStruct
		found, err = reloaded.Get("k:sliceB", &gotB)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, sliceB, gotB)
	})

	t.Run("two_slices_divergent_fields", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)

		// sliceA populates Value+ID; sliceB populates Value+Float+Bool
		sliceA := []TestNamedStruct{
			{Value: "a0", ID: 11},
			{Value: "a1", ID: 22},
		}
		sliceB := []TestNamedStruct{
			{Value: "b0", Float: 1.25, Bool: true},
			{Value: "b1", Float: 2.5},
		}
		require.NoError(t, mOrig.Set("k:sliceA", sliceA))
		require.NoError(t, mOrig.Set("k:sliceB", sliceB))
		require.NoError(t, mOrig.Commit())

		reloaded, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)

		var gotA []TestNamedStruct
		found, err := reloaded.Get("k:sliceA", &gotA)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, sliceA, gotA)

		var gotB []TestNamedStruct
		found, err = reloaded.Get("k:sliceB", &gotB)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, sliceB, gotB)
	})

	t.Run("save_load_custom_marshaler", func(t *testing.T) {
		t.Parallel()
		tmpFile, mOrig := makeTestMap(t)

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

type sliceOptEmptyable struct {
	A int
	B int
}

type sliceOptOther struct {
	X int
}

type sliceOptStringMarshaler struct {
	S string
}

func (m sliceOptStringMarshaler) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.S)
}

type sliceOptNamedBytes []byte

type sliceOptOuter struct {
	Name  string
	Inner []int
}

// writeRawFile creates a CSV file with the literal contents provided.
func writeRawFile(t *testing.T, contents string) string {
	t.Helper()

	f, err := os.CreateTemp("", "ffmap-raw.*.csv")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(f.Name()) })
	_, err = f.WriteString(contents)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

func TestKeyValueCSV_SliceOptimization(t *testing.T) {
	t.Parallel()

	t.Run("nil_element_in_slice", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		slice := []*TestNamedStruct{{Value: "a", ID: 1}, nil, {Value: "c", ID: 3}}
		require.NoError(t, m.Set("k", slice))
		require.NoError(t, m.Commit())

		contents, err := os.ReadFile(tmpFile)
		require.NoError(t, err)
		assert.Contains(t, string(contents), "\n11,k,")
		assert.Contains(t, string(contents), "\n12,null\n")

		reloaded, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)
		var got []*TestNamedStruct
		found, err := reloaded.Get("k", &got)
		require.NoError(t, err)
		assert.True(t, found)
		require.Len(t, got, 3)
		assert.Equal(t, "a", got[0].Value)
		assert.Nil(t, got[1])
		assert.Equal(t, "c", got[2].Value)
	})

	t.Run("length_one_stays_type9", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		require.NoError(t, m.Set("k", []TestNamedStruct{{Value: "solo", ID: 1}}))
		require.NoError(t, m.Commit())

		contents, err := os.ReadFile(tmpFile)
		require.NoError(t, err)
		assert.NotContains(t, string(contents), "\n11,")
		assert.Contains(t, string(contents), "\n9,k,")
	})

	t.Run("single_nonnil_stays_type9", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		require.NoError(t, m.Set("k", []*TestNamedStruct{{Value: "one", ID: 1}, nil}))
		require.NoError(t, m.Commit())

		contents, err := os.ReadFile(tmpFile)
		require.NoError(t, err)
		assert.NotContains(t, string(contents), "\n11,")
		assert.Contains(t, string(contents), "\n9,k,")
	})

	t.Run("all_zero_ptr_struct_falls_back", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		require.NoError(t, m.Set("k", []*sliceOptEmptyable{{}, {}}))
		require.NoError(t, m.Commit())

		contents, err := os.ReadFile(tmpFile)
		require.NoError(t, err)
		assert.NotContains(t, string(contents), "\n11,")
		assert.Contains(t, string(contents), "\n9,k,")
	})

	t.Run("all_zero_struct_falls_back", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		require.NoError(t, m.Set("k", []sliceOptEmptyable{{}, {}}))
		require.NoError(t, m.Commit())

		contents, err := os.ReadFile(tmpFile)
		require.NoError(t, err)
		assert.NotContains(t, string(contents), "\n11,")
		assert.Contains(t, string(contents), "\n9,k,")
	})

	t.Run("mid_slice_edit_single_line_diff", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		original := []TestNamedStruct{
			{Value: "a", ID: 1},
			{Value: "b", ID: 2},
			{Value: "c", ID: 3},
		}
		require.NoError(t, m.Set("k", original))
		require.NoError(t, m.Commit())
		before, err := os.ReadFile(tmpFile)
		require.NoError(t, err)

		modified := slices.Clone(original)
		modified[1] = TestNamedStruct{Value: "B!", ID: 22}
		require.NoError(t, m.Set("k", modified))
		require.NoError(t, m.Commit())
		after, err := os.ReadFile(tmpFile)
		require.NoError(t, err)

		beforeLines := strings.Split(string(before), "\n")
		afterLines := strings.Split(string(after), "\n")
		require.Len(t, afterLines, len(beforeLines))
		var diffCount int
		for i := range beforeLines {
			if beforeLines[i] != afterLines[i] {
				diffCount++
			}
		}
		assert.Equal(t, 1, diffCount)
	})

	t.Run("nested_slice_not_recursive", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		outer := []sliceOptOuter{
			{Name: "a", Inner: []int{1, 2, 3}},
			{Name: "b", Inner: []int{4, 5}},
		}
		require.NoError(t, m.Set("k", outer))
		require.NoError(t, m.Commit())

		contents, err := os.ReadFile(tmpFile)
		require.NoError(t, err)
		assert.Equal(t, 1, strings.Count(string(contents), "\n11,"))
		assert.Contains(t, string(contents), "[1,2,3]")
		assert.Contains(t, string(contents), "[4,5]")

		reloaded, err := OpenReadOnlyCSV(tmpFile)
		require.NoError(t, err)
		var got []sliceOptOuter
		found, err := reloaded.Get("k", &got)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, outer, got)
	})

	t.Run("byte_slice_stays_type9", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		require.NoError(t, m.Set("k", []byte{1, 2, 3, 4}))
		require.NoError(t, m.Commit())

		contents, err := os.ReadFile(tmpFile)
		require.NoError(t, err)
		assert.NotContains(t, string(contents), "\n11,")
		assert.Contains(t, string(contents), "\n9,k,")
	})

	t.Run("named_byte_alias_stays_type9", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		require.NoError(t, m.Set("k", sliceOptNamedBytes{1, 2, 3}))
		require.NoError(t, m.Commit())

		contents, err := os.ReadFile(tmpFile)
		require.NoError(t, err)
		assert.NotContains(t, string(contents), "\n11,")
		assert.Contains(t, string(contents), "\n9,k,")
	})

	t.Run("heterogeneous_any_falls_back", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		require.NoError(t, m.Set("k", []any{TestNamedStruct{ID: 1}, sliceOptOther{X: 9}}))
		require.NoError(t, m.Commit())

		assert.Equal(t, "[]any", m.memoryMap.data["k"].structId)
		contents, err := os.ReadFile(tmpFile)
		require.NoError(t, err)
		assert.NotContains(t, string(contents), "\n11,")
		assert.Contains(t, string(contents), "\n9,k,")
	})

	t.Run("heterogeneous_any_mixed_kinds_falls_back", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		require.NoError(t, m.Set("k", []any{TestNamedStruct{ID: 1}, "scalar"}))
		require.NoError(t, m.Commit())

		assert.Equal(t, "[]any", m.memoryMap.data["k"].structId)
		contents, err := os.ReadFile(tmpFile)
		require.NoError(t, err)
		assert.NotContains(t, string(contents), "\n11,")
	})

	t.Run("homogeneous_any_matches_typed", func(t *testing.T) {
		_, mTyped := makeTestMap(t)
		require.NoError(t, mTyped.Set("k", []TestNamedStruct{{Value: "x", ID: 1}, {Value: "y", ID: 2}}))
		require.NoError(t, mTyped.Commit())

		_, mAny := makeTestMap(t)
		require.NoError(t, mAny.Set("k", []any{TestNamedStruct{Value: "x", ID: 1}, TestNamedStruct{Value: "y", ID: 2}}))
		require.NoError(t, mAny.Commit())

		bytesTyped, err := os.ReadFile(mTyped.filename)
		require.NoError(t, err)
		bytesAny, err := os.ReadFile(mAny.filename)
		require.NoError(t, err)
		assert.Equal(t, string(bytesTyped), string(bytesAny))
	})

	t.Run("homogeneous_any_ptr_mixed", func(t *testing.T) {
		_, m := makeTestMap(t)
		require.NoError(t, m.Set("k", []any{&TestNamedStruct{Value: "x", ID: 1}, TestNamedStruct{Value: "y", ID: 2}}))
		require.NoError(t, m.Commit())

		contents, err := os.ReadFile(m.filename)
		require.NoError(t, err)
		assert.Contains(t, string(contents), "\n11,k,[]ffmap.TestNamedStruct-")
	})

	t.Run("nonobject_marshaler_falls_back", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		require.NoError(t, m.Set("k", []sliceOptStringMarshaler{{S: "one"}, {S: "two"}}))
		require.NoError(t, m.Commit())

		contents, err := os.ReadFile(tmpFile)
		require.NoError(t, err)
		assert.NotContains(t, string(contents), "\n11,")
		assert.Contains(t, string(contents), "\n9,k,")
	})

	t.Run("mixed_marshaler_falls_back", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		require.NoError(t, m.Set("k", []any{
			TestNamedStruct{Value: "obj", ID: 1},
			sliceOptStringMarshaler{S: "string"},
		}))
		require.NoError(t, m.Commit())

		contents, err := os.ReadFile(tmpFile)
		require.NoError(t, err)
		assert.NotContains(t, string(contents), "\n11,")
	})

	t.Run("eof_flush_type12", func(t *testing.T) {
		body := "ver:1\n" +
			"11,k,[]ffmap.TestNamedStruct-cijsoo,ID,Value\n" +
			"12,\"[1,\"\"a\"\"]\"\n" +
			"12,\"[2,\"\"b\"\"]\"\n"
		path := writeRawFile(t, body)
		loaded, err := OpenReadOnlyCSV(path)
		require.NoError(t, err)
		var got []TestNamedStruct
		found, err := loaded.Get("k", &got)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []TestNamedStruct{{Value: "a", ID: 1}, {Value: "b", ID: 2}}, got)
	})

	t.Run("stray_type12_rejected", func(t *testing.T) {
		body := "ver:1\n12,\"[1,\"\"a\"\"]\"\n"
		path := writeRawFile(t, body)
		_, err := OpenCSV(path)
		require.Error(t, err)
		var ve *ValidationError
		require.ErrorAs(t, err, &ve)
		assert.Contains(t, ve.Message, "without preceding header")
	})

	t.Run("type12_field_count_mismatch", func(t *testing.T) {
		body := "ver:1\n" +
			"11,k,[]ffmap.TestNamedStruct-cijsoo,ID,Value\n" +
			"12,\"[1]\"\n"
		path := writeRawFile(t, body)
		_, err := OpenCSV(path)
		require.Error(t, err)
		var ve *ValidationError
		require.ErrorAs(t, err, &ve)
		assert.Contains(t, ve.Message, "value count mismatch")
		assert.Contains(t, ve.Message, "key=k")
	})

	t.Run("type11_zero_values_loads_empty", func(t *testing.T) {
		body := "ver:1\n11,k,[]ffmap.TestNamedStruct-cijsoo,ID,Value\n"
		path := writeRawFile(t, body)
		m, err := OpenCSV(path)
		require.NoError(t, err)
		var got []TestNamedStruct
		found, err := m.Get("k", &got)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Empty(t, got)
	})

	t.Run("type11_one_value_loads_single", func(t *testing.T) {
		body := "ver:1\n" +
			"11,k,[]ffmap.TestNamedStruct-cijsoo,ID,Value\n" +
			"12,\"[7,\"\"x\"\"]\"\n"
		path := writeRawFile(t, body)
		m, err := OpenCSV(path)
		require.NoError(t, err)
		var got []TestNamedStruct
		found, err := m.Get("k", &got)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []TestNamedStruct{{Value: "x", ID: 7}}, got)
	})

	t.Run("two_consecutive_type11_headers", func(t *testing.T) {
		body := "ver:1\n" +
			"11,kA,[]ffmap.TestNamedStruct-cijsoo,ID,Value\n" +
			"11,kB,[]ffmap.TestNamedStruct-cijsoo,ID,Value\n" +
			"12,\"[2,\"\"b\"\"]\"\n"
		path := writeRawFile(t, body)
		m, err := OpenCSV(path)
		require.NoError(t, err)
		var gotA []TestNamedStruct
		foundA, err := m.Get("kA", &gotA)
		require.NoError(t, err)
		assert.True(t, foundA)
		assert.Empty(t, gotA)
		var gotB []TestNamedStruct
		foundB, err := m.Get("kB", &gotB)
		require.NoError(t, err)
		assert.True(t, foundB)
		assert.Equal(t, []TestNamedStruct{{Value: "b", ID: 2}}, gotB)
	})

	t.Run("ver0_load_then_commit_no_change", func(t *testing.T) {
		original := "ver:0\n4,k,42\n"
		path := writeRawFile(t, original)
		m, err := OpenCSV(path)
		require.NoError(t, err)
		require.NoError(t, m.Commit())
		got, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, original, string(got))
	})

	t.Run("reload_no_op_byte_stable_mixed", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		require.NoError(t, m.Set("k:singleton", TestNamedStruct{Value: "solo", ID: 1}))
		require.NoError(t, m.Set("k:a", TestNamedStruct{Value: "a", ID: 10}))
		require.NoError(t, m.Set("k:b", TestNamedStruct{Value: "b", ID: 20}))
		require.NoError(t, m.Set("k:rawslice", []byte{1, 2, 3}))
		require.NoError(t, m.Set("k:slice", []TestNamedStruct{{Value: "p", ID: 100}, {Value: "q", ID: 200}}))
		require.NoError(t, m.Commit())
		before, err := os.ReadFile(tmpFile)
		require.NoError(t, err)

		m2, err := OpenCSV(tmpFile)
		require.NoError(t, err)
		require.NoError(t, m2.Commit())
		after, err := os.ReadFile(tmpFile)
		require.NoError(t, err)
		assert.Equal(t, string(before), string(after))
	})

	t.Run("reload_unrelated_set_byte_stable_mixed", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)
		require.NoError(t, m.Set("k:singleton", TestNamedStruct{Value: "solo", ID: 1}))
		require.NoError(t, m.Set("k:a", TestNamedStruct{Value: "a", ID: 10}))
		require.NoError(t, m.Set("k:b", TestNamedStruct{Value: "b", ID: 20}))
		require.NoError(t, m.Set("k:rawslice", []byte{1, 2, 3}))
		require.NoError(t, m.Set("k:slice", []TestNamedStruct{{Value: "p", ID: 100}, {Value: "q", ID: 200}}))
		require.NoError(t, m.Commit())
		before, err := os.ReadFile(tmpFile)
		require.NoError(t, err)

		m2, err := OpenCSV(tmpFile)
		require.NoError(t, err)
		require.NoError(t, m2.Set("k:trigger", 99))
		require.NoError(t, m2.Commit())
		after, err := os.ReadFile(tmpFile)
		require.NoError(t, err)
		for _, line := range strings.Split(string(before), "\n") {
			if line == "" || line == "ver:1" {
				continue
			}
			assert.Contains(t, string(after), line)
		}
	})

	t.Run("ver0_upgrade_roundtrip", func(t *testing.T) {
		body := "ver:0\n" +
			"3,k:str,hello\n" +
			"4,k:int,42\n" +
			"2,k:struct,\"{\"\"ID\"\":1,\"\"Value\"\":\"\"solo\"\"}\"\n" +
			"9,k:slice,\"[{\"\"ID\"\":1,\"\"Value\"\":\"\"a\"\"},{\"\"ID\"\":2,\"\"Value\"\":\"\"b\"\"}]\"\n"
		path := writeRawFile(t, body)
		m, err := OpenCSV(path)
		require.NoError(t, err)
		require.NoError(t, m.Set("k:trigger", 1))
		require.NoError(t, m.Commit())

		reloaded, err := OpenReadOnlyCSV(path)
		require.NoError(t, err)
		var s string
		found, err := reloaded.Get("k:str", &s)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "hello", s)

		var i int
		found, err = reloaded.Get("k:int", &i)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, 42, i)

		var single TestNamedStruct
		found, err = reloaded.Get("k:struct", &single)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "solo", single.Value)
		assert.Equal(t, 1, single.ID)

		var slice []TestNamedStruct
		found, err = reloaded.Get("k:slice", &slice)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []TestNamedStruct{{Value: "a", ID: 1}, {Value: "b", ID: 2}}, slice)

		// the reloaded slice carries no structId and must remain type-9 on commit
		final, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Contains(t, string(final), "\n9,k:slice,")
		assert.NotContains(t, string(final), "\n11,k:slice,")
	})

	t.Run("ver0_upgrade_then_reset_explodes", func(t *testing.T) {
		body := "ver:0\n" +
			"9,k:slice,\"[{\"\"ID\"\":1,\"\"Value\"\":\"\"a\"\"},{\"\"ID\"\":2,\"\"Value\"\":\"\"b\"\"}]\"\n"
		path := writeRawFile(t, body)
		m, err := OpenCSV(path)
		require.NoError(t, err)

		var slice []TestNamedStruct
		found, err := m.Get("k:slice", &slice)
		require.NoError(t, err)
		assert.True(t, found)
		require.NoError(t, m.Set("k:slice", slice))
		require.NoError(t, m.Commit())

		final, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Contains(t, string(final), "\n11,k:slice,")
		assert.NotContains(t, string(final), "\n9,k:slice,")
	})
}

func TestKeyValueCSV_ArrayOptimization(t *testing.T) {
	t.Parallel()

	t.Run("ver0_upgrade_then_reset_explodes", func(t *testing.T) {
		body := "ver:0\n" +
			"9,k:arr,\"[{\"\"ID\"\":1,\"\"Value\"\":\"\"a\"\"},{\"\"ID\"\":2,\"\"Value\"\":\"\"b\"\"}]\"\n"
		path := writeRawFile(t, body)
		m, err := OpenCSV(path)
		require.NoError(t, err)

		var arr [2]TestNamedStruct
		found, err := m.Get("k:arr", &arr)
		require.NoError(t, err)
		assert.True(t, found)
		require.NoError(t, m.Set("k:arr", arr))
		require.NoError(t, m.Commit())

		final, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Contains(t, string(final), "\n11,k:arr,")
		assert.NotContains(t, string(final), "\n9,k:arr,")

		// reload + no-op commit is byte-stable
		m2, err := OpenCSV(path)
		require.NoError(t, err)
		require.NoError(t, m2.Commit())
		afterReload, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, string(final), string(afterReload))
	})
}

func TestKeyValueCSV_RejectSymlinks(t *testing.T) {
	t.Run("on_open", func(t *testing.T) {
		t.Parallel()

		// Create a real file
		realFile, err := os.CreateTemp("", "real.*.csv")
		require.NoError(t, err)
		realFileName := realFile.Name()
		t.Cleanup(func() { _ = os.Remove(realFileName) })
		require.NoError(t, realFile.Close())

		// Create a symlink pointing to it
		symlinkFile, err := os.CreateTemp("", "symlink.*.csv")
		require.NoError(t, err)
		symlinkName := symlinkFile.Name()
		require.NoError(t, symlinkFile.Close())
		require.NoError(t, os.Remove(symlinkName))
		require.NoError(t, os.Symlink(realFileName, symlinkName))
		t.Cleanup(func() { _ = os.Remove(symlinkName) })

		// Try to open via symlink - should fail
		_, err = OpenCSV(symlinkName)
		require.Error(t, err)
	})

	t.Run("on_commit", func(t *testing.T) {
		t.Parallel()

		// Create a temporary file for the map
		tmpFile, err := os.CreateTemp("", "testmap.*.csv")
		require.NoError(t, err)
		tmpFileName := tmpFile.Name()
		t.Cleanup(func() { _ = os.Remove(tmpFileName) })
		require.NoError(t, tmpFile.Close())
		require.NoError(t, os.Remove(tmpFileName))

		// Open the map normally
		m, err := OpenCSV(tmpFileName)
		require.NoError(t, err)

		// Set some data
		require.NoError(t, m.Set("key", "value"))

		// Now replace the file with a symlink
		realFile, err := os.CreateTemp("", "real.*.csv")
		require.NoError(t, err)
		realFileName := realFile.Name()
		t.Cleanup(func() { _ = os.Remove(realFileName) })
		require.NoError(t, realFile.Close())

		require.NoError(t, os.Symlink(realFileName, tmpFileName))

		// Try to commit - should fail
		err = m.Commit()
		require.Error(t, err)
	})
}

func TestKeyValueCSV_Size(t *testing.T) {
	t.Parallel()
	_, m := makeTestMap(t)

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
			name:             "string",
			value:            "testString",
			expectedDataType: dataString,
		},
		{
			name:             "bool",
			value:            true,
			expectedDataType: dataBool,
		},
		{
			name:             "float32",
			value:            float32(3.14),
			expectedDataType: dataFloat,
		},
		{
			name:             "float64",
			value:            float64(3.1415),
			expectedDataType: dataFloat,
		},
		{
			name:             "int",
			value:            int(42),
			expectedDataType: dataInt,
		},
		{
			name:             "int8",
			value:            int8(8),
			expectedDataType: dataInt,
		},
		{
			name:             "int16",
			value:            int16(16),
			expectedDataType: dataInt,
		},
		{
			name:             "int32",
			value:            int32(32),
			expectedDataType: dataInt,
		},
		{
			name:             "int64",
			value:            int64(64),
			expectedDataType: dataInt,
		},
		{
			name:             "uint",
			value:            uint(1),
			expectedDataType: dataUint,
		},
		{
			name:             "uint8",
			value:            uint8(8),
			expectedDataType: dataUint,
		},
		{
			name:             "uint16",
			value:            uint16(16),
			expectedDataType: dataUint,
		},
		{
			name:             "uint32",
			value:            uint32(32),
			expectedDataType: dataUint,
		},
		{
			name:             "uint64",
			value:            uint64(64),
			expectedDataType: dataUint,
		},
		{
			name:             "complex64",
			value:            complex64(complex(5, 6)),
			expectedDataType: dataComplexNum,
		},
		{
			name:             "complex128",
			value:            complex128(complex(7, 8)),
			expectedDataType: dataComplexNum,
		},
		{
			name:             "custom_struct",
			value:            struct{ Name string }{"Test"},
			expectedDataType: dataStructJson,
		},
		{
			name: "named_struct",
			value: TestNamedStruct{
				Value: "foo",
				ID:    123,
				Map:   map[string]TestNamedStruct{"bar": {Value: "bar", ID: 987, Bool: true}},
			},
			expectedDataType: dataStructJson,
		},
		{
			name: "pointer_struct",
			value: TestPointerStruct{
				S: &defaultStr,
				I: &defaultI,
				T: &defaultT,
			},
			expectedDataType: dataStructJson,
		},
		{
			name: "custom_json_struct",
			value: TestCustomJsonStruct{
				Value: "foo",
			},
			expectedDataType: dataStructJson,
		},
		{
			name:             "map",
			value:            map[string]string{"foo1": "bar1", "foo2": "bar2"},
			expectedDataType: dataMap,
		},
		{
			name:             "byte_array",
			value:            [4]byte{1, 2, 3, 4},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "byte_slice",
			value:            []byte{1, 2, 3, 4},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "int_array",
			value:            [4]int{1, 2, 3, 4},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "int_slice",
			value:            []int{1, 2, 3, 4},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "int64_array",
			value:            [4]int64{1000, 2000, 3000, 4000},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "int64_slice",
			value:            []int64{1000, 2000, 3000, 4000},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "string_array",
			value:            [2]string{"foo", "bar"},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "string_slice",
			value:            []string{"foo", "bar"},
			expectedDataType: dataArraySlice,
		},
		{
			name: "struct_slice",
			value: []TestNamedStruct{
				{Value: "foo", ID: 123},
				{Value: "bar", ID: 456},
			},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "time",
			value:            time.Now(),
			expectedDataType: dataStructJson,
		},
		{
			name:             "map_pointer",
			value:            &map[string]string{"foo": "bar", "key": "value"},
			expectedDataType: dataMap,
		},
		{
			name:             "struct_pointer",
			value:            &TestNamedStruct{Value: "foo", ID: 123},
			expectedDataType: dataStructJson,
		},
		{
			name:             "float32_slice",
			value:            []float32{1.1, 2.2, 3.3},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "float64_slice",
			value:            []float64{1.1, 2.2, 3.3},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "uint_slice",
			value:            []uint{1, 2, 3, 4},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "nested_int_slice",
			value:            [][]int{{1, 2}, {3, 4}},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "nested_string_slice",
			value:            [][]string{{"a", "b"}, {"c", "d"}},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "empty_slice",
			value:            []int{},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "empty_array",
			value:            [0]int{},
			expectedDataType: dataArraySlice,
		},
		{
			name:             "pointer_slice",
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
			name:     "string",
			setValue: "testString",
			getValue: new(string),
		},
		{
			name:     "bool",
			setValue: true,
			getValue: new(bool),
		},
		{
			name:     "float32",
			setValue: float32(3.14),
			getValue: new(float32),
		},
		{
			name:     "float64",
			setValue: float64(3.1415),
			getValue: new(float64),
		},
		{
			name:     "int",
			setValue: int(42),
			getValue: new(int),
		},
		{
			name:     "int8",
			setValue: int8(8),
			getValue: new(int8),
		},
		{
			name:     "int16",
			setValue: int16(16),
			getValue: new(int16),
		},
		{
			name:     "int32",
			setValue: int32(32),
			getValue: new(int32),
		},
		{
			name:     "int64",
			setValue: int64(64),
			getValue: new(int64),
		},
		{
			name:     "uint",
			setValue: uint(1),
			getValue: new(uint),
		},
		{
			name:     "uint8",
			setValue: uint8(8),
			getValue: new(uint8),
		},
		{
			name:     "uint16",
			setValue: uint16(16),
			getValue: new(uint16),
		},
		{
			name:     "uint32",
			setValue: uint32(32),
			getValue: new(uint32),
		},
		{
			name:     "uint64",
			setValue: uint64(64),
			getValue: new(uint64),
		},
		{
			name:     "complex64",
			setValue: complex64(complex(5, 6)),
			getValue: new(complex64),
		},
		{
			name:     "complex128",
			setValue: complex128(complex(7, 8)),
			getValue: new(complex128),
		},
		{
			name:     "complex64_neg_imag",
			setValue: complex64(complex(5, -6)),
			getValue: new(complex64),
		},
		{
			name:     "complex128_neg",
			setValue: complex128(complex(-7, -8)),
			getValue: new(complex128),
		},
		{
			name:     "custom_struct",
			setValue: struct{ Name string }{"Test"},
			getValue: new(struct{ Name string }),
		},
		{
			name: "named_struct",
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
			name: "map_value_with_zero_key",
			setValue: TestNamedStruct{
				Value:     "foo",
				MapIntKey: map[int]string{0: "zero", 1: "one"},
			},
			getValue: new(TestNamedStruct),
		},
		{
			name: "map_value_with_zero_value",
			setValue: TestNamedStruct{
				Value:     "foo",
				MapIntKey: map[int]string{1: "", 2: ""},
			},
			getValue: new(TestNamedStruct),
		},
		{
			name:     "named_struct_empty",
			setValue: TestNamedStruct{},
			getValue: new(TestNamedStruct),
		},
		{
			name:     "named_struct_pointer",
			setValue: &TestNamedStruct{},
			getValue: new(*TestNamedStruct),
		},
		{
			name: "pointer_struct",
			setValue: TestPointerStruct{
				S: &defaultStr,
				I: &defaultI,
				T: &defaultT,
			},
			getValue: new(TestPointerStruct),
		},
		{
			name:     "embedded_struct",
			setValue: TestStructEmbedded{TestNamedStruct: TestNamedStruct{Value: "foo", ID: 1}, DirectStr: "str"},
			getValue: new(TestStructEmbedded),
		},
		{
			name: "custom_json_struct_empty",
			setValue: TestCustomJsonStruct{
				Value: "foo",
			},
			getValue: new(TestCustomJsonStruct),
		},
		{
			name: "custom_json_struct_filled",
			setValue: TestCustomJsonStruct{
				Value:    "foo",
				EmptyStr: "str",
				EmptyInt: -1,
				ZValue:   "z",
			},
			getValue: new(TestCustomJsonStruct),
		},
		{
			name:     "map",
			setValue: map[string]string{"foo1": "bar1", "foo2": "bar2"},
			getValue: new(map[string]string),
		},
		{
			name:     "map_with_zero_key",
			setValue: map[string]string{"": "foo"},
			getValue: new(map[string]string),
		},
		{
			name:     "map_with_zero_value",
			setValue: map[string]string{"foo": ""},
			getValue: new(map[string]string),
		},
		{
			name:     "byte_array",
			setValue: [4]byte{1, 2, 3, 4},
			getValue: new([4]byte),
		},
		{
			name:     "byte_slice",
			setValue: []byte{1, 2, 3, 4},
			getValue: new([]byte),
		},
		{
			name:     "int_array",
			setValue: [4]int{1, 2, 3, 4},
			getValue: new([4]int),
		},
		{
			name:     "int_slice",
			setValue: []int{1, 2, 3, 4},
			getValue: new([]int),
		},
		{
			name:     "int64_array",
			setValue: [4]int64{1000, 2000, 3000, 4000},
			getValue: new([4]int64),
		},
		{
			name:     "int64_slice",
			setValue: []int64{1000, 2000, 3000, 4000},
			getValue: new([]int64),
		},
		{
			name:     "string_array",
			setValue: [2]string{"foo", "bar"},
			getValue: new([2]string),
		},
		{
			name:     "string_slice",
			setValue: []string{"foo", "bar"},
			getValue: new([]string),
		},
		{
			name: "struct_slice",
			setValue: []TestNamedStruct{
				{Value: "foo", ID: 123},
				{Value: "bar", ID: 456},
			},
			getValue: new([]TestNamedStruct),
		},
		{
			name:     "map_pointer",
			setValue: &map[string]string{"foo": "bar", "key": "value"},
			getValue: new(*map[string]string),
		},
		{
			name:     "struct_pointer",
			setValue: &TestNamedStruct{Value: "foo", ID: 123},
			getValue: new(*TestNamedStruct),
		},
		{
			name:     "nested_map",
			setValue: map[string]map[string]int{"outer": {"inner": 42}},
			getValue: new(map[string]map[string]int),
		},
		{
			name:     "mixed_type_slice",
			setValue: []interface{}{"two", 3.0},
			getValue: new([]interface{}),
		},
		{
			name:     "pointer_slice",
			setValue: []*int{new(int), new(int)},
			getValue: new([]*int),
		},
		{
			name:     "uint_slice",
			setValue: []uint{10, 20, 30},
			getValue: new([]uint),
		},
		{
			name:     "uint8_slice",
			setValue: []uint8{1, 2, 3, 4},
			getValue: new([]uint8),
		},
		{
			name:     "uint16_slice",
			setValue: []uint16{100, 200, 300},
			getValue: new([]uint16),
		},
		{
			name:     "uint32_slice",
			setValue: []uint32{1000, 2000, 3000},
			getValue: new([]uint32),
		},
		{
			name:     "uint64_slice",
			setValue: []uint64{10000, 20000, 30000},
			getValue: new([]uint64),
		},
		{
			name:     "float32_slice",
			setValue: []float32{3.14, 6.28},
			getValue: new([]float32),
		},
		{
			name:     "float64_slice",
			setValue: []float64{3.1415, 2.718},
			getValue: new([]float64),
		},
		{
			name:     "bool_slice",
			setValue: []bool{true, false, true},
			getValue: new([]bool),
		},
		{
			name:     "uint_array",
			setValue: [4]uint{10, 20, 30, 40},
			getValue: new([4]uint),
		},
		{
			name:     "uint8_array",
			setValue: [4]uint8{1, 2, 3, 4},
			getValue: new([4]uint8),
		},
		{
			name:     "uint16_array",
			setValue: [4]uint16{100, 200, 300, 400},
			getValue: new([4]uint16),
		},
		{
			name:     "uint32_array",
			setValue: [4]uint32{1000, 2000, 3000, 4000},
			getValue: new([4]uint32),
		},
		{
			name:     "uint64_array",
			setValue: [4]uint64{10000, 20000, 30000, 40000},
			getValue: new([4]uint64),
		},
		{
			name:     "float32_array",
			setValue: [4]float32{3.14, 6.28, 9.42, 12.56},
			getValue: new([4]float32),
		},
		{
			name:     "float64_array",
			setValue: [4]float64{3.1415, 2.718, 1.618, 0.577},
			getValue: new([4]float64),
		},
		{
			name:     "bool_array",
			setValue: [4]bool{true, false, true, false},
			getValue: new([4]bool),
		},
		{
			name: "nested_struct",
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
			name: "struct_with_slice",
			setValue: TestStructWithSlice{
				Values: []TestNamedStruct{
					{Value: "first", ID: 1},
					{Value: "second", ID: 2},
				},
			},
			getValue: new(TestStructWithSlice),
		},
		{
			name: "pointer_struct_expanded",
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
			name:     "anonymous_struct_slice",
			setValue: []struct{ Name string }{{Name: "Alice"}, {Name: "Bob"}},
			getValue: new([]struct{ Name string }),
		},
		{
			name:     "pointer_struct_slice",
			setValue: []*TestPointerStruct{{S: &defaultStr, I: &defaultI}, {T: &defaultT, F: &defaultF}},
			getValue: new([]*TestPointerStruct),
		},
		{
			name:     "empty_struct",
			setValue: struct{}{},
			getValue: new(struct{}),
		},
		{
			name: "struct_with_mixed_fields",
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
			name: "struct_with_embedded_struct",
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
			name: "pointer_to_empty_struct",
			setValue: &struct {
				Name string
			}{},
			getValue: new(*struct {
				Name string
			}),
		},
		{
			name:     "rune_slice",
			setValue: []rune{'a', 'b', 'c'},
			getValue: new([]rune),
		},
		{
			name:     "byte_slice_with_zeros",
			setValue: []byte{0, 1, 2, 3, 0},
			getValue: new([]byte),
		},
		{
			name:     "empty_byte_slice",
			setValue: []byte{},
			getValue: new([]byte),
		},
		{
			name:     "nil_byte_slice",
			setValue: ([]byte)(nil),
			getValue: new([]byte),
		},
		{
			name:     "struct_pointer_slice",
			setValue: []*TestNamedStruct{{Value: "first", ID: 1}, nil, {Value: "second", ID: 2}},
			getValue: new([]*TestNamedStruct),
		},
		{
			name: "embedded_anonymous_struct",
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
			name: "pointer_to_anonymous_struct",
			setValue: TestStructWithPointerToAnonymous{
				Ptr: &struct {
					Name string
					ID   int
				}{Name: "ptrAnon", ID: 888},
			},
			getValue: new(TestStructWithPointerToAnonymous),
		},
		{
			name: "typed_pointer_to_empty_struct",
			setValue: TestStructWithPointerToEmpty{
				Ptr: &struct{}{},
			},
			getValue: new(TestStructWithPointerToEmpty),
		},
		{
			name: "map_with_empty_values",
			setValue: map[string]interface{}{
				"emptyStr":   "",
				"emptyFloat": 0.0,
				"emptyBool":  false,
			},
			getValue: new(map[string]interface{}),
		},
		{
			name: "nested_pointer_map",
			setValue: map[string]*TestNamedStruct{
				"key1": {Value: "nested1", ID: 111},
				"key2": nil,
			},
			getValue: new(map[string]*TestNamedStruct),
		},
		{
			name: "struct_with_interface_field",
			setValue: struct {
				Value interface{}
			}{Value: "interfaceString"},
			getValue: new(struct {
				Value interface{}
			}),
		},
		{
			name: "embedded_anonymous_pointer",
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
			name: "deeply_nested_struct",
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
			name: "large_int_slice",
			setValue: []int{
				1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
			},
			getValue: new([]int),
		},
		{
			name: "map_with_mixed_types",
			setValue: map[string]interface{}{
				"string": "foo",
				"float":  42.0,
				"bool":   true,
			},
			getValue: new(map[string]interface{}),
		},
		{
			name:     "custom_marshaler_struct",
			setValue: TestCustomMarshaler{Value: "test", Encoded: json.RawMessage(`"custom"`)},
			getValue: new(TestCustomMarshaler),
		},
		{
			name: "embedded_custom_marshaler",
			setValue: TestStructWithEmbeddedCustomMarshaler{
				Embedded: TestCustomMarshaler{Value: "inner", Encoded: json.RawMessage(`"encoded"`)},
				Extra:    "extra",
			},
			getValue: new(TestStructWithEmbeddedCustomMarshaler),
		},
		{
			name: "multi_pointer",
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
			name: "deep_nested_struct",
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
			name: "deeply_nested_map",
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
			name:     "large_byte_slice",
			setValue: make([]byte, 1024),
			getValue: new([]byte),
		},
		{
			name:     "mixed_byte_array_and_slice",
			setValue: struct{ Data [4]byte }{Data: [4]byte{1, 2, 3, 4}},
			getValue: new(struct{ Data [4]byte }),
		},
		{
			name: "struct_with_empty_map",
			setValue: struct {
				EmptyMap map[string]int
			}{EmptyMap: map[string]int{}},
			getValue: new(struct {
				EmptyMap map[string]int
			}),
		},
		{
			name: "struct_with_nil_map",
			setValue: struct {
				NilMap map[string]int
			}{NilMap: nil},
			getValue: new(struct {
				NilMap map[string]int
			}),
		},
		{
			name: "struct_with_empty_slice",
			setValue: struct {
				EmptySlice []string
			}{EmptySlice: []string{}},
			getValue: new(struct {
				EmptySlice []string
			}),
		},
		{
			name: "struct_with_nil_slice",
			setValue: struct {
				NilSlice []string
			}{NilSlice: nil},
			getValue: new(struct {
				NilSlice []string
			}),
		},
		{
			name: "anon_pointer_to_empty_struct",
			setValue: struct {
				Ptr *struct{}
			}{Ptr: &struct{}{}},
			getValue: new(struct {
				Ptr *struct{}
			}),
		},
		{
			name: "struct_slice_with_defaults",
			setValue: []TestNamedStruct{
				{Value: "", ID: 0},
				{Value: "filled", ID: 123},
			},
			getValue: new([]TestNamedStruct),
		},
		{
			name: "struct_with_nil_interface",
			setValue: struct {
				Field interface{}
			}{Field: nil},
			getValue: new(struct{ Field interface{} }),
		},
		{
			name: "struct_with_pointer_to_slice",
			setValue: struct {
				PtrSlice *[]int
			}{PtrSlice: &[]int{1, 2, 3}},
			getValue: new(struct{ PtrSlice *[]int }),
		},
		{
			name: "empty_struct_pointer_slice",
			setValue: []*struct {
				Name string
			}{},
			getValue: new([]*struct {
				Name string
			}),
		},
		{
			name: "nil_struct_pointer_slice",
			setValue: []*struct {
				Name string
			}(nil),
			getValue: new([]*struct {
				Name string
			}),
		},
		{
			name: "map_with_empty_and_filled",
			setValue: map[string]interface{}{
				"emptyMap":  map[string]interface{}{},
				"filledMap": map[string]interface{}{"a": 1.0},
			},
			getValue: new(map[string]interface{}),
		},
		{
			name:     "slice_with_nil_values",
			setValue: []interface{}{nil, "non-nil", nil},
			getValue: new([]interface{}),
		},
		{
			name: "double_pointer",
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
			name: "triple_pointer",
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
			name: "nested_pointer_slice",
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
			name: "mixed_map_with_nil",
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
			name: "nested_mixed_map",
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
			name: "struct_with_raw_message",
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
			name: "struct_with_nil_int_pointer",
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
			_, m := makeTestMap(t)

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
	t.Run("pointer_to_empty_slice", func(t *testing.T) {
		type TestPointerSlice struct {
			Slice *[]int
		}
		emptySlice := make([]int, 0)
		orig := TestPointerSlice{Slice: &emptySlice}
		_, m := makeTestMap(t)

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
		_, m := makeTestMap(t)

		require.NoError(t, SetMapValues[string](m, nil))
		require.Equal(t, 0, m.Size())
	})
	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		_, m := makeTestMap(t)

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
		_, m := makeTestMap(t)

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
		_, m := makeTestMap(t)

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
		_, m := makeTestMap(t)

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
		_, m := makeTestMap(t)

		require.NoError(t, SetSliceValues(m, make([]int, 0), strconv.Itoa))
		require.Equal(t, 0, m.Size())
	})
	t.Run("nil_func", func(t *testing.T) {
		t.Parallel()
		_, m := makeTestMap(t)

		require.Error(t, SetSliceValues(m, make([]int, 1), nil))
		require.Equal(t, 0, m.Size())
	})
	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		_, m := makeTestMap(t)

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
		_, m := makeTestMap(t)

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
		_, m := makeTestMap(t)

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
		_, m := makeTestMap(t)

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
			name:     "nil_value",
			setValue: nil,
		},
		{
			name: "struct_with_func",
			setValue: struct {
				FuncField func() int
				Value     int
			}{FuncField: func() int { return 42 }, Value: 100},
		},
		{
			name:     "nil_ptr_to_empty_slice",
			setValue: (*[]int)(nil),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, m := makeTestMap(t)

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
			name:     "float32_over",
			setValue: math.MaxFloat32 * 2,
			getValue: new(float32),
		},
		{
			name:     "float32_under",
			setValue: -math.MaxFloat32 * 2,
			getValue: new(float32),
		},
		{
			name:     "int8_over",
			setValue: int16(math.MaxInt8 + 1),
			getValue: new(int8),
		},
		{
			name:     "int8_under",
			setValue: int16(math.MinInt8 - 1),
			getValue: new(int8),
		},
		{
			name:     "int16_over",
			setValue: int32(math.MaxInt16 + 1),
			getValue: new(int16),
		},
		{
			name:     "int16_under",
			setValue: int32(math.MinInt16 - 1),
			getValue: new(int16),
		},
		{
			name:     "int32_over",
			setValue: int64(math.MaxInt32 + 1),
			getValue: new(int32),
		},
		{
			name:     "int32_under",
			setValue: int64(math.MinInt32 - 1),
			getValue: new(int32),
		},
		{
			name:     "uint8_over",
			setValue: uint16(math.MaxUint8 + 1),
			getValue: new(uint8),
		},
		{
			name:     "uint16_over",
			setValue: uint32(math.MaxUint16 + 1),
			getValue: new(uint16),
		},
		{
			name:     "uint32_over",
			setValue: uint64(math.MaxUint32 + 1),
			getValue: new(uint32),
		},
		{
			name:     "complex64_real_over",
			setValue: complex(math.MaxFloat32*2, 6),
			getValue: new(complex64),
		},
		{
			name:     "complex64_real_under",
			setValue: complex(-math.MaxFloat32*2, 6),
			getValue: new(complex64),
		},
		{
			name:     "complex64_imag_over",
			setValue: complex(5, math.MaxFloat32*2),
			getValue: new(complex64),
		},
		{
			name:     "complex64_imag_under",
			setValue: complex(5, -math.MaxFloat32*2),
			getValue: new(complex64),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, m := makeTestMap(t)

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
	_, m := makeTestMap(t)

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
	_, m := makeTestMap(t)

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

	require.NoError(t, SetMapValues(m, testData))

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
			name:                "string",
			value:               "testString",
			expectedStrSize:     10,
			expectedFileSizeOne: 22,
			expectedFileSizeTwo: 38,
		},
		{
			name:                "bool",
			value:               true,
			expectedStrSize:     1,
			expectedFileSizeOne: 13,
			expectedFileSizeTwo: 20,
		},
		{
			name:                "float32",
			value:               float32(3.1414999961853027),
			expectedStrSize:     18,
			expectedFileSizeOne: 30,
			expectedFileSizeTwo: 54,
		},
		{
			name:                "float64",
			value:               float64(3.1415),
			expectedStrSize:     6,
			expectedFileSizeOne: 18,
			expectedFileSizeTwo: 30,
		},
		{
			name:                "int",
			value:               int(42),
			expectedStrSize:     2,
			expectedFileSizeOne: 14,
			expectedFileSizeTwo: 22,
		},
		{
			name:                "int8",
			value:               int8(8),
			expectedStrSize:     1,
			expectedFileSizeOne: 13,
			expectedFileSizeTwo: 20,
		},
		{
			name:                "int16",
			value:               int16(16),
			expectedStrSize:     2,
			expectedFileSizeOne: 14,
			expectedFileSizeTwo: 22,
		},
		{
			name:                "int32",
			value:               int32(32),
			expectedStrSize:     2,
			expectedFileSizeOne: 14,
			expectedFileSizeTwo: 22,
		},
		{
			name:                "int64",
			value:               int64(64),
			expectedStrSize:     2,
			expectedFileSizeOne: 14,
			expectedFileSizeTwo: 22,
		},
		{
			name:                "uint",
			value:               uint(1),
			expectedStrSize:     1,
			expectedFileSizeOne: 13,
			expectedFileSizeTwo: 20,
		},
		{
			name:                "uint8",
			value:               uint8(8),
			expectedStrSize:     1,
			expectedFileSizeOne: 13,
			expectedFileSizeTwo: 20,
		},
		{
			name:                "uint16",
			value:               uint16(16),
			expectedStrSize:     2,
			expectedFileSizeOne: 14,
			expectedFileSizeTwo: 22,
		},
		{
			name:                "uint32",
			value:               uint32(32),
			expectedStrSize:     2,
			expectedFileSizeOne: 14,
			expectedFileSizeTwo: 22,
		},
		{
			name:                "uint64",
			value:               uint64(64),
			expectedStrSize:     2,
			expectedFileSizeOne: 14,
			expectedFileSizeTwo: 22,
		},
		{
			name:                "complex64",
			value:               complex64(complex(5, 6)),
			expectedStrSize:     6,
			expectedFileSizeOne: 18,
			expectedFileSizeTwo: 30,
		},
		{
			name:                "complex128",
			value:               complex128(complex(7, 8)),
			expectedStrSize:     6,
			expectedFileSizeOne: 18,
			expectedFileSizeTwo: 30,
		},
		{
			name:                "custom_struct",
			value:               struct{ Name string }{"Test"},
			expectedStrSize:     15,
			expectedFileSizeOne: 33,
			expectedFileSizeTwo: 75,
		},
		{
			name: "custom_struct_empty_field",
			value: struct {
				Name     string
				EmptyStr string
			}{Name: "Test"},
			expectedStrSize:     15,
			expectedFileSizeOne: 33,
			expectedFileSizeTwo: 91,
		},
		{
			name: "named_struct",
			value: TestNamedStruct{
				Value: "foo",
				ID:    123,
				Map:   map[string]TestNamedStruct{"bar": {Value: "bar", ID: 987, Bool: true}},
			},
			expectedStrSize:     135,
			expectedFileSizeOne: 175,
			expectedFileSizeTwo: 325,
		},
		{
			name: "pointer_struct",
			value: TestPointerStruct{
				S: &defaultStr,
				I: &defaultI,
				T: &defaultT,
			},
			expectedStrSize:     41,
			expectedFileSizeOne: 65,
			expectedFileSizeTwo: 128,
		},
		{
			name: "struct_embedded",
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
			expectedFileSizeOne: 211,
			expectedFileSizeTwo: 370,
		},
		{
			name: "custom_json_struct_empty",
			value: TestCustomJsonStruct{
				Value: "foo",
			},
			expectedStrSize:     11,
			expectedFileSizeOne: 29,
			expectedFileSizeTwo: 78,
		},
		{
			name: "deeply_nested_map",
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
			expectedFileSizeOne: 84,
			expectedFileSizeTwo: 194,
		},
		{
			name:                "custom_marshaler_struct",
			value:               TestCustomMarshaler{Value: "test", Encoded: json.RawMessage(`"custom"`)},
			expectedStrSize:     35,
			expectedFileSizeOne: 57,
			expectedFileSizeTwo: 113,
		},
		{
			name: "embedded_custom_marshaler",
			value: TestStructWithEmbeddedCustomMarshaler{
				Embedded: TestCustomMarshaler{Value: "inner", Encoded: json.RawMessage(`"encoded"`)},
				Extra:    "extra",
			},
			expectedStrSize:     66,
			expectedFileSizeOne: 94,
			expectedFileSizeTwo: 204,
		},
		{
			name:                "map",
			value:               map[string]string{"foo1": "bar1", "foo2": "bar2"},
			expectedStrSize:     29,
			expectedFileSizeOne: 52,
			expectedFileSizeTwo: 98,
		},
		{
			name:                "map_zero_key",
			value:               map[string]string{"": "foo"},
			expectedStrSize:     10,
			expectedFileSizeOne: 29,
			expectedFileSizeTwo: 52,
		},
		{
			name:                "map_zero_value",
			value:               map[string]string{"foo": ""},
			expectedStrSize:     10,
			expectedFileSizeOne: 29,
			expectedFileSizeTwo: 52,
		},
		{
			name:                "byte_slice",
			value:               []byte{1, 2, 3, 4},
			expectedStrSize:     10,
			expectedFileSizeOne: 26,
			expectedFileSizeTwo: 46,
		},
		{
			name: "byte_slice_large",
			value: []byte{
				1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
				17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
			},
			expectedStrSize:     46,
			expectedFileSizeOne: 62,
			expectedFileSizeTwo: 118,
		},
		{
			name: "byte_array_64",
			value: [64]byte{
				1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
				17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
				33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
				49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
			},
			expectedStrSize:     184,
			expectedFileSizeOne: 198,
			expectedFileSizeTwo: 390,
		},
		{
			name:                "int_slice",
			value:               []int{1, 2, 3, 4},
			expectedStrSize:     9,
			expectedFileSizeOne: 23,
			expectedFileSizeTwo: 40,
		},
		{
			name:                "int64_slice",
			value:               []int64{1000, 2000, 3000, 4000},
			expectedStrSize:     21,
			expectedFileSizeOne: 35,
			expectedFileSizeTwo: 64,
		},
		{
			name:                "string_slice",
			value:               []string{"foo", "bar"},
			expectedStrSize:     13,
			expectedFileSizeOne: 31,
			expectedFileSizeTwo: 56,
		},
		{
			name: "struct_slice",
			value: []TestNamedStruct{
				{Value: "foo", ID: 123},
				{Value: "bar", ID: 456},
			},
			expectedStrSize:     111,
			expectedFileSizeOne: 145,
			expectedFileSizeTwo: 284,
		},
		{
			name: "struct_slice_five",
			value: []TestNamedStruct{
				{Value: "v0", ID: 0},
				{Value: "v1", ID: 1},
				{Value: "v2", ID: 2},
				{Value: "v3", ID: 3},
				{Value: "v4", ID: 4},
			},
			expectedStrSize:     254,
			expectedFileSizeOne: 262,
			expectedFileSizeTwo: 518,
		},
		{
			name: "struct_array_five",
			value: [5]TestNamedStruct{
				{Value: "v0", ID: 0},
				{Value: "v1", ID: 1},
				{Value: "v2", ID: 2},
				{Value: "v3", ID: 3},
				{Value: "v4", ID: 4},
			},
			expectedStrSize:     254,
			expectedFileSizeOne: 262,
			expectedFileSizeTwo: 518,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmpFile, m := makeTestMap(t)

			require.NoError(t, m.Set("k1", tc.value))

			valueHolder, found := m.memoryMap.data["k1"]
			assert.True(t, found)
			value := valueHolder.value
			assert.Lenf(t, value, tc.expectedStrSize, "unexpected encoded value size: %s", value)

			require.NoError(t, m.Commit())
			verifyFileSize(t, tmpFile, tc.expectedFileSizeOne)

			require.NoError(t, m.Set("k2", tc.value))
			require.NoError(t, m.Commit())
			verifyFileSize(t, tmpFile, tc.expectedFileSizeTwo)
		})
	}

	t.Run("mixed_fields", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)

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

		require.NoError(t, SetMapValues(m, values))

		require.NoError(t, m.Commit())
		verifyFileSize(t, tmpFile, 669)
	})

	t.Run("two_struct_slices_size", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)

		sliceA := []TestNamedStruct{
			{Value: "a0", ID: 1, Bool: true},
			{Value: "a1", ID: 2},
			{Value: "a2", ID: 3, Float: 1.5},
		}
		sliceB := []TestNamedStruct{
			{Value: "b0", ID: 100},
			{Value: "b1", ID: 200, Bool: true},
		}
		require.NoError(t, m.Set("k:sliceA", sliceA))
		require.NoError(t, m.Set("k:sliceB", sliceB))
		require.NoError(t, m.Commit())

		verifyFileSize(t, tmpFile, 381)
	})

	t.Run("mixed_field_types", func(t *testing.T) {
		tmpFile, m := makeTestMap(t)

		values := map[string]TestAnyStruct{
			"int": {Value: "int", Any: 1},
			"str": {Value: "str", Any: "str"},
			"nil": {Value: "nil", Any: nil},
		}

		require.NoError(t, SetMapValues(m, values))

		require.NoError(t, m.Commit())
		verifyFileSize(t, tmpFile, 114)
	})
}

func verifyFileSize(t *testing.T, fileStr string, expectedSize int64) {
	t.Helper()

	file, err := os.Open(fileStr)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()
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
	_, m := makeTestMap(t)

	for _, k := range []string{"foo1", "bar1", "foo2", "bar2"} {
		require.NoError(t, m.Set(k, k))

		assert.True(t, m.ContainsKey(k))
		assert.False(t, m.ContainsKey("-"+k))
	}
}

func TestKeyValueCSV_KeySet(t *testing.T) {
	t.Parallel()
	_, m := makeTestMap(t)

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
	_, m := makeTestMap(t)

	key := "keyDelete"
	value := "testValue"
	require.NoError(t, m.Set(key, value))

	m.Delete(key)

	verifyEmpty(t, m, key)
}

func TestKeyValueCSV_DeleteAll(t *testing.T) {
	t.Parallel()
	_, m := makeTestMap(t)

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
		{name: "zero", value: 0, overflow: false},
		{name: "small", value: 123.456, overflow: false},
		{name: "max_float32", value: math.MaxFloat32, overflow: false},
		{name: "neg_max_float32", value: -math.MaxFloat32, overflow: false},
		{name: "over_pos", value: math.MaxFloat32 * 2, overflow: true},
		{name: "over_neg", value: -math.MaxFloat32 * 2, overflow: true},
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
		lines := strings.Split(encodedLines, "\n")
		records := make([][]string, 1, len(lines)+1)
		records[0] = []string{currentFileVersion}
		for _, line := range lines {
			lineSlice := strings.Split(line, ",")
			if !strings.HasPrefix(line, "0,") { // only three values expected
				recombinedLine := make([]string, 1, 3)
				recombinedLine[0] = lineSlice[0]
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

	t.Run("validation_nil_map", func(t *testing.T) {
		err := SetMapValues(nil, map[string]string{"key": "value"})
		require.Error(t, err)

		var validationErr *ValidationError
		require.ErrorAs(t, err, &validationErr)
		assert.Contains(t, validationErr.Message, "nil MutableFFMap")
	})

	t.Run("validation_nil_key_provider", func(t *testing.T) {
		_, m := makeTestMap(t)

		err := SetSliceValues(m, []string{"value"}, nil)
		require.Error(t, err)

		var validationErr *ValidationError
		require.ErrorAs(t, err, &validationErr)
		assert.Contains(t, validationErr.Message, "nil keyProvider function")
	})

	t.Run("validation_invalid_file_format", func(t *testing.T) {
		// Create a file with invalid format
		tmpFile, err := os.CreateTemp("", "invalid.*.csv")
		require.NoError(t, err)

		// Write invalid header
		_, err = tmpFile.WriteString("invalid_version\ngarbage_data\n")
		require.NoError(t, err)
		require.NoError(t, tmpFile.Close())

		_, err = OpenCSV(tmpFile.Name())
		require.Error(t, err)

		var validationErr *ValidationError
		require.ErrorAs(t, err, &validationErr)
		assert.Contains(t, validationErr.Message, "invalid header line")
	})

	t.Run("encoding_error_propagation", func(t *testing.T) {
		_, m := makeTestMap(t)

		// Test that encoding errors from the underlying memory map are propagated
		err := m.Set("nil_key", nil)
		require.Error(t, err)

		var encodingErr *EncodingError
		require.ErrorAs(t, err, &encodingErr)
		assert.Equal(t, "nil_key", encodingErr.Key)
		assert.Contains(t, encodingErr.Message, "cannot encode nil value")
	})

	t.Run("type_mismatch_propagation", func(t *testing.T) {
		_, m := makeTestMap(t)

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

	t.Run("distinct_error_types", func(t *testing.T) {
		_, m := makeTestMap(t)

		// Test encoding error
		encodingErr := m.Set("key1", nil)
		var encErr *EncodingError
		var typeErr1 *TypeMismatchError
		var valErr1 *ValidationError

		require.ErrorAs(t, encodingErr, &encErr)
		assert.NotErrorAs(t, encodingErr, &typeErr1)
		assert.NotErrorAs(t, encodingErr, &valErr1)

		// Test validation error
		validationErr := SetMapValues(nil, map[string]string{"key": "value"})
		var encErr2 *EncodingError
		var typeErr2 *TypeMismatchError
		var valErr2 *ValidationError

		require.ErrorAs(t, validationErr, &valErr2)
		assert.NotErrorAs(t, validationErr, &encErr2)
		assert.NotErrorAs(t, validationErr, &typeErr2)
	})
}

func TestStructFieldUnion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		items          []map[string]interface{}
		expectedFields []string
		expectedKinds  map[string]reflect.Kind
	}{
		{
			name:           "empty_input",
			items:          nil,
			expectedFields: nil,
			expectedKinds:  map[string]reflect.Kind{},
		},
		{
			name: "single_item",
			items: []map[string]interface{}{
				{"a": float64(1), "b": "x"},
			},
			expectedFields: []string{"a", "b"},
			expectedKinds: map[string]reflect.Kind{
				"a": reflect.Float64,
				"b": reflect.String,
			},
		},
		{
			name: "multi_item_disjoint_fields",
			items: []map[string]interface{}{
				{"b": "x", "a": float64(1)},
				{"c": true},
			},
			expectedFields: []string{"a", "b", "c"},
			expectedKinds: map[string]reflect.Kind{
				"a": reflect.Float64,
				"b": reflect.String,
				"c": reflect.Bool,
			},
		},
		{
			name: "kind_mismatch_promotes_to_ptr",
			items: []map[string]interface{}{
				{"a": float64(1)},
				{"a": "x"},
			},
			expectedFields: []string{"a"},
			expectedKinds: map[string]reflect.Kind{
				"a": reflect.Ptr,
			},
		},
		{
			name: "sorted_output",
			items: []map[string]interface{}{
				{"z": float64(1), "m": float64(2), "a": float64(3)},
			},
			expectedFields: []string{"a", "m", "z"},
			expectedKinds: map[string]reflect.Kind{
				"a": reflect.Float64,
				"m": reflect.Float64,
				"z": reflect.Float64,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields, kinds := structFieldUnion(tt.items)

			assert.Equal(t, tt.expectedFields, fields)
			assert.True(t, slices.IsSorted(fields))
			assert.Equal(t, tt.expectedKinds, kinds)
		})
	}
}

func TestProjectStructValues(t *testing.T) {
	t.Parallel()

	sortedFields := []string{"a", "b", "c"}
	kinds := map[string]reflect.Kind{
		"a": reflect.Float64,
		"b": reflect.String,
		"c": reflect.Bool,
	}

	tests := []struct {
		name         string
		item         map[string]interface{}
		fields       []string
		fieldKinds   map[string]reflect.Kind
		expectValues []interface{}
	}{
		{
			name:         "all_fields_present",
			item:         map[string]interface{}{"a": float64(1), "b": "x", "c": true},
			fields:       sortedFields,
			fieldKinds:   kinds,
			expectValues: []interface{}{float64(1), "x", true},
		},
		{
			name:         "missing_field_uses_zero",
			item:         map[string]interface{}{"a": float64(1)},
			fields:       sortedFields,
			fieldKinds:   kinds,
			expectValues: []interface{}{float64(1), "", false},
		},
		{
			name:   "ptr_kind_emits_nil",
			item:   map[string]interface{}{},
			fields: []string{"a"},
			fieldKinds: map[string]reflect.Kind{
				"a": reflect.Ptr,
			},
			expectValues: []interface{}{nil},
		},
		{
			name:         "mixed_present_and_missing",
			item:         map[string]interface{}{"b": "x"},
			fields:       sortedFields,
			fieldKinds:   kinds,
			expectValues: []interface{}{float64(0), "x", false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := projectStructValues(tt.item, tt.fields, tt.fieldKinds)
			assert.Equal(t, tt.expectValues, got)
		})
	}
}
