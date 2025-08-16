package ffmap

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsZeroValue(t *testing.T) {
	t.Parallel()

	type sampleStruct struct{ A int }
	var (
		intPtrNil  *int
		zero       = 0
		intPtrZero = &zero
		one        = 1
		intPtrOne  = &one
	)

	tests := []struct {
		name   string
		val    interface{}
		expect bool
	}{
		{name: "bool_true", val: true, expect: false},
		{name: "bool_false", val: false, expect: true},
		{name: "string_empty", val: "", expect: true},
		{name: "string_nonempty", val: "foo", expect: false},
		{name: "int_zero", val: 0, expect: true},
		{name: "int_nonzero", val: 5, expect: false},
		{name: "float_zero", val: 0.0, expect: true},
		{name: "float_nonzero", val: 1.1, expect: false},
		{name: "nil_slice", val: []int(nil), expect: true},
		{name: "empty_slice", val: []int{}, expect: false},
		{name: "slice_nonempty", val: []int{1}, expect: false},
		{name: "nil_map", val: map[string]int(nil), expect: true},
		{name: "empty_map", val: map[string]int{}, expect: false},
		{name: "map_nonempty", val: map[string]int{"a": 1}, expect: false},
		{name: "nil_ptr", val: intPtrNil, expect: true},
		{name: "ptr_zero", val: intPtrZero, expect: false},
		{name: "ptr_nonzero", val: intPtrOne, expect: false},
		{name: "array_zero_len", val: [0]int{}, expect: true},
		{name: "array_nonzero_len", val: [1]int{0}, expect: false},
		{name: "struct_zero", val: sampleStruct{}, expect: true},
		{name: "struct_nonzero", val: sampleStruct{A: 1}, expect: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isZeroValue(reflect.ValueOf(tt.val))
			assert.Equal(t, tt.expect, got)
		})
	}
}

func TestZeroValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		kind   reflect.Kind
		expect interface{}
	}{
		{name: "int", kind: reflect.Int, expect: 0},
		{name: "int8", kind: reflect.Int8, expect: 0},
		{name: "int16", kind: reflect.Int16, expect: 0},
		{name: "int32", kind: reflect.Int32, expect: 0},
		{name: "int64", kind: reflect.Int64, expect: 0},
		{name: "uint", kind: reflect.Uint, expect: uint64(0)},
		{name: "uint8", kind: reflect.Uint8, expect: uint64(0)},
		{name: "uint16", kind: reflect.Uint16, expect: uint64(0)},
		{name: "uint32", kind: reflect.Uint32, expect: uint64(0)},
		{name: "uint64", kind: reflect.Uint64, expect: uint64(0)},
		{name: "float32", kind: reflect.Float32, expect: 0.0},
		{name: "float64", kind: reflect.Float64, expect: 0.0},
		{name: "bool", kind: reflect.Bool, expect: false},
		{name: "string", kind: reflect.String, expect: ""},
		{name: "complex64", kind: reflect.Complex64, expect: nil},
		{name: "slice", kind: reflect.Slice, expect: nil},
		{name: "map", kind: reflect.Map, expect: nil},
		{name: "ptr", kind: reflect.Ptr, expect: nil},
		{name: "interface", kind: reflect.Interface, expect: nil},
		{name: "struct", kind: reflect.Struct, expect: nil},
		{name: "array", kind: reflect.Array, expect: nil},
		{name: "chan", kind: reflect.Chan, expect: nil},
		{name: "func", kind: reflect.Func, expect: nil},
		{name: "invalid", kind: reflect.Invalid, expect: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := zeroValue(tt.kind)
			assert.Equal(t, tt.expect, got)
		})
	}
}
