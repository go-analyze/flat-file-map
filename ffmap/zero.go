package ffmap

import (
	"encoding/json"
	"reflect"
)

// isZeroValue determines whether a reflect.Value is its type's zero value.
// For slices and maps, emptiness is defined by nil rather than Len()==0,
// so that non-nil empty slices/maps (which the user explicitly provided) are preserved.
func isZeroValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Bool:
		return !v.Bool()
	case reflect.String:
		return v.Len() == 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Slice, reflect.Map:
		return v.IsNil() // only consider nil to match prior pointer behavior
	case reflect.Array:
		return v.Len() == 0
	case reflect.Ptr, reflect.Interface:
		return v.IsNil()
	case reflect.Struct:
		// If the struct implements json.Marshaler, check its output.
		if v.CanInterface() {
			if marshaler, ok := v.Interface().(json.Marshaler); ok {
				if rv := reflect.ValueOf(marshaler); rv.Kind() == reflect.Ptr && rv.IsNil() {
					return true
				} else if jsonBytes, err := marshaler.MarshalJSON(); err == nil {
					s := string(jsonBytes)
					return s == `""` || s == `{}`
				}
			}
		}
		for i := 0; i < v.NumField(); i++ {
			if !isZeroValue(v.Field(i)) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// zeroValue returns the default or zero value for the provided reflected field.
// This is the counterpart to isZeroValue used for encoding needed zero values that were omitted.
func zeroValue(t reflect.Kind) interface{} {
	// not all types are possible after json.Unmarshal, but specified for completeness
	switch t {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return uint64(0)
	case reflect.Float32, reflect.Float64:
		return 0.0
	case reflect.Bool:
		return false
	case reflect.String:
		return ""
	default:
		return nil
	}
}
