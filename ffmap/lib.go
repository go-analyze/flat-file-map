package ffmap

import (
	"github.com/go-analyze/bulk"
)

// sliceUniqueUnion returns the union of all elements from the provided slices, removing duplicates.
func sliceUniqueUnion[T comparable](slice [][]T) []T {
	switch len(slice) {
	case 0:
		return nil
	case 1:
		return slice[0] // assumed no duplications within a single slice
	}

	return bulk.MapKeysSlice(bulk.SliceToSet(slice...))
}
