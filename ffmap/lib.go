package ffmap

import (
	"github.com/go-analyze/bulk"
)

func mapKeys[K comparable, V any](m map[K]V) []K {
	result := make([]K, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	return result
}

func sliceUniqueUnion[T comparable](slice [][]T) []T {
	switch len(slice) {
	case 0:
		return nil
	case 1:
		return slice[0] // assumed no duplications within a single slice
	}

	return mapKeys(bulk.SliceToSet(slice...))
}
