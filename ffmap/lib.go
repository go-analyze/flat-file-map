package ffmap

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
		return slice[0]
	}

	uniqMap := make(map[T]bool)
	for _, s := range slice {
		for _, v := range s {
			uniqMap[v] = true
		}
	}
	return mapKeys(uniqMap)
}
