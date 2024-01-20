package ffmap

// OpenCSV will create or read an existing CSV map file.
func OpenCSV(filename string) (*KeyValueCSV, error) {
	db := &KeyValueCSV{
		filename: filename,
		data:     make(map[string]dataItem),
	}
	if err := db.loadFromDisk(); err != nil {
		return nil, err
	} else {
		return db, nil
	}
}

// OpenReadOnlyCSV will read a CSV map file, providing a read only view of the data.
func OpenReadOnlyCSV(filename string) (FlatFileMap, error) {
	return OpenCSV(filename)
}

type FlatFileMap interface {
	// Size reports how many entries are stored in the map.
	Size() int
	// Get will set the value for the given key.  The returned bool indicates if the value was found and matches the
	// type, check the error for possible parsing or type errors.
	Get(key string, value interface{}) (bool, error)
	// ContainsKey will return true if the map has an associated value with the provided key
	ContainsKey(key string) bool
	// KeySet will return all the keys stored within the map.
	KeySet() []string
}

type WritableFlatFileMap interface {
	FlatFileMap
	// Set will set the provided value into the map, when retrieved the same type must be used.  If a value already
	// exists, it will be replaced with the new value.
	Set(key string, value interface{}) error
	// Delete will remove the key from the map (if present).
	Delete(key string)
	// Commit will update the disk representation to match the in-memory state.  If this is not invoked the disk will
	// never be updated.  This must not be called concurrently, and may be slow as the file format is optimized.
	Commit() error
}
