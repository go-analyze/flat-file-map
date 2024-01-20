# go-analyze/flat-file-map
A Golang library for storing key-value data in a flat file.

## Flat File Advantage

There are several reasons you may want to persist data in a simple flat file:
* Accessibility with common text editors and various tools.
* Line-delineated changes for each record update, facilitating easy examination and efficient storage with diff-based workflows (e.g., Git).

## Not a Database

The `flat-file-map` module is focused on providing a specialized file format to be used in niche workflows. Consequently, it makes several trade-offs:
* All data must fit into memory.
* Concurrent access of the file (for example by multiple processes) is not supported.
* File loading and writing may be slower compared to other database solutions.

## Basic Usage

Here's a example of how to use the `flat-file-map`:

```go
package main

import (
    "fmt"
    "log"
    "github.com/go-analyze/flat-file-map/ffmap"
)

func main() {
    // Open or create a CSV map file.
    db, err := ffmap.OpenCSV("example.csv")
	// ...

    // Set a value in the map.
    err = db.Set("string key", "value1")
	// ...

    // Get a value from the map, values can be anything including primitives, strings, maps, and complex structs
    var value map[string]string
    found, err := db.Get("map key", &value)
	// ...

    // Commit the current state to the file on disk
    err = db.Commit()
	// ...
}
```

## Project Maturity

While this module is dedicated to a small and straightforward purpose, it is in its early stages of development. This means that there may be unexpected changes to the API in the future. Such changes could affect the file format, potentially leading to compatibility issues with file formats currently in use.

The release of version `1.0` will signify our confidence in the module's implementation. From this version onward, we commit to ensuring file format compatibility and API stability.

