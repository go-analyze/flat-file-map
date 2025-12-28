# go-analyze/flat-file-map

[![license](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/go-analyze/flat-file-map/blob/master/LICENSE)
[![Tests - Main Push](https://github.com/go-analyze/flat-file-map/actions/workflows/tests-main.yml/badge.svg)](https://github.com/go-analyze/flat-file-map/actions/workflows/tests-main.yml)

A Golang library for storing key-value data in a flat file. Allowing easy data storage within tools optimized for text file storage (like git).

## Flat File Advantage

There are several reasons you may want to persist data in a simple flat file:
* Accessibility with common text editors and various tools.
* Line-delineated changes for each record update, facilitating easy examination and efficient storage with diff-based workflows (e.g., Git).

## Not a Database

The `flat-file-map` module is focused on providing a specialized file format to be used in niche workflows. Consequently, it makes several trade-offs:
* All data must fit into memory.
* Concurrent access of the file (for example by multiple processes) is not supported.
* Value storing and retrieval, as well as file loading and writing is slower compared to other database solutions.

TL:DR; this project makes the file size and format the priority in order to ensure it can be easily stored in systems which are well adapted for flat text based files.

## Basic Usage

Here's a example of how to use the `flat-file-map`:

```go
package main

import (
    "github.com/go-analyze/flat-file-map/ffmap"
)

func main() {
    // Open or create a CSV map file.
    db, err := ffmap.OpenCSV("example.csv")
    // ... err check for IO error

    // Set a value in the map.
    err = db.Set("string key", "value1")
    // ... err check for value encoding error

    // Get a value from the map, values can be anything including primitives, strings, maps, and complex structs
    var value map[string]string
    found, err := db.Get("map key", &value)
    // ... found check for present and error check for value decoding error

    // If working with the same value type, a TypedFFMap can make it easier
    typedMap := ffmap.NewTypedFFMap[string](db)

    // Typed values can now be retrieved easier
    stringResult, ok := tfm.Get("string key")
    // ...

    // Commit the current state to the file on disk
    err = db.Commit()
    // ... err check for IO error
}
```

## Project Maturity

This module is dedicated to a small and straightforward purpose, and because of that we are already finding stability with our Go API. It is still possible we may need to have future changes to the Go API or file format, potentially leading to compatibility issues with file formats currently in use. However we believe we have a solid foundation established that will be easy to support long term.

The release of version `1.0` will signify our confidence in the module's implementation. From this version onward, we commit to ensuring file format compatibility and API stability.

