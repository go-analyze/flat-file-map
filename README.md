# flat-file-map
A Golang library for storing key-value data in a flat file.

## Flat File Advantage

There are several reasons you may want to persist data in a simple flat file:
* Accessibility with common text editors and various tools.
* Line-delineated changes for each record update, facilitating easy examination and efficent storage with diff-based workflows (e.g., Git).

## Not a Database

The `flat-file-map` module is focused on providing a specialized file format to be used in niche workflows. Consequently, it makes several trade-offs:
* All data must fit into memory.
* Concurrent access by multiple Go routines or processes is not supported.
* File loading and writing may be slower compared to other database solutions.

