# bfile

[![GoDoc](https://godoc.org/github.com/tidwall/bfile?status.svg)](https://godoc.org/github.com/tidwall/bfile)

A buffer pool file I/O library for Go.

The purpose of the library is to provide I/O mechanisms for copying data to and from a buffer in user space and maintain complete control over how and when it transfers pages. 

This is an alternative to using `mmap` on large DBMS like files.

## Install

```
go get github.com/tidwall/bfile
```

## Usage

There are two functions to open or create a file:

```go
func Create(path string, fileSize int64, bufferSize int64) (*File, error)
func Open(path string, bufferSize int64) (*File, error)
```

The `Create` function will create or replace a file for reading and writing and resize the file to `fileSize`.  
The `Open` function will open an existing file for reading and writing.  
The `bufferSize` param tells the underlying file to use no more than that much memory for maintaining buffers.

The resulting file works a lot like a standard `os.File` and includes most of
the same methods.

The main difference is that the size of the file is capped when `Open` or `Create` is called.
Reading or writing beyond the file size will return an `io.EOF` error.

**All operations are thread-safe.**

Other important methods:

```go
func (*File) WriteAt([]byte, int64) (int, error) // random writes
func (*File) ReadAt([]byte, int64) (int, error)  // random reads
func (*File) Read([]byte) (int, error)           // sequential reads
func (*File) Write([]byte) (int, error)          // sequential writes
func (*File) Flush() error                       // flush buffer data
func (*File) Sync() error                        // flush and sync data to stable storage
func (*File) Close() error                       // close the file
func (*File) Clone() error                       // create a shallow copy
```

The `Clone` method will create a shallow copy of `File`, which shares the same
in memory pages as all other clones. 

## Contact

Josh Baker [@tidwall](http://twitter.com/tidwall)

## License

Source code is available under the MIT [License](/LICENSE).
