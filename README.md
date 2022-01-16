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

Has the normal file opening functions:

```go
func Create(name string) (*File, error)
func Open(name string) (*File, error)
func OpenFile(name string, flat int, perm fs.FileMode) (*File, error)
```

The resulting file works a lot like a standard `os.File`, but with an 
automatically maintained pool of buffered page. The default size of all
buffered pages will not exceed 8 MB. 

For custom size buffers use the `OpenFileSize`.

**All operations are thread-safe.**

Other important functions:

```go
func (*File) WriteAt([]byte, int64) (int, error) // random writes
func (*File) ReadAt([]byte, int64) (int, error)  // random reads
func (*File) Read([]byte) (int, error)           // sequential reads
func (*File) Write([]byte) (int, error)          // sequential writes
func (*File) Flush() error                       // flush buffer data
func (*File) Sync() error                        // flush and sync data to stable storage
func (*File) Close() error                       // close the file
func (*File) Clone() error                       // create a shallow copy
func (*File) Truncate(int64) error               // resize the file
func (*File) Stat() os.FileInfo                  // file size and other info
```

The `Clone` function will create a shallow copy of `File`, which shares the same
in memory pages as all other clones. 

The `Close` function will always flush and sync your data prior to returning.

## Contact

Josh Baker [@tidwall](http://twitter.com/tidwall)

## License

Source code is available under the MIT [License](/LICENSE).
