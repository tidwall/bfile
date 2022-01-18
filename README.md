# bio

[![GoDoc](https://godoc.org/github.com/tidwall/bio?status.svg)](https://godoc.org/github.com/tidwall/bio)

A buffer pool file I/O library for Go.

The purpose of the library is to provide I/O mechanisms for copying data to and from a buffer in user space and maintain complete control over how and when it transfers pages. 

This is an alternative to using `mmap` on large DBMS like files.

## Install

```
go get github.com/tidwall/bio
```

## Usage

Use the `NewPager` function to create a new `Pager`, which includes three
functions: `ReadAt`, `WriteAt`, and `Flush`.
The Pager reading and writing works like an `os.File`, but with an 
automatically maintained pool of buffered pages.

```go
// Open a file for read/write
f, err := os.OpenFile("bigfile.dat", os.O_RDWR, 0)

// Create a new Pager for accessing the data in opened file.
p := bio.NewPager(f)

// Read some data at a specific offset.
data := make([]byte, 50)
n, err := p.ReadAt(data, 827364)

// Write some data at the same offset.
n, err := p.WriteAt([]byte("hello"), c)

// Flush unwritten data to the underlying os.File
err = p.Flush()
```

There's also a `Stream` object for sequentially reading and writing data, which
includes the three functions: `Read`, `Write`, and `Flush`.

```go

// Create a new Pager
p := bio.NewPager(f)

// Create a Stream that is backed by the Pager starting at a specific offset
s := p.Stream(827364)

// Read 50 bytes data at the offset 827364.
data := make([]byte, 50)
n, err := s.Read(data)

// Write the string "hello" at 827414, which is after the previous read.
n, err := s.Write([]byte("hello"))

// Flush unwritten data to the underlying os.File
err = p.Flush()
```

The default page size is 4096 and the default size of all buffered pages will 
not exceed 8 MB.
These defaults can be changed by using the `NewPagerSize` function.

**All operations are thread-safe.**

## Contact

Josh Baker [@tidwall](http://twitter.com/tidwall)

## License

Source code is available under the MIT [License](/LICENSE).
