package bfile

import (
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"sync"
	"sync/atomic"
)

const (
	pageSize          = 8192     // all pages are this size
	minPages          = 4        // minimum total pages per file
	pagesPerShard     = 32       // ideal number of pages per shard
	maxShards         = 128      // maximum number of shards per file
	defaultBufferSize = 0x800000 // default max buffer size, 8 MB
)

type File struct {
	bfile *bfile
	off   int64
}

type page struct {
	num  int64
	prev *page
	next *page
	data []byte
}

type bfile struct {
	size   int64
	file   *os.File
	pgmax  int64
	mu     sync.RWMutex
	closed bool
	shards []shard
}

type shard struct {
	mu    sync.Mutex
	pages map[int64]*page
	dirty map[int64]bool
	head  *page
	tail  *page
}

func (s *shard) push(p *page) {
	s.head.next.prev = p
	p.next = s.head.next
	p.prev = s.head
	s.head.next = p
}

func (s *shard) pop(p *page) {
	p.prev.next = p.next
	p.next.prev = p.prev
}

func (s *shard) bump(p *page) {
	s.pop(p)
	s.push(p)
}

func Create(name string) (*File, error) {
	return OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

func Open(name string) (*File, error) {
	return OpenFile(name, os.O_RDONLY, 0)
}

// OpenFile works like the builtin os.OpenFile.
// Uses a default buffer size of 8 MB.
// To choose your own buffer size use OpenFileSize
func OpenFile(name string, flag int, perm fs.FileMode) (*File, error) {
	return OpenFileSize(name, flag, perm, 0)
}

// OpenFile works like the builtin os.OpenFile.
// Uses a default buffer size of 8 MB.
// To choose your own buffer size use OpenFileSize
func OpenFileSize(name string, flag int, perm fs.FileMode, bufferSize int64) (*File, error) {
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	fi, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	return newFile(file, fi.Size(), bufferSize)
}

func newFile(file *os.File, size, bufferSize int64) (*File, error) {
	f := new(bfile)
	f.file = file
	f.size = size

	// calculate the max number of pages across all shards
	pgmax := bufferSize / pageSize
	if pgmax < minPages {
		pgmax = minPages
	}

	// calculate how many shards are needed, power of 2
	nshards := int64(math.Ceil(float64(pgmax) / float64(pagesPerShard)))
	if nshards > maxShards {
		nshards = maxShards
	}
	x := int64(1)
	for x < nshards {
		x *= 2
	}
	nshards = x

	// calculate the max number of pages per shard
	f.pgmax = int64(math.Floor(float64(pgmax) / float64(nshards)))
	f.shards = make([]shard, nshards)
	return &File{bfile: f}, nil
}

func (s *shard) init() {
	if s.pages == nil {
		s.pages = make(map[int64]*page)
		s.dirty = make(map[int64]bool)
		s.head = new(page)
		s.tail = new(page)
		s.head.next = s.tail
		s.tail.prev = s.head
	}
}

func (f *bfile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return os.ErrClosed
	}
	errs := [...]error{
		f.flush(),
		f.file.Sync(),
		f.file.Close(),
	}
	f.closed = true
	for i := range errs {
		if errs[i] != nil {
			return errs[i]
		}
	}
	return nil
}

func (f *bfile) write(p *page) error {
	off := p.num * pageSize
	end := int64(pageSize)
	if off+end > f.size {
		end = f.size - off
	}
	_, err := f.file.WriteAt(p.data[:end], off)
	if err != nil {
		return err
	}
	return nil
}

func (f *bfile) read(p *page) error {
	_, err := f.file.ReadAt(p.data, p.num*pageSize)
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}

func (f *bfile) flush() error {
	var dirty []int64
	for i := range f.shards {
		dirty = dirty[:0]
		for pnum := range f.shards[i].dirty {
			dirty = append(dirty, pnum)
		}
		for _, pnum := range dirty {
			if err := f.write(f.shards[i].pages[pnum]); err != nil {
				return err
			}
			delete(f.shards[i].dirty, pnum)
		}
	}
	return nil
}

func (f *bfile) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return os.ErrClosed
	}
	return f.flush()
}

func (f *bfile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return os.ErrClosed
	}
	if err := f.flush(); err != nil {
		return err
	}
	return f.file.Sync()
}

func (f *bfile) ReadAt(b []byte, off int64) (n int, err error) {
	return f.io(b, off, false)
}

func (f *bfile) WriteAt(b []byte, off int64) (n int, err error) {
	return f.io(b, off, true)
}

func (f *bfile) io(b []byte, off int64, write bool) (n int, err error) {
	// validate input
	var eof bool
	start, end := off, off+int64(len(b))
	if start < 0 {
		return 0, fmt.Errorf("negative offset")
	} else if end > f.size {
		end = f.size
		if end-start < 0 {
			end = start
		}
		eof = true
	}
	b = b[:end-start]
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.closed {
		return 0, os.ErrClosed
	}
	// perform page i/o
	var total int
	for len(b) > 0 {
		pnum := off / pageSize
		pstart := off & (pageSize - 1)
		pend := pstart + int64(len(b))
		if pend > pageSize {
			pend = pageSize
		}
		n, err := f.pio(b[:pend-pstart], pnum, pstart, pend, write)
		if err != nil {
			return total, err
		}
		off += int64(n)
		total += n
		b = b[n:]
	}
	if eof {
		return total, io.EOF
	}
	return total, nil
}

func (f *bfile) pio(b []byte, pnum, pstart, pend int64, write bool,
) (n int, err error) {
	s := &f.shards[pnum&(int64(len(f.shards)-1))]
	s.mu.Lock()
	defer s.mu.Unlock()
	s.init()
	p := s.pages[pnum]
	if p == nil {
		// page does not exist. acquire a new one
		if int64(len(s.pages)) == f.pgmax {
			// at capacity. evict lru page and take it.
			p = s.tail.prev
			s.pop(p)
			delete(s.pages, p.num)
			if s.dirty[p.num] {
				// dirty page. flush it now
				if err := f.write(p); err != nil {
					return 0, err
				}
				delete(s.dirty, p.num)
			}
			// clear the previous page memory.
			// ignore for reads and full page writes.
			if write && pend-pstart < pageSize {
				for i := range p.data {
					p.data[i] = 0
				}
			}
		} else {
			// allocate a new page.
			p = new(page)
			p.data = make([]byte, pageSize)
		}
		p.num = pnum
		// read contents of page from file.
		// ignore for full page writes.
		if !write || pend-pstart < pageSize {
			if err := f.read(p); err != nil {
				return 0, err
			}
		}
		// add the newly acquired page to list.
		s.pages[p.num] = p
		s.push(p)
	} else {
		s.bump(p)
	}
	if write {
		copy(p.data[pstart:pend], b)
		s.dirty[pnum] = true
	} else {
		copy(b, p.data[pstart:pend])
	}
	return len(b), nil
}

// Close the file.
// This operation calls Sync prior to closing to ensure that the any buffered
// data has been fully written to stable storage.
func (f *File) Close() error {
	return f.bfile.Close()
}

// Size returns the size of the file
func (f *File) Size() int64 {
	return f.bfile.size
}

// WriteAt writes len(b) bytes to the File starting at byte offset off.
// It returns the number of bytes written and an error, if any.
// WriteAt returns a non-nil error when n != len(b).
//
// The byte offset off and len(b) must fall within the range of the size of
// the underlying file from Open or Create, otherwise an error is returned.
func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
	return f.bfile.WriteAt(b, off)
}

// ReadAt reads len(b) bytes from the File starting at byte offset off.
// It returns the number of bytes read and the error, if any.
// ReadAt returns a non-nil error when n < len(b).
//
// The byte offset off and len(b) must fall within the range of the size of
// the underlying file from Open or Create, otherwise an error is returned.
func (f *File) ReadAt(b []byte, off int64) (n int, err error) {
	return f.bfile.WriteAt(b, off)
}

// Stat returns the FileInfo structure describing file.
func (f *File) Stat() (os.FileInfo, error) {
	return f.bfile.file.Stat()
}

// Clone creates a shallow clone. All clones share the same pages in memory.
// All reads and writes are thread safe. Calling Close will also close the file
// for all shared clones.
func (f *File) Clone() *File {
	f2 := new(File)
	f2.bfile = f.bfile
	return f2
}

func (f *File) Write(p []byte) (n int, err error) {
	n, err = f.bfile.WriteAt(p, atomic.LoadInt64(&f.off))
	atomic.AddInt64(&f.off, int64(n))
	return n, err
}

func (f *File) WriteString(s string) (n int, err error) {
	return f.Write([]byte(s))
}

func (f *File) Read(p []byte) (n int, err error) {
	n, err = f.bfile.ReadAt(p, atomic.LoadInt64(&f.off))
	atomic.AddInt64(&f.off, int64(n))
	return n, err
}

// Flush writes any buffered data to the underlying os.File.
func (f *File) Flush() error {
	return f.bfile.Flush()
}

// Sync commits the current contents of the file to stable storage.
func (f *File) Sync() error {
	return f.bfile.Sync()
}

// Pages returns the number of in memory pages
func (f *File) Pages() int64 {
	f.bfile.mu.Lock()
	defer f.bfile.mu.Unlock()
	var n int64
	for i := range f.bfile.shards {
		n += int64(len(f.bfile.shards[i].pages))
	}
	return n
}

func (f *File) Chmod(mode os.FileMode) error {
	return f.bfile.file.Chmod(mode)
}

func (f *File) Chown(uid, gid int) error {
	return f.bfile.file.Chown(uid, gid)
}

// Name returns the name of the file as presented to Open.
func (f *File) Name() string {
	return f.bfile.file.Name()
}

// Truncate resizes the file
func (f *File) Truncate(size int64) error {
	f.bfile.mu.Lock()
	defer f.bfile.mu.Unlock()
	if err := f.bfile.file.Truncate(size); err != nil {
		return err
	}
	f.bfile.size = size
	return nil
}
