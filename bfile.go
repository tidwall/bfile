package bfile

import (
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
)

const (
	defaultPageSize   = 4096     // all pages are this size
	defaultBufferSize = 0x800000 // default buffer size, 8 MB
	minPages          = 4        // minimum total pages per file
	pagesPerShard     = 32       // ideal number of pages per shard
	maxShards         = 128      // maximum number of shards per file
)

// Pager is a page buffer that is backed by an os.File
type Pager struct {
	file   *os.File
	pgsize int64
	pgmax  int64
	mu     sync.RWMutex
	size   int64
	shards []shard
}

type page struct {
	num  int64
	prev *page
	next *page
	data []byte
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

// NewPager returns a new Pager that is backed by the provided file.
func NewPager(file *os.File) *Pager {
	return NewPagerSize(file, 0, 0)
}

// NewPagerSize returns a new Pager with a custom page size and buffer size.
// The bufferSize is the maximum amount of memory dedicated to individual
// pages. Setting pageSize and bufferSize to zero will use their defaults,
// which are 4096 and 8 MB respectively. Custom values are rounded up to the
// nearest power of 2.
func NewPagerSize(file *os.File, pageSize, bufferSize int) *Pager {
	if pageSize <= 0 {
		pageSize = defaultPageSize
	} else if pageSize&4095 != 0 {
		// must be a power of two
		x := 1
		for x < pageSize {
			x *= 2
		}
		pageSize = x
	}

	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	} else if bufferSize < pageSize {
		bufferSize = pageSize
	}

	f := new(Pager)
	f.file = file
	f.size = -1
	f.pgsize = int64(pageSize)

	// calculate the max number of pages across all shards
	pgmax := int64(bufferSize) / f.pgsize
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
	return f
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

func (f *Pager) write(p *page) error {
	off := p.num * f.pgsize
	end := int64(f.pgsize)
	if off+end > f.size {
		end = f.size - off
	}
	_, err := f.file.WriteAt(p.data[:end], off)
	if err != nil {
		return err
	}
	return nil
}

func (f *Pager) read(p *page) error {
	_, err := f.file.ReadAt(p.data, p.num*f.pgsize)
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}

// incrSize initializes and increases file size to be at least equal to end¸
// This operation expects that the pager is currently in a RLock state.
func (f *Pager) incrSize(end int64) error {
	f.mu.RUnlock()
	f.mu.Lock()
	defer func() {
		f.mu.Unlock()
		f.mu.RLock()
	}()
	if f.size == -1 {
		fi, err := f.file.Stat()
		if err != nil {
			return nil
		}
		f.size = fi.Size()
	}
	if end > f.size {
		f.size = end
	}
	return nil
}

func (f *Pager) io(b []byte, off int64, write bool) (n int, err error) {
	eof := false
	start, end := off, off+int64(len(b))
	if start < 0 {
		return 0, fmt.Errorf("negative offset")
	}

	// Check the upper bounds of the input to the known file size.
	// Increase the file size if needed.
	f.mu.RLock()
	defer f.mu.RUnlock()
	if end > f.size {
		if err := f.incrSize(end); err != nil {
			return 0, err
		}
		if !write {
			end = f.size
			if end-start < 0 {
				end = start
			}
			eof = true
			b = b[:end-start]
		}
	}

	// Perform the page I/O.
	var total int
	for len(b) > 0 {
		pnum := off / f.pgsize
		pstart := off & (f.pgsize - 1)
		pend := pstart + int64(len(b))
		if pend > f.pgsize {
			pend = f.pgsize
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

func (f *Pager) pio(b []byte, pnum, pstart, pend int64, write bool,
) (n int, err error) {
	s := &f.shards[pnum&(int64(len(f.shards)-1))]
	s.mu.Lock()
	defer s.mu.Unlock()
	s.init()
	p := s.pages[pnum]
	if p == nil {
		// Page does not exist in memory.
		// Acquire a new one.
		if int64(len(s.pages)) == f.pgmax {
			// The buffer is at capacity.
			// Evict lru page and hang on to it.
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
			// Clear the previous page memory for partial page writes for
			// pages that are being partially written to.
			if write && pend-pstart < f.pgsize {
				for i := range p.data {
					p.data[i] = 0
				}
			}
		} else {
			// Allocate an entirely new page.
			p = new(page)
			p.data = make([]byte, f.pgsize)
		}
		p.num = pnum
		// Read contents of page from file for all read operations, and
		// partial write operations. Ignore for full page writes.
		if !write || pend-pstart < f.pgsize {
			if err := f.read(p); err != nil {
				return 0, err
			}
		}
		// Add the newly acquired page to the list.
		s.pages[p.num] = p
		s.push(p)
	} else {
		// Bump the page to the front of the list.
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

// Flush writes any unwritten buffered data to the underlying file.
func (f *Pager) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.shards {
		for pnum := range f.shards[i].dirty {
			if err := f.write(f.shards[i].pages[pnum]); err != nil {
				return err
			}
			delete(f.shards[i].dirty, pnum)
		}
	}
	return nil
}

// The byte offset off and len(b) must fall within the range of the size of
// the underlying file from Open or Create, otherwise an error is returned.
// ReadAt reads len(b) bytes from the File starting at byte offset off.
// It returns the number of bytes read and the error, if any.
// ReadAt returns a non-nil error when n < len(b).
func (f *Pager) ReadAt(b []byte, off int64) (n int, err error) {
	return f.io(b, off, false)
}

// WriteAt writes len(b) bytes to the File starting at byte offset off.
// It returns the number of bytes written and an error, if any.
// WriteAt returns a non-nil error when n != len(b).
func (f *Pager) WriteAt(b []byte, off int64) (n int, err error) {
	return f.io(b, off, true)
}

// Stream is a sequential read/writer that is backed by a Pager.
type Stream struct {
	pager *Pager
	off   int64
}

// Stream returns a new Stream for sequentially reading and writing data
// that is backed by a Pager.
func (f *Pager) Stream(off int64) *Stream {
	return &Stream{pager: f, off: off}
}

func (s *Stream) Write(p []byte) (n int, err error) {
	n, err = s.pager.WriteAt(p, atomic.LoadInt64(&s.off))
	atomic.AddInt64(&s.off, int64(n))
	return n, err
}

func (s *Stream) Read(p []byte) (n int, err error) {
	n, err = s.pager.ReadAt(p, atomic.LoadInt64(&s.off))
	atomic.AddInt64(&s.off, int64(n))
	return n, err
}

func (s *Stream) Flush() error {
	return s.pager.Flush()
}
