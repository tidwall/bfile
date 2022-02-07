package bfile

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func makeRandom(N int) []byte {
	data := make([]byte, N)
	rand.Read(data)
	return data
}

func randSize() int64 {
	var bsize int64
	switch rand.Int() % 4 {
	case 0:
		bsize = int64(rand.Int() % 10)
	case 1:
		bsize = int64(rand.Int() % 100)
	case 2:
		bsize = int64(rand.Int() % 1000)
	case 3:
		bsize = int64(rand.Int() % 10000)
	}
	return bsize
}

func TestFile(t *testing.T) {
	// Create a random 100+ MB file and perform random sized buffered write of
	// random data.
	defer os.Remove("test.dat")
	data := makeRandom(123456789)
	f1, err := os.Create("test.dat")
	if err != nil {
		t.Fatal(err)
	}
	p := NewPager(f1)
	bf := p.Stream(0)
	fsize := int64(len(data))
	var off int64
	for off < fsize {
		bsize := randSize()
		if off+bsize > fsize {
			bsize = fsize - off
		}
		n, err := bf.Write(data[off : off+bsize])
		if err != nil {
			t.Fatal(err)
		}
		if int64(n) != bsize {
			t.Fatalf("expected %d got %d", bsize, n)
		}
		off += bsize
	}
	if err := p.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := f1.Close(); err != nil {
		t.Fatal(err)
	}
	f1, err = os.Open("test.dat")
	if err != nil {
		t.Fatal(err)
	}
	fi, err := f1.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != int64(len(data)) {
		t.Fatalf("expected %d got %d", int64(len(data)), fi.Size())
	}
	p = NewPager(f1)
	data2, err := io.ReadAll(p.Stream(0))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data2, data) {
		t.Fatal("mismatch")
	}
	data2, err = io.ReadAll(p.Stream(0))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data2, data) {
		t.Fatal("mismatch")
	}
	data2, err = os.ReadFile("test.dat")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data2, data) {
		t.Fatal("mismatch")
	}
	if err := f1.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(p.Stream(0))
	if err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("expected %v got %v", os.ErrClosed, err)
	}

}

func TestThreads(t *testing.T) {
	defer os.Remove("test.dat")
	fsize := int64(10_000_000)
	f, err := os.Create("test.dat")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	bf := NewPager(f)
	var wg sync.WaitGroup
	nprocs := 100
	for i := 0; i < nprocs; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				data := makeRandom(int(randSize()))
				off := int64(rand.Int()) & fsize
				var n int
				var err error
				if rand.Int()%2 == 0 {
					n, err = bf.ReadAt(data, off)
				} else {
					n, err = bf.WriteAt(data, off)
				}
				_, _ = n, err
			}
		}(i)
	}
	wg.Wait()
	if err := bf.Flush(); err != nil {
		t.Fatal(err)
	}
	f.Close()
}

func TestWritePerf(t *testing.T) {
	defer os.Remove("hello.dat")
	N := 1 * 1024 * 1024 * 1024 / 2
	P := 8192  // page size
	B := P * 2 // buffer size
	M := 256
	bufs := make([]byte, P*M)
	rand.Read(bufs)
	t.Run("bufio", func(t *testing.T) {
		os.Remove("hello.dat")
		start := time.Now()
		f, err := os.Create("hello.dat")
		if err != nil {
			t.Fatal(f)
		}
		if err := f.Truncate(int64(N)); err != nil {
			t.Fatal(err)
		}
		w := bufio.NewWriterSize(f, B)
		var n int
		for j := 0; ; j++ {
			buf := bufs[P*(j&(M-1)) : P*((j&(M-1))+1)]
			end := len(buf)
			if n+end > N {
				end = N - n
			}
			if _, err := w.Write(buf[:end]); err != nil {
				t.Fatal(err)
			}
			n += end
			if n >= N {
				break
			}
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(start)
		fmt.Printf("%s %d MB in %d ms, %d MB/s\n",
			t.Name(),
			N/1024/1024, elapsed.Milliseconds(),
			int(float64(N)/elapsed.Seconds()/1024/1024),
		)
	})
	t.Run("bfile", func(t *testing.T) {
		os.Remove("hello.dat")
		start := time.Now()
		f, err := os.Create("hello.dat")
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Truncate(int64(N)); err != nil {
			t.Fatal(err)
		}
		p := NewPagerSize(f, P, B)
		w := p.Stream(0)
		var n int
		for j := 0; ; j++ {
			buf := bufs[P*(j&(M-1)) : P*((j&(M-1))+1)]
			end := len(buf)
			if n+end > N {
				end = N - n
			}
			if _, err := w.Write(buf[:end]); err != nil {
				t.Fatal(err)
			}
			n += end
			if n >= N {
				break
			}
		}
		if err := p.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}

		elapsed := time.Since(start)
		fmt.Printf("%s %d MB in %d ms, %d MB/s\n",
			t.Name(),
			N/1024/1024, elapsed.Milliseconds(),
			int(float64(N)/elapsed.Seconds()/1024/1024),
		)
	})
}
