package limits

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestReadAllAcceptsExactLimit(t *testing.T) {
	got, err := ReadAll(bytes.NewReader([]byte("abc")), 3)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "abc" {
		t.Fatalf("ReadAll = %q, want abc", got)
	}
}

func TestReadAllRejectsOversize(t *testing.T) {
	_, err := ReadAll(bytes.NewReader([]byte("abcd")), 3)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("ReadAll error = %v, want ErrTooLarge", err)
	}
}

func TestReaderZeroLengthReadDoesNotConsume(t *testing.T) {
	r := NewReader(bytes.NewReader([]byte("a")), 1)
	if n, err := r.Read(nil); n != 0 || err != nil {
		t.Fatalf("zero-length Read = (%d, %v), want (0, nil)", n, err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll after zero-length Read: %v", err)
	}
	if string(got) != "a" {
		t.Fatalf("ReadAll = %q, want a", got)
	}
}

func TestReaderRejectsNoProgress(t *testing.T) {
	r := NewReader(noProgressReader{}, 8)
	if n, err := r.Read(make([]byte, 1)); n != 0 || err != io.ErrNoProgress {
		t.Fatalf("Read = (%d, %v), want (0, %v)", n, err, io.ErrNoProgress)
	}
}

func TestReaderRejectsInvalidCount(t *testing.T) {
	for _, count := range []int{-1, 2} {
		r := NewReader(invalidCountReader{count: count}, 8)
		if n, err := r.Read(make([]byte, 1)); n != 0 || !errors.Is(err, ErrInvalidRead) {
			t.Fatalf("Read count %d = (%d, %v), want ErrInvalidRead", count, n, err)
		}
	}
}

type noProgressReader struct{}

func (noProgressReader) Read([]byte) (int, error) { return 0, nil }

type invalidCountReader struct {
	count int
}

func (r invalidCountReader) Read([]byte) (int, error) { return r.count, nil }

func TestWriterRejectsOversize(t *testing.T) {
	var dst bytes.Buffer
	w := NewWriter(&dst, 3)
	if _, err := w.Write([]byte("abcd")); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("Write error = %v, want ErrTooLarge", err)
	}
	if got := dst.String(); got != "abc" {
		t.Fatalf("underlying bytes = %q, want abc", got)
	}
	if !w.Exceeded() {
		t.Fatal("Exceeded = false, want true")
	}
}
