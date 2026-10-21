package backend

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"
)

type stalledContent struct {
	*io.PipeReader
	started chan struct{}
	once    sync.Once
}

func (s *stalledContent) Read(p []byte) (int, error) {
	s.once.Do(func() { close(s.started) })
	return s.PipeReader.Read(p)
}

func (*stalledContent) Seek(int64, int) (int64, error) { return 0, nil }

func TestMemoryCreateObjectDoesNotBlockReadsWhileContentStreams(t *testing.T) {
	storage, err := NewStorageMemory(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.CreateBucket("bucket", BucketAttrs{}); err != nil {
		t.Fatal(err)
	}
	existing := StreamingObject{
		ObjectAttrs: ObjectAttrs{BucketName: "bucket", Name: "existing"},
		Content:     noopSeekCloser{bytes.NewReader([]byte("data"))},
	}
	if _, err := storage.CreateObject(existing, NoConditions{}); err != nil {
		t.Fatal(err)
	}

	body, writer := io.Pipe()
	started := make(chan struct{})
	uploaded := make(chan error, 1)
	defer func() {
		writer.Close()
		select {
		case err := <-uploaded:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(2 * time.Second):
			t.Error("upload did not finish after releasing its body")
		}
	}()
	go func() {
		_, err := storage.CreateObject(StreamingObject{
			ObjectAttrs: ObjectAttrs{BucketName: "bucket", Name: "stalled"},
			Content:     &stalledContent{PipeReader: body, started: started},
		}, NoConditions{})
		uploaded <- err
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("upload did not start reading its body")
	}

	done := make(chan error, 1)
	go func() {
		_, err := storage.GetObject("bucket", "existing")
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GetObject blocked behind an upload whose body is still streaming")
	}
}
