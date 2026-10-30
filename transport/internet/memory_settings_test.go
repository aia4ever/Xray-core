package internet

import (
	"sync/atomic"
	"testing"
)

func TestMemoryStreamConfigClose(t *testing.T) {
	mss := &MemoryStreamConfig{
		DownloadSettings: &MemoryStreamConfig{},
	}

	var parentCloses, childCloses, lateCloses atomic.Int32
	if !mss.RegisterOnClose(func() { parentCloses.Add(1) }) {
		t.Fatal("expected registration on an open config to succeed")
	}
	if !mss.DownloadSettings.RegisterOnClose(func() { childCloses.Add(1) }) {
		t.Fatal("expected registration on an open download config to succeed")
	}

	if err := mss.Close(); err != nil {
		t.Fatal(err)
	}
	if parentCloses.Load() != 1 {
		t.Fatal("expected the close callback to run once, got ", parentCloses.Load())
	}
	if childCloses.Load() != 1 {
		t.Fatal("expected the download settings to be closed as well, got ", childCloses.Load())
	}

	if err := mss.Close(); err != nil {
		t.Fatal(err)
	}
	if parentCloses.Load() != 1 || childCloses.Load() != 1 {
		t.Fatal("expected close callbacks to run only once")
	}

	if mss.RegisterOnClose(func() { lateCloses.Add(1) }) {
		t.Fatal("expected registration on a closed config to fail")
	}
	if err := mss.Close(); err != nil {
		t.Fatal(err)
	}
	if lateCloses.Load() != 0 {
		t.Fatal("expected the late callback to never run")
	}
}
