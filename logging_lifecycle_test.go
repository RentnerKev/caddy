// Copyright 2015 Matthew Holt and The Caddy Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package caddy

import (
	"bytes"
	"context"
	"io"
	"log"
	"strings"
	"sync"
	"testing"
	"time"
)

type loggingLifecycleWriter struct {
	key          string
	mu           sync.Mutex
	content      bytes.Buffer
	closed       bool
	closedWrites int
	closes       int
}

func (writer *loggingLifecycleWriter) WriterKey() string                   { return writer.key }
func (writer *loggingLifecycleWriter) String() string                      { return writer.key }
func (writer *loggingLifecycleWriter) OpenWriter() (io.WriteCloser, error) { return writer, nil }
func (writer *loggingLifecycleWriter) Write(data []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.closed {
		writer.closedWrites++
		return 0, io.ErrClosedPipe
	}
	return writer.content.Write(data)
}

func (writer *loggingLifecycleWriter) Close() error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.closes++
	writer.closed = true
	return nil
}

func (writer *loggingLifecycleWriter) check(t *testing.T, marker string, wantCloses int) {
	t.Helper()
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if marker != "" && !strings.Contains(writer.content.String(), marker) {
		t.Errorf("writer %s missed %q: %s", writer.key, marker, writer.content.String())
	}
	if writer.closes != wantCloses || writer.closedWrites != 0 {
		t.Errorf("writer %s closes=%d writes-after-close=%d; want closes=%d", writer.key, writer.closes, writer.closedWrites, wantCloses)
	}
}

func loggingLifecycleConfig(t *testing.T, name string, sink bool) (*Logging, *loggingLifecycleWriter, *loggingLifecycleWriter) {
	t.Helper()
	structured := &loggingLifecycleWriter{key: t.Name() + "/" + name + "/default"}
	logging := &Logging{Logs: map[string]*CustomLog{DefaultLoggerName: {BaseLog: BaseLog{writerOpener: structured}}}}
	var unstructured *loggingLifecycleWriter
	if sink {
		unstructured = &loggingLifecycleWriter{key: t.Name() + "/" + name + "/sink"}
		logging.Sink = &SinkLog{BaseLog: BaseLog{writerOpener: unstructured}}
	}
	return logging, structured, unstructured
}

func loggingLifecycleOpen(t *testing.T, logging *Logging) (Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := NewContext(Context{Context: context.Background()})
	t.Cleanup(cancel)
	if err := logging.openLogs(ctx); err != nil {
		t.Fatal(err)
	}
	return ctx, cancel
}

func TestLoggingLifecycleRetiredGenerationKeepsCurrentSink(t *testing.T) {
	old, oldDefault, oldSink := loggingLifecycleConfig(t, "old", true)
	oldContext, cancelOld := loggingLifecycleOpen(t, old)
	release := oldContext.HoldCleanup()
	t.Cleanup(release)
	current, currentDefault, currentSink := loggingLifecycleConfig(t, "current", true)
	_, cancelCurrent := loggingLifecycleOpen(t, current)
	cancelOld()
	oldDefault.check(t, "", 0)
	oldSink.check(t, "", 0)
	release()
	log.Print("stdlib remains current")
	Log().Info("structured remains current")
	oldDefault.check(t, "", 1)
	oldSink.check(t, "", 1)
	currentDefault.check(t, "structured remains current", 0)
	currentSink.check(t, "stdlib remains current", 0)
	cancelCurrent()
	log.Print("stdlib after stop")
	Log().Info("structured after stop")
	currentDefault.check(t, "", 1)
	currentSink.check(t, "", 1)
}

func TestLoggingLifecycleSinkWithoutCustomDefault(t *testing.T) {
	_, _, sink := loggingLifecycleConfig(t, "sink", true)
	logging := &Logging{Sink: &SinkLog{BaseLog: BaseLog{writerOpener: sink}}}
	_, cancel := loggingLifecycleOpen(t, logging)
	log.Print("stdlib reaches explicit sink")
	sink.check(t, "stdlib reaches explicit sink", 0)
	cancel()
	sink.check(t, "", 1)
}

func TestLoggingLifecycleReplacementWithoutSink(t *testing.T) {
	old, _, oldSink := loggingLifecycleConfig(t, "old", true)
	_, cancelOld := loggingLifecycleOpen(t, old)
	current, currentDefault, _ := loggingLifecycleConfig(t, "current", false)
	_, _ = loggingLifecycleOpen(t, current)
	cancelOld()
	log.Print("replacement default receives stdlib")
	currentDefault.check(t, "replacement default receives stdlib", 0)
	oldSink.check(t, "", 1)
}

func TestLoggingLifecycleFailedProvisionRestoresLiveLoggers(t *testing.T) {
	current, currentDefault, currentSink := loggingLifecycleConfig(t, "current", true)
	_, _ = loggingLifecycleOpen(t, current)
	failed, failedDefault, failedSink := loggingLifecycleConfig(t, "failed", true)
	failed.Logs["invalid"] = &CustomLog{BaseLog: BaseLog{Level: "invalid-level"}}
	_, err := provisionContext(&Config{Logging: failed}, false)
	if err == nil {
		t.Fatal("invalid logging configuration was accepted")
	}
	log.Print("stdlib restored after failed provision")
	Log().Info("structured restored after failed provision")
	currentDefault.check(t, "structured restored after failed provision", 0)
	currentSink.check(t, "stdlib restored after failed provision", 0)
	failedDefault.check(t, "", 1)
	failedSink.check(t, "", 1)
}

func TestLoggingLifecycleNewestRetiresBeforeOld(t *testing.T) {
	old, oldDefault, oldSink := loggingLifecycleConfig(t, "old", true)
	_, cancelOld := loggingLifecycleOpen(t, old)
	current, currentDefault, currentSink := loggingLifecycleConfig(t, "current", true)
	_, cancelCurrent := loggingLifecycleOpen(t, current)
	cancelCurrent()
	log.Print("live prior sink restored")
	Log().Info("live prior default restored")
	oldDefault.check(t, "live prior default restored", 0)
	oldSink.check(t, "live prior sink restored", 0)
	cancelOld()
	log.Print("no retired logger resurrected")
	Log().Info("no retired structured logger resurrected")
	oldDefault.check(t, "", 1)
	oldSink.check(t, "", 1)
	currentDefault.check(t, "", 1)
	currentSink.check(t, "", 1)
}

func TestLoggingLifecycleBufferedStartupKeepsFallbackUsable(t *testing.T) {
	buffered, original, buffer := BufferedLog()
	buffered.Info("buffered startup")
	logging, writer, _ := loggingLifecycleConfig(t, "current", false)
	_, cancel := loggingLifecycleOpen(t, logging)
	writer.check(t, "buffered startup", 0)
	cancel()
	if Log() != original {
		t.Error("stopping configuration did not restore the unbuffered process logger")
	}
	if Log().Core() == buffer {
		t.Error("process logger still buffers after configuration cleanup")
	}
	Log().Info("fallback after buffered startup")
	writer.check(t, "", 1)
}

// A network sink can block in Write until its underlying connection is closed.
// Cleanup must be able to close it without first acquiring log.Logger's outMu.
type loggingLifecycleBlockedWriter struct {
	key       string
	entered   chan struct{}
	closed    chan struct{}
	writeOnce sync.Once
	closeOnce sync.Once
}

func (writer *loggingLifecycleBlockedWriter) WriterKey() string { return writer.key }

func (writer *loggingLifecycleBlockedWriter) String() string { return writer.key }

func (writer *loggingLifecycleBlockedWriter) OpenWriter() (io.WriteCloser, error) { return writer, nil }

func (writer *loggingLifecycleBlockedWriter) Write(data []byte) (int, error) {
	writer.writeOnce.Do(func() { close(writer.entered) })
	<-writer.closed
	return 0, io.ErrClosedPipe
}

func (writer *loggingLifecycleBlockedWriter) Close() error {
	writer.closeOnce.Do(func() { close(writer.closed) })
	return nil
}

func TestLoggingLifecycleBlockedSinkDoesNotBlockCleanup(t *testing.T) {
	logging, structured, _ := loggingLifecycleConfig(t, "current", false)
	blocked := &loggingLifecycleBlockedWriter{key: t.Name(), entered: make(chan struct{}), closed: make(chan struct{})}
	logging.Sink = &SinkLog{BaseLog: BaseLog{writerOpener: blocked}}
	_, cancel := loggingLifecycleOpen(t, logging)
	t.Cleanup(func() { _ = blocked.Close() })
	written := make(chan struct{})
	go func() { log.Print("blocked network sink"); close(written) }()
	select {
	case <-blocked.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("standard log did not enter the network sink")
	}
	cleaned := make(chan struct{})
	go func() { cancel(); close(cleaned) }()
	select {
	case <-cleaned:
	case <-time.After(3 * time.Second):
		_ = blocked.Close() // release the writer even when the regression returns
		t.Error("cleanup could not close a blocked standard log sink")
	}
	select {
	case <-written:
	case <-time.After(3 * time.Second):
		t.Fatal("closing the sink did not unblock its writer")
	}
	select {
	case <-cleaned:
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup remained blocked after closing the sink")
	}
	structured.check(t, "", 1)
	Log().Info("logging still works after closing the blocked sink")
}
