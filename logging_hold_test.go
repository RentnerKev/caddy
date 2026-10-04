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
	"context"
	"sync"
	"testing"
)

func loggingHoldContext(t *testing.T, logging *Logging) (Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := loggingLifecycleOpen(t, logging)
	ctx.cfg = &Config{Logging: logging}
	return ctx, cancel
}

func TestHoldLoggingRetainsOnlyWriters(t *testing.T) {
	logging, writer, _ := loggingLifecycleConfig(t, "retired", false)
	ctx, cancel := loggingHoldContext(t, logging)
	first, err := ctx.HoldLogging()
	if err != nil {
		t.Fatal(err)
	}
	defer first()
	second, err := ctx.WithValue("copy", true).HoldLogging()
	if err != nil {
		t.Fatal(err)
	}
	defer second()
	cleaned := make(chan struct{})
	ctx.OnCancel(func() { close(cleaned) })
	cancel()
	select {
	case <-cleaned:
	default:
		t.Fatal("log writer retention held configuration cleanup")
	}
	logger := logging.defaultLog.logger
	logger.Info("retained after cleanup")
	writer.check(t, "retained after cleanup", 0)
	first()
	first()
	logger.Info("second holder remains")
	writer.check(t, "second holder remains", 0)
	var releases sync.WaitGroup
	for range 8 {
		releases.Go(second)
	}
	releases.Wait()
	writer.check(t, "", 1)
	if refs, ok := writers.References(writer.WriterKey()); ok {
		t.Fatalf("released writer still pooled with %d references", refs)
	}
	if release, err := ctx.HoldLogging(); err == nil || release != nil {
		t.Fatal("canceled context accepted a log writer hold")
	}
}

func TestHoldLoggingSharedWriters(t *testing.T) {
	logging, writer, _ := loggingLifecycleConfig(t, "shared", false)
	// Both the sink and structured log own the same pooled destination.
	logging.Sink = &SinkLog{BaseLog: BaseLog{writerOpener: writer}}
	ctx, cancel := loggingHoldContext(t, logging)
	release, err := ctx.HoldLogging()
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	next := &Logging{Logs: map[string]*CustomLog{DefaultLoggerName: {BaseLog: BaseLog{writerOpener: writer}}}}
	_, nextCancel := loggingHoldContext(t, next)
	cancel()
	logging.defaultLog.logger.Info("retained shared writer")
	writer.check(t, "retained shared writer", 0)
	release()
	writer.check(t, "", 0)
	nextCancel()
	writer.check(t, "", 1)
	if _, ok := writers.References(writer.WriterKey()); ok {
		t.Fatal("shared writer leaked")
	}
}

func TestHoldLoggingDerivedContext(t *testing.T) {
	logging, writer, _ := loggingLifecycleConfig(t, "parent", false)
	parent, cancel := loggingHoldContext(t, logging)
	child, childCancel := NewContext(parent)
	defer childCancel()
	release, err := child.HoldLogging()
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	cancel()
	logging.defaultLog.logger.Info("child retained parent logger")
	writer.check(t, "child retained parent logger", 0)
	release()
	writer.check(t, "", 1)
}

func TestHoldLoggingUnavailableWriters(t *testing.T) {
	ctx, cancel := NewContext(Context{Context: context.Background()})
	defer cancel()
	release, err := ctx.HoldLogging()
	if err != nil {
		t.Fatal(err)
	}
	release()
	release()
	logging, writer, _ := loggingLifecycleConfig(t, "closed", false)
	ctx, cancelLogging := loggingHoldContext(t, logging)
	defer cancelLogging()
	if err := logging.closeLogs(); err != nil {
		t.Fatal(err)
	}
	if release, err := ctx.HoldLogging(); err == nil || release != nil {
		t.Fatal("closed logging configuration accepted a hold")
	}
	writer.check(t, "", 1)
	if _, ok := writers.References(writer.WriterKey()); ok {
		t.Fatal("closed writer resurrected")
	}
}
