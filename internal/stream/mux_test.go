package stream

import (
	"errors"
	"testing"
	"time"

	"github.com/grepplabs/vectap/internal/output"
	"github.com/stretchr/testify/require"
)

func TestMux(t *testing.T) {
	m := NewMux()

	evCh := make(chan output.Event, 1)
	errCh := make(chan error, 1)

	evCh <- output.Event{Message: "hello"}
	errCh <- errors.New("boom")
	close(evCh)
	close(errCh)

	m.Add("t1", evCh, errCh)
	m.CloseWhenDone()

	select {
	case ev, ok := <-m.Events():
		require.True(t, ok, "expected event channel to remain open long enough to read")
		require.Equal(t, "hello", ev.Message)
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for event")
	}

	select {
	case err := <-m.Errors():
		require.EqualError(t, err, "boom")
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for error")
	}
}

func TestMuxSupportsDynamicAddAfterCloseWhenDone(t *testing.T) {
	m := NewMux()

	keepAliveEvents := make(chan output.Event)
	keepAliveErrs := make(chan error)
	m.Add("watcher", keepAliveEvents, keepAliveErrs)
	close(keepAliveEvents)

	m.CloseWhenDone()

	evCh := make(chan output.Event, 1)
	errCh := make(chan error)

	m.Add("t1", evCh, errCh)

	evCh <- output.Event{Message: "dynamic"}
	close(evCh)
	close(errCh)

	select {
	case ev, ok := <-m.Events():
		require.True(t, ok, "expected event channel to remain open long enough to read")
		require.Equal(t, "dynamic", ev.Message)
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for dynamic event")
	}

	close(keepAliveErrs)

	select {
	case _, ok := <-m.Events():
		require.False(t, ok, "expected event channel to close after dynamic stream completion")
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for mux shutdown")
	}
}
