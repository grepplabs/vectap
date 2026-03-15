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
