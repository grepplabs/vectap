package forward

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/grepplabs/vectap/internal/targets"
	"github.com/stretchr/testify/require"
)

type fakeForwarder struct {
	errCh chan error
}

func (f *fakeForwarder) ForwardPorts() error {
	if f.errCh != nil {
		return <-f.errCh
	}
	return nil
}

func TestPortForwardManagerStartReturnsEndpointURL(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	mgr := &PortForwardManager{}
	mgr.allocateLocalPort = func() (int, error) { return 21001, nil }
	mgr.newPortForwarder = func(_ targets.Target, _ int, stopCh chan struct{}, readyCh chan struct{}) (forwarder, error) {
		close(readyCh)

		errCh := make(chan error, 1)
		go func() {
			<-stopCh
			errCh <- nil
		}()
		return &fakeForwarder{errCh: errCh}, nil
	}

	session, err := mgr.Start(ctx, targets.Target{ID: "ns/pod", RemotePort: 8686})
	require.NoError(t, err)

	require.Equal(t, 21001, session.LocalPort)
	require.Equal(t, "http://127.0.0.1:21001/graphql", session.EndpointURL)
}

func TestPortForwardManagerStartClosesOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	stopObserved := make(chan struct{})
	mgr := &PortForwardManager{}
	mgr.allocateLocalPort = func() (int, error) { return 21002, nil }
	mgr.newPortForwarder = func(_ targets.Target, _ int, stopCh, readyCh chan struct{}) (forwarder, error) {
		close(readyCh)

		errCh := make(chan error, 1)
		go func() {
			<-stopCh
			close(stopObserved)
			errCh <- nil
		}()
		return &fakeForwarder{errCh: errCh}, nil
	}

	session, err := mgr.Start(ctx, targets.Target{ID: "ns/pod", RemotePort: 8686})
	require.NoError(t, err)
	require.NotNil(t, session)

	cancel()

	select {
	case <-stopObserved:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "stop signal was not observed")
	}
}

func TestPortForwardManagerStartFailsIfForwarderErrorsBeforeReady(t *testing.T) {
	mgr := &PortForwardManager{}
	mgr.allocateLocalPort = func() (int, error) { return 21003, nil }
	mgr.newPortForwarder = func(_ targets.Target, _ int, _ chan struct{}, _ chan struct{}) (forwarder, error) {
		ch := make(chan error, 1)
		ch <- errors.New("boom")
		return &fakeForwarder{errCh: ch}, nil
	}

	_, err := mgr.Start(t.Context(), targets.Target{ID: "ns/pod", RemotePort: 8686})
	require.Error(t, err)
}

func TestAllocateLocalPort(t *testing.T) {
	port, err := allocateLocalPort()
	require.NoError(t, err)
	require.Positive(t, port)
}
