package forward

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"

	"github.com/grepplabs/vectap/internal/kube"
	"github.com/grepplabs/vectap/internal/targets"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

type Session struct {
	TargetID    string
	LocalPort   int
	RemotePort  int
	EndpointURL string
}

type Manager interface {
	Start(ctx context.Context, target targets.Target) (*Session, error)
}

type PortForwardManager struct {
	client            kubernetes.Interface
	cfg               *rest.Config
	allocateLocalPort func() (int, error)
	newPortForwarder  func(target targets.Target, localPort int, stopCh, readyCh chan struct{}) (forwarder, error)
}

type forwarder interface {
	ForwardPorts() error
}

func NewManagerFromConfig(kubeConfigPath, kubeContext string) (*PortForwardManager, error) {
	cfg, err := kube.LoadConfig(kubeConfigPath, kubeContext)
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}
	return NewManager(client, cfg), nil
}

func NewManager(client kubernetes.Interface, cfg *rest.Config) *PortForwardManager {
	m := &PortForwardManager{client: client, cfg: cfg}
	m.allocateLocalPort = allocateLocalPort
	m.newPortForwarder = m.defaultNewPortForwarder
	return m
}

func (m *PortForwardManager) Start(ctx context.Context, target targets.Target) (*Session, error) {
	localPort, err := m.allocateLocalPort()
	if err != nil {
		return nil, fmt.Errorf("allocate local port: %w", err)
	}

	stopCh := make(chan struct{})
	readyCh := make(chan struct{})
	closeStop := sync.OnceFunc(func() { close(stopCh) })

	pf, err := m.newPortForwarder(target, localPort, stopCh, readyCh)
	if err != nil {
		return nil, fmt.Errorf("create port-forward for %s: %w", target.ID, err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- pf.ForwardPorts()
	}()

	go func() {
		<-ctx.Done()
		closeStop()
	}()

	select {
	case <-readyCh:
		return &Session{
			TargetID:    target.ID,
			LocalPort:   localPort,
			RemotePort:  target.RemotePort,
			EndpointURL: fmt.Sprintf("http://127.0.0.1:%d/graphql", localPort),
		}, nil
	case err := <-errCh:
		closeStop()
		if err == nil {
			return nil, fmt.Errorf("port-forward for %s closed before ready", target.ID)
		}
		return nil, fmt.Errorf("start port-forward for %s: %w", target.ID, err)
	case <-ctx.Done():
		closeStop()
		return nil, ctx.Err()
	}
}

func (m *PortForwardManager) defaultNewPortForwarder(target targets.Target, localPort int, stopCh, readyCh chan struct{}) (forwarder, error) {
	reqURL := m.client.CoreV1().RESTClient().Post().Resource("pods").Namespace(target.Namespace).Name(target.PodName).SubResource("portforward").URL()

	transport, upgrader, err := spdy.RoundTripperFor(m.cfg)
	if err != nil {
		return nil, fmt.Errorf("create roundtripper: %w", err)
	}
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, reqURL)

	ports := []string{fmt.Sprintf("%d:%d", localPort, target.RemotePort)}
	pf, err := portforward.New(dialer, ports, stopCh, readyCh, io.Discard, io.Discard)
	if err != nil {
		return nil, err
	}
	return pf, nil
}

func allocateLocalPort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0") //nolint:noctx
	if err != nil {
		return 0, err
	}
	defer ln.Close() //nolint:errcheck

	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("unexpected listener address type %T", ln.Addr())
	}
	return addr.Port, nil
}
