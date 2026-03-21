package stream

import (
	"sync"

	"github.com/grepplabs/vectap/internal/output"
)

type Mux struct {
	events chan output.Event
	errors chan error

	mu       sync.Mutex
	active   int
	closing  bool
	closeMux func()
}

func NewMux() *Mux {
	m := &Mux{
		events: make(chan output.Event),
		errors: make(chan error, 16),
	}
	m.closeMux = sync.OnceFunc(func() {
		close(m.events)
		close(m.errors)
	})
	return m
}

func (m *Mux) Add(_ string, evCh <-chan output.Event, errCh <-chan error) {
	m.mu.Lock()
	if m.closing && m.active == 0 {
		m.mu.Unlock()
		return
	}
	m.active += 2
	m.mu.Unlock()

	go func() {
		defer m.done()
		for ev := range evCh {
			m.events <- ev
		}
	}()

	go func() {
		defer m.done()
		for err := range errCh {
			if err != nil {
				m.errors <- err
			}
		}
	}()
}

func (m *Mux) Events() <-chan output.Event {
	return m.events
}

func (m *Mux) Errors() <-chan error {
	return m.errors
}

func (m *Mux) CloseWhenDone() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closing = true
	if m.active == 0 {
		m.closeLocked()
	}
}

func (m *Mux) done() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.active--
	if m.closing && m.active == 0 {
		m.closeLocked()
	}
}

func (m *Mux) closeLocked() {
	m.closeMux()
}
