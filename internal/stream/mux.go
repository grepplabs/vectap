package stream

import (
	"sync"

	"github.com/grepplabs/vectap/internal/output"
)

type Mux struct {
	events chan output.Event
	errors chan error
	wg     sync.WaitGroup
}

func NewMux() *Mux {
	return &Mux{
		events: make(chan output.Event),
		errors: make(chan error, 16),
	}
}

func (m *Mux) Add(_ string, evCh <-chan output.Event, errCh <-chan error) {
	m.wg.Add(2)

	go func() {
		defer m.wg.Done()
		for ev := range evCh {
			m.events <- ev
		}
	}()

	go func() {
		defer m.wg.Done()
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
	go func() {
		m.wg.Wait()
		close(m.events)
		close(m.errors)
	}()
}
