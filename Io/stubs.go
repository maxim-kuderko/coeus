package Io

import (
	"github.com/maxim-kuderko/coeus/events"
	"sync"
)

type Stub struct {
	n      int
	output []*events.Event
	mu     sync.Mutex
}

func NewStub(n int) *Stub {
	return &Stub{n: n, output: make([]*events.Event, 0)}
}

func (s *Stub) Output(events chan *events.Events) {
	for es := range events {
		for _, e := range es.Data() {
			s.mu.Lock()
			s.output = append(s.output, e)
			s.mu.Unlock()
		}
		es.Ack()
	}
}

func (s *Stub) Input() chan *events.Events {
	output := make(chan *events.Events)
	go func() {
		defer close(output)
		for i := 0; i < s.n; i++ {
			output <- events.NewEvents(func() error {
				return nil
			}, []*events.Event{
				{Data: int64(i)},
			})
		}
	}()
	return output
}

func (s *Stub) OutputCount() int {
	return len(s.output)
}

func (s *Stub) OutputEvents() []*events.Event {
	return s.output
}

func (s *Stub) Reset() {
	s.output = make([]*events.Event, 0)
}
