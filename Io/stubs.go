package Io

import (
	"github.com/maxim-kuderko/coeus/events"
	"go.uber.org/atomic"
)

type Stub struct {
	n           int
	outputCount atomic.Int64
}

func NewStub(n int) *Stub {
	return &Stub{n: n}
}

func (s *Stub) Output(events chan *events.Events) {
	for es := range events {
		for range es.Data() {
			s.outputCount.Add(1)
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
				{Data: i},
			})
		}
	}()
	return output
}

func (s *Stub) OutputCount() int64 {
	return s.outputCount.Load()
}
