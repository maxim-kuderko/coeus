package drivers

import (
	"context"
	"github.com/maxim-kuderko/coeus/events"
	"go.uber.org/atomic"
	"strconv"
	"time"
)

type Stub struct {
	n           int
	outputCount atomic.Int32
}

func NewStub(n int) *Stub {
	return &Stub{n: n}
}

func (s *Stub) Store(events chan *events.Events) chan error {
	errs := make(chan error)
	go func() {
		defer close(errs)
		for es := range events {
			for range es.Data() {
				s.outputCount.Add(1)
			}
			if err := es.Ack(); err != nil {
				errs <- err
			}
		}
	}()
	return errs
}

func (s *Stub) Next(ctx context.Context, n int, timeout time.Duration) (chan *events.Events, chan error) {
	output, errs := make(chan *events.Events), make(chan error)
	go func() {
		defer close(output)
		defer close(errs)
		for i := 0; i < s.n; i++ {
			output <- events.NewEvents(func() error {
				return nil
			}, []*events.Event{
				{ID: `test`, Data: []byte(strconv.Itoa(i))},
			})
		}
	}()
	return output, errs
}

func (s *Stub) OutputCount() int {
	return int(s.outputCount.Load())
}
