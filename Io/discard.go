package Io

import (
	"github.com/maxim-kuderko/coeus/events"
)

type Discard struct {
}

func (d *Discard) Store(events chan *events.Events) chan error {
	errs := make(chan error)
	go func() {
		defer close(errs)
		for e := range events {
			if err := e.Ack(); err != nil {
				errs <- err
			}
		}
	}()
	return errs
}

func NewDiscard() *Discard {
	return &Discard{}
}
