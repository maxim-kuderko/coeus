package Io

import (
	"github.com/maxim-kuderko/coeus/events"
)

type Discard struct {
	errs chan<- error
}

func (d *Discard) Store(events chan *events.Events) {
	for e := range events {
		if err := e.Ack(); err != nil {
			d.errs <- err
		}
	}
}

func NewDiscard(errs chan<- error) *Discard {
	return &Discard{errs: errs}
}
