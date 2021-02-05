package Io

import (
	"fmt"
	"github.com/maxim-kuderko/coeus/events"
	"os"
)

type StdOut struct {
	err chan<- error
}

func (d *StdOut) Output(events chan *events.Events) {

	for es := range events {
		for _, e := range es.Data() {
			fmt.Fprint(os.Stdout, e.Data)
			fmt.Fprint(os.Stdout, "\n")
		}
		if err := es.Ack(); err != nil {
			d.err <- err
		}
	}

}

func NewStdOut(errs chan<- error) *StdOut {
	return &StdOut{err: errs}
}
