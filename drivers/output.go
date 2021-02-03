package drivers

import (
	"github.com/maxim-kuderko/coeus/events"
)

type OutputError struct {
}

func (o *OutputError) Error() string {
	panic("implement me")
}

type Output interface {
	Store(events chan *events.Events) chan error
}
