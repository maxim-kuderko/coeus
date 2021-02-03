package drivers

import (
	"context"
	"github.com/maxim-kuderko/coeus/events"
	"time"
)

type InputError struct {
}

func (i *InputError) Error() string {
	panic("implement me")
}

type Input interface {
	Next(ctx context.Context, n int, timeout time.Duration) (chan *events.Events, chan error)
}
