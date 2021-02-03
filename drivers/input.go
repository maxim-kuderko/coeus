package drivers

import (
	"context"
	"time"
)

type InputError struct {
}

func (i *InputError) Error() string {
	panic("implement me")
}

type Input interface {
	Next(ctx context.Context, n int, timeout time.Duration) (chan *Events, chan error)
}
