package processors

import (
	"github.com/maxim-kuderko/coeus/events"
)

func Sum(summer func(events2 *events.Events) int64) func(es chan *events.Events) chan *events.Events {
	return func(es chan *events.Events) chan *events.Events {
		output := make(chan *events.Events)
		sum := int64(0)
		go func() {
			defer close(output)
			for e := range es {
				sum += summer(e)
			}
			output <- events.NewEvents(func() error {
				return nil
			}, []*events.Event{{Data: sum}})
		}()
		return output
	}
}
