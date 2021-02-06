package processors

import (
	"github.com/maxim-kuderko/coeus/events"
)

func Avg(summer func(events2 *events.Events) float64) func(es chan *events.Events) chan *events.Events {
	return func(es chan *events.Events) chan *events.Events {
		output := make(chan *events.Events)
		sum := float64(0)
		count := float64(0)
		go func() {
			defer close(output)
			acks := make([]func() error, 0)
			for e := range es {
				sum += summer(e)
				count++
				acks = append(acks, e.Ack)
			}
			output <- events.NewEvents(func() error {
				for _, ack := range acks {
					if err := ack(); err != nil {
						return err
					}
				}
				return nil
			}, []*events.Event{{Data: sum / count}})
		}()
		return output
	}
}
