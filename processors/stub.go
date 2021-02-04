package processors

import "github.com/maxim-kuderko/coeus/events"

func Stub(es chan *events.Events) chan *events.Events {
	output := make(chan *events.Events)
	go func() {
		defer close(output)
		for e := range es {
			output <- e
		}
	}()
	return output
}
