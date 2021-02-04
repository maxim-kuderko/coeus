package coeus

import (
	"github.com/maxim-kuderko/coeus/events"
	"sync"
)

func fanIn(output chan *events.Events, in ...chan *events.Events) {
	wg := sync.WaitGroup{}
	wg.Add(len(in))
	for _, es := range in {
		go func(es chan *events.Events) {
			defer wg.Done()
			for e := range es {
				output <- e
			}
		}(es)
	}
	wg.Wait()
}
