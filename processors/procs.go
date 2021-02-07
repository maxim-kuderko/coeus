package processors

import "github.com/maxim-kuderko/coeus/events"

type Processor struct {
	Func        func(eventsChan chan *events.Events) chan *events.Events
	Concurrency int
}
