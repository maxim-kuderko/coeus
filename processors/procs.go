package processors

import "github.com/maxim-kuderko/coeus/events"

type Processor func(events chan *events.Events) chan *events.Events
