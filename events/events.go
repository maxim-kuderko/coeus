package events

type Event struct {
	ID   string
	Data interface{}
}

type Events struct {
	data []*Event
	ack  func() error
}

func NewEvents(ack func() error, events []*Event) *Events {
	return &Events{
		data: events,
		ack:  ack,
	}
}

func (e *Events) Data() []*Event {
	return e.data
}

func (e *Events) Ack() error {
	return e.ack()
}
