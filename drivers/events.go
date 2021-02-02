package drivers

type Event struct {
	acker
}

type Events struct {
	data []*Event
	acker
}

type acker func() error
