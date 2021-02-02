package drivers

type OutputError struct {
}

func (o *OutputError) Error() string {
	panic("implement me")
}

type Output interface {
	Store(event chan *Event) chan error
	BulkStore(events chan *Events) chan error
}
