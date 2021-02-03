package drivers

type OutputError struct {
}

func (o *OutputError) Error() string {
	panic("implement me")
}

type Output interface {
	Store(events chan *Events) chan error
}
