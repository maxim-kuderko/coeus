package coeus

func fanIn(output chan error, in chan error) {
	for err := range in {
		output <- err
	}
}
