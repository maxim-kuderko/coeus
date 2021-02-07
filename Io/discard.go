package Io

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/maxim-kuderko/coeus/events"
)

type Discard struct {
	errs chan<- error
}

func (d *Discard) Output(events chan *events.Events) {
	for e := range events {
		fmt.Println(e.Data()[0].Metadata.(kafka.TopicPartition).Partition, "  offest   ", e.Data()[0].Metadata.(kafka.TopicPartition).Offset)
		if err := e.Ack(); err != nil {
			d.errs <- err
		}
	}
}

func NewDiscard(errs chan<- error) *Discard {
	return &Discard{errs: errs}
}
