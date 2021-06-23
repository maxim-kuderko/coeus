package Io

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/maxim-kuderko/coeus/events"
	"strings"
	"time"
)

type Kakfa struct {
	ctx  context.Context
	opt  *KafkaOpt
	errs chan error
}

type KafkaOpt struct {
	BootstrapServers string
	ConsumerGroupID  string
	ReadTimeout      time.Duration
	DefaultOffset    string
	Topics           string
	Batch            int
}

func NewKafka(ctx context.Context, errs chan error, opt *KafkaOpt) *Kakfa {
	return &Kakfa{ctx: ctx, opt: opt, errs: errs}
}

func (k *Kakfa) Output(events chan *events.Events) {

}

func (k *Kakfa) Input() chan *events.Events {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     k.opt.BootstrapServers,
		"group.id":              k.opt.ConsumerGroupID,
		"broker.address.family": "v4",
		"auto.offset.reset":     k.opt.DefaultOffset,
		"enable.auto.commit":    false,
		//"session.timeout.ms":              int(600),
	})
	if err != nil {
		panic(err)
	}

	if err = c.SubscribeTopics(strings.Split(k.opt.Topics, `,`), nil); err != nil {
		panic(err)
	}
	output := make(chan *events.Events)
	go func() {
		defer c.Close()
		defer close(output)
		run := true
		buffer := make([]*events.Event, 0, k.opt.Batch)
		defer func() {
			if len(buffer) > 0 {
				output <- events.NewEvents(k.ackFnBuilder(buffer, c), buffer)
			}
		}()
		for run {
			select {
			case <-k.ctx.Done():
				fmt.Printf("closing kafka consumer")
				return
			default:
				ev := c.Poll(2)
				if ev == nil {
					if len(buffer) > 0 {
						output <- events.NewEvents(k.ackFnBuilder(buffer, c), buffer)
						buffer = make([]*events.Event, 0, k.opt.Batch)
					}
					continue
				}
				switch e := ev.(type) {
				case *kafka.Message:
					buffer = append(buffer, &events.Event{
						Data:     e,
						Metadata: e.TopicPartition,
					})
					if len(buffer) == k.opt.Batch {
						ackFn := k.ackFnBuilder(buffer, c)
						output <- events.NewEvents(ackFn, buffer)
						buffer = make([]*events.Event, 0, k.opt.Batch)
					}
				case kafka.Error:
					k.errs <- e
					if e.Code() == kafka.ErrAllBrokersDown {
						run = false
					}
				default:
					fmt.Printf("Ignored %v\n", e)
				}
			}

		}
	}()
	return output
}

func (k *Kakfa) ackFnBuilder(buffer []*events.Event, c *kafka.Consumer) func() error {
	return func() error {

		perPart := map[int]kafka.TopicPartition{}
		for _, msg := range buffer {
			v, ok := perPart[int(msg.Metadata.(kafka.TopicPartition).Partition)]
			if !ok {
				perPart[int(msg.Metadata.(kafka.TopicPartition).Partition)] = msg.Metadata.(kafka.TopicPartition)
				continue
			}
			if v.Offset < msg.Metadata.(kafka.TopicPartition).Offset {
				perPart[int(msg.Metadata.(kafka.TopicPartition).Partition)] = msg.Metadata.(kafka.TopicPartition)
			}
		}
		tmp := make([]kafka.TopicPartition, 0, len(buffer))
		for _, topic := range perPart {
			tmp = append(tmp, topic)
		}
		oo, err := c.CommitOffsets(tmp)
		if err != nil && len(oo) > 0 {
			fmt.Println(oo[0].Partition, `   offset: `, oo[0].Offset)
		}
		return err
	}
}
