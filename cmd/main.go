package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/maxim-kuderko/coeus"
	"github.com/maxim-kuderko/coeus/Io"
	"github.com/maxim-kuderko/coeus/events"
	"github.com/maxim-kuderko/coeus/processors"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 100)

	p := coeus.NewPipeline(Io.NewKafka(ctx, errs, &Io.KafkaOpt{
		BootstrapServers: os.Getenv(`KAFKA_BOOTSTRAP_SERVERS`),
		ConsumerGroupID:  os.Getenv(`CONSUMER_GROUP_ID`),
		ReadTimeout:      time.Millisecond * 5000,
		DefaultOffset:    `earliest`,
		Topics:           os.Getenv(`TOPICS`),
		Batch:            5,
	}).Input, []processors.Processor{
		{
			processKafkaMsgs,
			1,
		},
	}, Io.NewDiscard(errs).Output)
	go func() {
		for err := range errs {
			fmt.Println(err)
		}
	}()
	go p.Run()
	<-c

	cancel()

}

func processKafkaMsgs(eventsChan chan *events.Events) chan *events.Events {
	output := make(chan *events.Events)
	go func() {
		defer close(output)
		for es := range eventsChan {
			for _, e := range es.Data() {
				e.Data = string(e.Data.(*kafka.Message).Value)
			}
			output <- es
		}
	}()
	return output
}
