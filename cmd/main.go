package main

import (
	"context"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go"
	jsoniter "github.com/json-iterator/go"
	"github.com/maxim-kuderko/coeus"
	"github.com/maxim-kuderko/coeus/Io"
	"github.com/maxim-kuderko/coeus/events"
	"github.com/maxim-kuderko/coeus/processors"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"
)

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 100)
	go handleErrors(errs)

	go coeus.NewPipeline(
		Io.NewSqs(ctx, errs, &Io.SqsOpt{
			Region:   os.Getenv(`AWS_REGION`),
			Endpoint: os.Getenv(`SQS_INPUT`),
			Timeout:  time.Second,
			Count:    1,
		}).Input,
		[]processors.Processor{
			{sqsTos3File(errs), 1},
			{processPlutosMsg, runtime.NumCPU()},
		},
		initClickhouse(errs).Output).Run()

	<-c
	cancel()

}

func handleErrors(errs chan error) {
	for err := range errs {
		fmt.Println(err)
	}
}

func initClickhouse(errs chan error) *Io.SQL {
	return Io.NewClickHouse(errs, &Io.SQLOpt{
		Driver:           `clickhouse`,
		Concurrency:      16,
		Endpoint:         "tcp://localhost:9000",
		InsertIntoStmt:   "insert into default.ad_calls (request_id, customer_id, campaign, action, user_id, date, sent_at, written_at) values (?, ?, ?, ?, ?, ?, ?, ?) on duplicate key",
		EventToValueFunc: eventToValues,
		MaxRetries:       1,
	})
}

func sqsTos3File(errs chan error) func(eventsChan chan *events.Events) chan *events.Events {
	return func(eventsChan chan *events.Events) chan *events.Events {
		output := make(chan *events.Events, 1000)
		go func() {
			defer close(output)
			for msg := range eventsChan {
				var sqsE S3SqsEvent
				if err := jsoniter.ConfigFastest.Unmarshal(msg.Data()[0].Data.([]byte), &sqsE); err != nil {
					errs <- err
					continue
				}
				readS3File(errs, sqsE, output, msg)
			}
		}()
		return output
	}
}

func readS3File(errs chan error, sqsE S3SqsEvent, output chan *events.Events, msg *events.Events) {
	s3 := Io.NewS3(errs, &Io.S3Opt{
		Region: sqsE.Records[0].AwsRegion,
		Bucket: sqsE.Records[0].S3.Bucket.Name,
		Path:   sqsE.Records[0].S3.Object.Key,
		Reader: Io.NewlineZSTDReader,
		Batch:  10000,
	}).Input()
	wg := sync.WaitGroup{}
	for e := range s3 {
		wg.Add(1)
		output <- events.NewEvents(func() error {
			wg.Done()
			return nil
		}, e.Data())
	}
	go func(msg *events.Events) {
		wg.Wait()
		if err := msg.Ack(); err != nil {
			errs <- err
		}
	}(msg)
}

func processPlutosMsg(eventsChan chan *events.Events) chan *events.Events {
	output := make(chan *events.Events)
	go func() {
		defer close(output)
		for es := range eventsChan {
			ok := true
			for _, e := range es.Data() {
				var plutosEvent PlutosEvent
				if err := jsoniter.ConfigFastest.Unmarshal(e.Data.([]byte), &plutosEvent); err != nil {
					fmt.Println(string(e.Data.([]byte)), err)
					ok = false
					break
				}
				e.Data = &plutosEvent
			}
			if ok {
				output <- es
			}
		}
	}()
	return output
}

func eventToValues(event *events.Event) []interface{} {
	pe := event.Data.(*PlutosEvent)
	ts, _ := time.Parse(time.RFC3339Nano, pe.WrittenAt)
	t := time.Now()
	output := make([]interface{}, 8)
	output[0] = pe.RequestID
	output[1] = pe.RawData[`customer_id`]
	output[2] = pe.RawData[`campaign`]
	output[3] = pe.RawData[`action`]
	output[4] = pe.RawData[`user_id`]
	output[5] = t
	output[6] = ts
	output[7] = t
	return output
}

type PlutosEvent struct {
	RawData   map[string]string `json:"raw_data"`
	WrittenAt string            `json:"written_at"`
	RequestID string            `json:"request_id"`
}

type S3SqsEvent struct {
	WrittenAt string `json:"written_at"`
	Records   []struct {
		AwsRegion string `json:"awsRegion"`
		S3        struct {
			Bucket struct {
				Name string `json:"name"`
			} `json:"bucket"`
			Object struct {
				Key  string `json:"key"`
				Size int    `json:"size"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}
