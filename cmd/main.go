package main

import (
	"context"
	"encoding/json"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/maxim-kuderko/coeus"
	"github.com/maxim-kuderko/coeus/Io"
	"github.com/maxim-kuderko/coeus/events"
	"github.com/maxim-kuderko/coeus/processors"
	"net/url"
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

	p := coeus.NewPipeline(Io.NewSqs(ctx, errs, &Io.SqsOpt{
		Region:   os.Getenv(`AWS_REGION`),
		Endpoint: os.Getenv(`SQS_INPUT`),
		Timeout:  time.Second,
		Count:    10,
	}).Input, [][]processors.Processor{
		{
			sqsParse(errs),
			sqsParse(errs),
			sqsParse(errs),
			sqsParse(errs),
		},
		{
			sqsProcessor(errs),
		},
	}, Io.NewDiscard(errs).Store)
	go func() {
		for err := range errs {
			fmt.Println(err)
		}
	}()
	go p.Run()
	<-c

	cancel()

}

func sqsParse(errs chan error) func(eventsChan chan *events.Events) chan *events.Events {
	return func(eventsChan chan *events.Events) chan *events.Events {
		output := make(chan *events.Events)
		go func() {
			defer close(output)
			for es := range eventsChan {
				for _, e := range es.Data() {
					var tmp S3SqsEvent
					if err := jsoniter.ConfigFastest.Unmarshal(e.Data.([]byte), &tmp); err != nil {
						errs <- err
						continue
					}
					e.Data = tmp
				}
				output <- es
			}
		}()
		return output
	}
}

var dedupe = map[string]bool{}

func sqsProcessor(errs chan error) func(eventsChan chan *events.Events) chan *events.Events {
	return func(eventsChan chan *events.Events) chan *events.Events {
		output := make(chan *events.Events)
		dedupe := map[string]bool{}
		go func() {
			defer close(output)
			for es := range eventsChan {
				for _, e := range es.Data() {
					file := e.Data.(S3SqsEvent).Records[0]
					if ok := dedupe[file.S3.Object.Key]; !ok {
						if err := processFile(file.AwsRegion, file.S3.Bucket.Name, file.S3.Object.Key, errs); err != nil {
							errs <- err
						}
						dedupe[file.S3.Object.Key] = true
					}

				}
				output <- es
			}

		}()

		return output
	}

}

func processFile(region, bucket, key string, errs chan<- error) error {
	unescapedUrl, _ := url.QueryUnescape(key)
	p := coeus.NewPipeline(Io.NewS3(errs, &Io.S3Opt{
		Region: region,
		Bucket: bucket,
		Path:   unescapedUrl,
		Reader: Io.NewlineGzipReader,
	}).Input,
		[][]processors.Processor{
			{
				processS3File,
				processS3File,
				processS3File,
				processS3File,
				processS3File,
				processS3File,
				processS3File,
				processS3File,
				processS3File,
				processS3File,
			},
			{
				processors.Count(func(events2 *events.Events) int64 {
					return int64(len(events2.Data()))
				}),
			},
		},
		Io.NewStdOut(errs).Output)
	p.Run()
	return nil
}

type S3SqsEvent struct {
	Records []struct {
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

func processS3File(eeee chan *events.Events) chan *events.Events {
	output := make(chan *events.Events)
	go func(eventsChannel chan *events.Events) {
		defer close(output)
		t := int64(0)
		c := int64(0)
		for es := range eventsChannel {
			for _, e := range es.Data() {
				var tmp Event
				if err := jsoniter.ConfigFastest.Unmarshal(e.Data.([]byte), &tmp); err != nil {
					continue
				}
				elapsedTime, _ := time.Parse(time.RFC3339, tmp.Metadata.WrittenAt)
				t += time.Now().Sub(elapsedTime).Milliseconds()
				c++
				e.Data = tmp
			}
			output <- es
		}
		fmt.Println(`avg latency: `, float64(t)/float64(c))
	}(eeee)
	return output
}

type Event struct {
	RawData    json.RawMessage `json:"raw_data"`
	Enrichment Enrichment      `json:"enrichment"`
	Metadata   Metadata        `json:"metadata"`
}

type Enrichment struct {
	Headers map[string]string
}

type Metadata struct {
	WrittenAt string
	RequestID string
}
