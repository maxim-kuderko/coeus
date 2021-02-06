package main

import (
	"context"
	"fmt"
	"github.com/influxdata/influxdb-client-go/api/write"
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
		Timeout:  time.Millisecond * 100,
		Count:    1,
	}).Input, []processors.Processor{
		{

			sqsParse(errs),
			8,
		},
		{
			sqsProcessor(errs),
			8,
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
					if err := jsoniter.ConfigFastest.Unmarshal(es.Data()[0].Data.([]byte), &tmp); err != nil {
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

func sqsProcessor(errs chan error) func(eventsChan chan *events.Events) chan *events.Events {
	return func(eventsChan chan *events.Events) chan *events.Events {
		output := make(chan *events.Events)
		go func() {
			defer close(output)
			for es := range eventsChan {
				for _, e := range es.Data() {
					file := e.Data.(S3SqsEvent).Records[0]
					if err := processFile(file.AwsRegion, file.S3.Bucket.Name, file.S3.Object.Key, errs); err != nil {
						errs <- err
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
		[]processors.Processor{
			{
				processS3File,
				8,
			},
			{
				Func: func(eventsChan chan *events.Events) chan *events.Events {
					output := make(chan *events.Events)
					agg := map[string]*struct {
						E     Event
						Count int
					}{}
					acks := make([]func() error, 0)
					go func() {
						defer close(output)
						for es := range eventsChan {
							acks = append(acks, es.Ack)
							for _, e := range es.Data() {
								d := e.Data.(Event)
								key := d.Data[`customer_id`] + d.Data[`campign`] + d.Data[`action`] + d.Data[`user_id`]
								v, ok := agg[key]
								if !ok {
									v = &struct {
										E     Event
										Count int
									}{E: d, Count: 1}
									agg[key] = v
									continue
								}
								v.Count++
							}
						}
						es := make([]*events.Event, 0, len(agg))
						for _, v := range agg {
							es = append(es, &events.Event{
								Data: v,
							})
						}
						output <- events.NewEvents(func() error {
							for _, ack := range acks {
								if err := ack(); err != nil {
									return err
								}
							}
							return nil
						}, es)
					}()
					return output
				},
				Concurrency: 8,
			},
			{
				func(eventsChan chan *events.Events) chan *events.Events {
					output := make(chan *events.Events)
					go func() {
						defer close(output)
						for es := range eventsChan {
							for _, e := range es.Data() {
								parsed := e.Data.(*struct {
									E     Event
									Count int
								})
								e.Data = write.NewPoint(`events`, parsed.E.Data, map[string]interface{}{`count`: parsed.Count}, time.Now())
							}
							output <- es
						}
					}()

					return output
				},
				8,
			},
		},
		Io.NewInfluxDB(errs, &Io.InfluxDBOpt{
			Endpoint: "http://localhost:8086",
			Token:    "jZaceKailVqqnFNVnazmWVV9bFlNFfRc9xiXIX_xrW4UODSPBO_c1lWFq7hV3JbkW33UTXzeYJyW7Vj51vbe7Q==",
			Org:      "org",
			Bucket:   "bucket",
			Timeout:  time.Millisecond * 500,
		}).Output)
	p.Run()
	return nil
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

func processS3File(eeee chan *events.Events) chan *events.Events {
	output := make(chan *events.Events)
	go func(eventsChannel chan *events.Events) {
		defer close(output)
		for es := range eventsChannel {
			for _, e := range es.Data() {
				var tmp Event
				if err := jsoniter.ConfigFastest.Unmarshal(e.Data.([]byte), &tmp); err != nil {
					continue
				}
				e.Data = tmp
			}

			output <- es
		}
	}(eeee)
	return output
}

type Event struct {
	Metadata Metadata          `json:"metadata"`
	Data     map[string]string `json:"raw_data"`
}

type Enrichment struct {
	Headers map[string]string
}

type Metadata struct {
	WrittenAt string
}
