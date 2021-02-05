package Io

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	jsoniter "github.com/json-iterator/go"
	"github.com/maxim-kuderko/coeus/events"
	"time"
)

type Sqs struct {
	client *sqs.SQS
	opt    *SqsOpt
	errs   chan<- error
	ctx    context.Context
}

type SqsOpt struct {
	Region   string
	Endpoint string
	Timeout  time.Duration
	Count    int64
}

func NewSqs(ctx context.Context, errs chan<- error, opt *SqsOpt) *Sqs {
	svc := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(opt.Region),
		//Credentials: credentials.NewStaticCredentials(key, secret, ""),
	}))
	client := sqs.New(svc)
	return &Sqs{client: client, opt: opt, errs: errs, ctx: ctx}
}

func (s *Sqs) Input() chan *events.Events {
	output := make(chan *events.Events)
	go func() {
		defer close(output)
		for {
			select {
			case <-s.ctx.Done():
				return
			default:
				resp, err := s.client.ReceiveMessage(&sqs.ReceiveMessageInput{
					MaxNumberOfMessages: aws.Int64(s.opt.Count),
					QueueUrl:            aws.String(s.opt.Endpoint),
					WaitTimeSeconds:     aws.Int64(int64(s.opt.Timeout.Seconds())),
				})
				if err != nil {
					s.errs <- err
					continue
				}
				if len(resp.Messages) == 0 {
					continue
				}
				es := make([]*events.Event, 0, len(resp.Messages))
				msgsAckIds := make([]*sqs.DeleteMessageBatchRequestEntry, 0, len(resp.Messages))
				for _, e := range resp.Messages {
					es = append(es, &events.Event{
						Data:     []byte(*e.Body),
						Metadata: *e.MD5OfBody,
					})
					msgsAckIds = append(msgsAckIds, &sqs.DeleteMessageBatchRequestEntry{
						Id:            e.MessageId,
						ReceiptHandle: e.ReceiptHandle,
					})
				}

				output <- events.NewEvents(func() error {
					_, err := s.client.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
						Entries:  msgsAckIds,
						QueueUrl: aws.String(s.opt.Endpoint),
					})
					return err
				}, es)

			}
		}

	}()

	return output
}

func (s *Sqs) Output(events chan *events.Events) {
	for e := range events {
		payloads := make([]*sqs.SendMessageBatchRequestEntry, 0, len(e.Data()))
		for _, d := range e.Data() {
			var data *string
			switch v := d.Data.(type) {
			case []byte:
				data = aws.String(string(v))
			case string:
				data = aws.String(v)
			default:
				b, err := jsoniter.ConfigFastest.Marshal(v)
				if err != nil {
					s.errs <- err
					continue
				}
				data = aws.String(string(b))
			}
			payloads = append(payloads, &sqs.SendMessageBatchRequestEntry{
				MessageBody: data,
			})
		}
		_, err := s.client.SendMessageBatch(&sqs.SendMessageBatchInput{
			Entries:  payloads,
			QueueUrl: aws.String(s.opt.Endpoint),
		})
		if err != nil {
			s.errs <- err
			continue
		}
		if err = e.Ack(); err != nil {
			s.errs <- err
		}
	}
}
