package drivers

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/maxim-kuderko/coeus/events"
	"sync"
	"time"
)

type Sqs struct {
	client *sqs.SQS
	opt    *SqsOpt
}

type SqsOpt struct {
	Region   string
	Endpoint string
}

func NewSqs(opt *SqsOpt) *Sqs {
	svc := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(opt.Region),
		//Credentials: credentials.NewStaticCredentials(key, secret, ""),
	}))
	client := sqs.New(svc)
	return &Sqs{client: client, opt: opt}
}

func (s *Sqs) Next(ctx context.Context, n int, timeout time.Duration) (chan *events.Events, chan error) {
	output, errs := make(chan *events.Events), make(chan error)
	go func() {
		defer close(output)
		defer close(errs)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				resp, err := s.client.ReceiveMessage(&sqs.ReceiveMessageInput{
					MaxNumberOfMessages: aws.Int64(int64(n)),
					QueueUrl:            aws.String(s.opt.Endpoint),
					WaitTimeSeconds:     aws.Int64(int64(timeout.Seconds())),
				})
				if err != nil {
					errs <- err
					continue
				}
				es := make([]*events.Event, 0, len(resp.Messages))
				msgsAckIds := make([]*sqs.DeleteMessageBatchRequestEntry, 0, len(resp.Messages))
				for _, e := range resp.Messages {
					es = append(es, &events.Event{
						ID:   *e.MessageId,
						Data: []byte(*e.Body),
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

	return output, errs
}

type sqsAcker struct {
	sqs   *sqs.SQS
	errs  chan error
	queue *string

	buff []*sqs.DeleteMessageBatchRequestEntry

	mu sync.Mutex
}
