package Io

import (
	"bufio"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/maxim-kuderko/coeus/events"
	"io"
	"net/http"
	"time"
)

type S3 struct {
	sess *session.Session
	opt  *S3Opt
}

type S3Opt struct {
	Region string
	Bucket string
	Path   string
	reader func(r io.Reader) <-chan *events.Event
}

func NewS3(opt *S3Opt) *S3 {
	svc := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(opt.Region),
		//Credentials: credentials.NewStaticCredentials(key, secret, ""),
	}))
	return &S3{sess: svc, opt: opt}
}

func (s *S3) Input(n int, timeout time.Duration) (chan *events.Events, chan error) {
	output, errs := make(chan *events.Events, n), make(chan error, n)
	go func() {
		defer close(output)
		defer close(errs)
		c := *http.DefaultClient
		c.Timeout = timeout
		obj, err := s3.New(s.sess, &aws.Config{HTTPClient: &c}).GetObject(&s3.GetObjectInput{
			Bucket: aws.String(s.opt.Bucket),
			Key:    aws.String(s.opt.Path),
		})
		if err != nil {
			errs <- err
			return
		}
		defer obj.Body.Close()
		buff := make([]*events.Event, 0, n)
		for e := range s.opt.reader(obj.Body) {
			if len(buff) == n {
				s.flushbuffer(output, buff, n)
				continue
			}
			buff = append(buff, e)
		}
		if len(buff) > 0 {
			s.flushbuffer(output, buff, n)
		}
	}()

	return output, errs
}

func NewlineReader(r io.Reader) chan *events.Event {
	output := make(chan *events.Event, 2)
	go func() {
		defer close(output)
		b := bufio.NewReader(r)
		for {
			data, err := b.ReadBytes('\n')
			if err != nil {
				return
			}
			output <- &events.Event{Data: data}
		}
	}()
	return output
}

func (s *S3) flushbuffer(output chan *events.Events, buff []*events.Event, n int) {
	output <- events.NewEvents(func() error {
		return nil
	}, buff)
	buff = make([]*events.Event, 0, n)
}

func (s *S3) Output(events chan *events.Events) {
	for e := range events {
		if err := e.Ack(); err != nil {
			continue
		}
	}
}
