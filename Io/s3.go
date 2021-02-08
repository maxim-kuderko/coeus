package Io

import (
	"bufio"
	"compress/gzip"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/maxim-kuderko/coeus/events"
	"io"
)

type S3 struct {
	sess *session.Session
	opt  *S3Opt
	errs chan<- error
}

type S3Opt struct {
	Region string
	Bucket string
	Path   string
	Reader func(r io.Reader, metadata interface{}) <-chan *events.Event

	Batch int
}

func NewS3(errs chan<- error, opt *S3Opt) *S3 {
	svc := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(opt.Region),
		//Credentials: credentials.NewStaticCredentials(key, secret, ""),
	}))
	return &S3{sess: svc, opt: opt, errs: errs}
}

func (s *S3) Input() chan *events.Events {
	output := make(chan *events.Events, s.opt.Batch)
	go func() {
		defer close(output)
		/*t := time.Now()
		defer func() {
			fmt.Println("file read time took:", time.Since(t))
		}()*/
		obj, err := s3.New(s.sess).GetObject(&s3.GetObjectInput{
			Bucket: aws.String(s.opt.Bucket),
			Key:    aws.String(s.opt.Path),
		})
		if err != nil {
			s.errs <- err
			return
		}
		defer obj.Body.Close()
		buff := make([]*events.Event, 0, s.opt.Batch)
		for e := range s.opt.Reader(obj.Body, map[string]string{`path`: s.opt.Path}) {
			buff = append(buff, e)
			if len(buff) == s.opt.Batch {
				output <- events.NewEvents(func() error {
					return nil
				}, buff)
				buff = make([]*events.Event, 0, s.opt.Batch)
			}
		}
		if len(buff) > 0 {
			output <- events.NewEvents(func() error {
				return nil
			}, buff)
		}
	}()

	return output
}

func NewlineGzipReader(r io.Reader, metadata interface{}) <-chan *events.Event {
	output := make(chan *events.Event, 2)
	go func() {
		defer close(output)
		gr, _ := gzip.NewReader(r)
		b := bufio.NewReader(gr)
		for {
			data, err := b.ReadBytes('\n')
			if err != nil {
				return
			}
			output <- &events.Event{Data: data, Metadata: metadata}
		}
	}()
	return output
}

func (s *S3) Output(events chan *events.Events) {
	for e := range events {
		if err := e.Ack(); err != nil {
			continue
		}
	}
}
