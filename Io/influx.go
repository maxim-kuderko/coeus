package Io

import (
	inflx "github.com/influxdata/influxdb-client-go"
	"github.com/influxdata/influxdb-client-go/api/write"
	"github.com/maxim-kuderko/coeus/events"
	"time"
)

type InfluxDB struct {
	client inflx.Client
	opt    *InfluxDBOpt
	errs   chan<- error
}

type InfluxDBOpt struct {
	Endpoint, Token, Org, Bucket string
	Timeout                      time.Duration
}

func NewInfluxDB(errs chan<- error, opt *InfluxDBOpt) *InfluxDB {
	return &InfluxDB{client: inflx.NewClient(opt.Endpoint, opt.Token), opt: opt, errs: errs}
}

func (s *InfluxDB) Output(events chan *events.Events) {
	defer s.client.Close()
	w := s.client.WriteAPI(s.opt.Org, s.opt.Bucket)
	go func() {
		for err := range w.Errors() {
			s.errs <- err
		}
	}()
	for es := range events {
		for _, e := range es.Data() {
			w.WritePoint(e.Data.(*write.Point))
		}
		if err := es.Ack(); err != nil {
			s.errs <- err
		}
	}
}
