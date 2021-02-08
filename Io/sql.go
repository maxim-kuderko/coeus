package Io

import (
	"database/sql"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/maxim-kuderko/coeus/events"
	"sync"
)

type SQL struct {
	opt  *SQLOpt
	errs chan<- error
}

type SQLOpt struct {
	Endpoint         string
	Driver           string
	InsertIntoStmt   string
	EventToValueFunc func(event *events.Event) []interface{}
	MaxRetries       int
	Concurrency      int
}

func NewClickHouse(errs chan<- error, opt *SQLOpt) *SQL {
	return &SQL{opt: opt, errs: errs}
}

func (s *SQL) Output(eventsChan chan *events.Events) {
	connect, err := sql.Open(s.opt.Driver, s.opt.Endpoint)
	if err != nil {
		panic(err)
	}
	defer connect.Close()
	if err := connect.Ping(); err != nil {
		panic(err)
	}
	sem := make(chan bool, s.opt.Concurrency)
	wg := &sync.WaitGroup{}
	for es := range eventsChan {
		sem <- true
		wg.Add(1)
		go func(connect *sql.DB, es *events.Events) {
			defer func() {
				<-sem
				wg.Done()
			}()
			s.insertWithRetry(connect, es)
		}(connect, es)
	}
	wg.Wait()
}

func (s *SQL) insertWithRetry(connect *sql.DB, es *events.Events) {
	retries := 0
	failed := false
	for retries < s.opt.MaxRetries {
		trx, err := connect.Begin()
		if err != nil {
			s.errs <- err
			continue
		}

		tmpStmt, err := trx.Prepare(s.opt.InsertIntoStmt)
		if err != nil {
			s.errs <- err
			continue
		}
		for _, e := range es.Data() {
			if err != nil {
				s.errs <- err
				continue
			}
			_, err := tmpStmt.Exec(s.opt.EventToValueFunc(e)...)
			if err != nil {
				failed = true
				trx.Rollback()
				retries++
				s.errs <- err
				break
			}
		}
		if !failed {
			if err := trx.Commit(); err != nil {
				s.errs <- err
				continue
			}
			if err := es.Ack(); err != nil {
				s.errs <- err
			}
			break
		}
	}
}
