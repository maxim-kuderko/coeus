package coeus

import (
	"context"
	"github.com/maxim-kuderko/coeus/drivers"
	"sync"
	"time"
)

type Pipeline struct {
	input      drivers.Input
	output     drivers.Output
	processors []Processor
	opt        *Opt

	ctx context.Context
}
type Processor func(events chan *drivers.Event) chan *drivers.Event
type MultiProcessor func(events chan *drivers.Events) chan *drivers.Events

type Opt struct {
	Bulk        bool
	BulkTimeout time.Duration

	ConcurrentWorkers    int
	ConcurrentOutputters int
}

func NewPipeline(input drivers.Input, output drivers.Output, opt *Opt, processors ...Processor) (*Pipeline, error) {
	opt, err := validate(opt)
	if err != nil {
		return nil, err
	}

	p := &Pipeline{
		input:      input,
		output:     output,
		processors: processors,
		opt:        opt,
	}
	return p, nil
}

func (p *Pipeline) Run(ctx context.Context) chan error {
	if p.opt.Bulk {
		return p.bulkRun(ctx)
	}
	return p.run(ctx)
}

func (p *Pipeline) run(ctx context.Context) chan error {
	input, errs := p.input.Next(ctx)
	output := make(chan *drivers.Event)
	go p.processPool(output, input)
	go p.storePool(output)
	return errs
}

func (p *Pipeline) storePool(output chan *drivers.Event) {
	wg := sync.WaitGroup{}
	wg.Add(p.opt.ConcurrentOutputters)
	for i := 0; i < p.opt.ConcurrentOutputters; i++ {
		go func() {
			wg.Done()
			p.output.Store(output)
		}()
	}
	wg.Wait()

}

func (p *Pipeline) processPool(output chan *drivers.Event, input chan *drivers.Event) {
	defer close(output)
	wg := sync.WaitGroup{}
	wg.Add(p.opt.ConcurrentWorkers)
	for i := 0; i < p.opt.ConcurrentWorkers; i++ {
		go func() {
			wg.Done()
			p.process(input, output)
		}()
	}
	wg.Wait()
}

func (p *Pipeline) bulkRun(ctx context.Context) chan error {

}

func (p *Pipeline) process(input, output chan *drivers.Event) chan *drivers.Events {

}

func validate(opt *Opt) (*Opt, error) {
	return opt, nil
}
