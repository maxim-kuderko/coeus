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
type Processor func(events chan *drivers.Events) chan *drivers.Events

type Opt struct {
	BulkSize    int
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
	return p.run(ctx)
}

func (p *Pipeline) run(ctx context.Context) chan error {
	errs := make(chan error, p.opt.ConcurrentWorkers+p.opt.ConcurrentOutputters)
	input, inputErrs := p.input.Next(ctx, p.opt.BulkSize, p.opt.BulkTimeout)
	go fanIn(errs, inputErrs)
	output := make(chan *drivers.Events)
	go func() {
		defer close(output)
		p.processPool(output, input)
	}()
	go func() {
		defer close(errs)
		p.storePool(output, errs)
	}()
	return errs
}

func (p *Pipeline) storePool(output chan *drivers.Events, errorsOutput chan error) {
	wg := sync.WaitGroup{}
	wg.Add(p.opt.ConcurrentOutputters)
	for i := 0; i < p.opt.ConcurrentOutputters; i++ {
		go func() {
			wg.Done()
			fanIn(errorsOutput, p.output.Store(output))
		}()
	}
	wg.Wait()

}

func (p *Pipeline) processPool(output chan *drivers.Events, input chan *drivers.Events) {
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

func (p *Pipeline) process(input, output chan *drivers.Events) {
	var pipeline Processor
	for _, proc := range p.processors {
		pipeline = func(events chan *drivers.Events) chan *drivers.Events {
			return proc(events)
		}
	}
	if pipeline == nil {
		return
	}
	for e := range pipeline(input) {
		output <- e
	}
}

func validate(opt *Opt) (*Opt, error) {
	return opt, nil
}
