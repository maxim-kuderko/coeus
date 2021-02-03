package coeus

import (
	"context"
	"github.com/maxim-kuderko/coeus/drivers"
	"github.com/maxim-kuderko/coeus/events"
	"sync"
	"time"
)

type Pipeline struct {
	input      drivers.Input
	output     drivers.Output
	processors []Processor
	opt        *Opt

	ctx context.Context

	wg sync.WaitGroup
}
type Processor func(events chan *events.Events) chan *events.Events

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
func (p *Pipeline) Wait() {
	p.wg.Wait()
}

func (p *Pipeline) run(ctx context.Context) chan error {
	errs := make(chan error, p.opt.ConcurrentWorkers+p.opt.ConcurrentOutputters)
	input, inputErrs := p.input.Next(ctx, p.opt.BulkSize, p.opt.BulkTimeout)
	go fanIn(errs, inputErrs)
	output := make(chan *events.Events)
	p.wg.Add(2)
	go func() {
		defer p.wg.Done()
		defer close(output)
		p.processPool(output, input)
	}()
	go func() {
		defer p.wg.Done()
		defer close(errs)
		p.storePool(output, errs)
	}()
	return errs
}

func (p *Pipeline) storePool(output chan *events.Events, errorsOutput chan error) {
	wg := sync.WaitGroup{}
	wg.Add(p.opt.ConcurrentOutputters)
	for i := 0; i < p.opt.ConcurrentOutputters; i++ {
		go func() {
			defer wg.Done()
			fanIn(errorsOutput, p.output.Store(output))
		}()
	}
	wg.Wait()

}

func (p *Pipeline) processPool(output chan *events.Events, input chan *events.Events) {
	wg := sync.WaitGroup{}
	wg.Add(p.opt.ConcurrentWorkers)
	for i := 0; i < p.opt.ConcurrentWorkers; i++ {
		go func() {
			defer wg.Done()
			p.process(output, input)
		}()
	}
	wg.Wait()
}

func (p *Pipeline) process(output, input chan *events.Events) {
	if len(p.processors) == 0 {
		for e := range input {
			output <- e
		}
		return
	}
	var pipeline Processor
	for _, proc := range p.processors {
		if pipeline == nil {
			pipeline = proc
			continue
		}
		tmp := func(events chan *events.Events) chan *events.Events {
			return proc(pipeline(events))
		}
		pipeline = tmp

	}
	for e := range pipeline(input) {
		output <- e
	}
}

func validate(opt *Opt) (*Opt, error) {
	return opt, nil
}
