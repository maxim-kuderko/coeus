package coeus

import (
	"github.com/maxim-kuderko/coeus/Io"
	"github.com/maxim-kuderko/coeus/events"
	"github.com/maxim-kuderko/coeus/processors"
)

type Pipeline struct {
	input      Io.Input
	processors [][]processors.Processor
	output     Io.Output
}

func NewPipeline(input Io.Input, processors [][]processors.Processor, output Io.Output) *Pipeline {
	p := &Pipeline{
		input:      input,
		output:     output,
		processors: processors,
	}
	return p
}

func (p *Pipeline) Run() {
	input := p.input()
	for _, procGroup := range p.processors {
		output := make(chan *events.Events, len(procGroup)*2)
		tmp := make([]chan *events.Events, 0, len(procGroup))
		for _, proc := range procGroup {
			tmp = append(tmp, proc(input))
		}
		input = output
		go func(o chan *events.Events, t []chan *events.Events) {
			defer close(o)
			fanIn(o, t...)
		}(output, tmp)
	}
	p.output(input)
}
