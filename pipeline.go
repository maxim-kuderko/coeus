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

func NewPipeline(input Io.Input, processors [][]processors.Processor, output Io.Output) (*Pipeline, error) {
	p := &Pipeline{
		input:      input,
		output:     output,
		processors: processors,
	}
	return p, nil
}

func (p *Pipeline) Run() {
	input := p.input()
	for _, procGroup := range p.processors {
		output := make(chan *events.Events, len(procGroup)*2)
		tmp := make([]chan *events.Events, 0, len(procGroup))
		for _, proc := range procGroup {
			tmp = append(tmp, proc(input))
		}
		go func(output chan *events.Events, tmp []chan *events.Events) {
			defer close(output)
			fanIn(output, tmp...)
		}(output, tmp)
		input = output
	}
	p.output(input)
}
