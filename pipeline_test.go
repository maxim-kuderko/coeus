package coeus

import (
	"context"
	"github.com/maxim-kuderko/coeus/drivers"
	"github.com/maxim-kuderko/coeus/events"
	"testing"
	"time"
)

func TestNewPipeline(t *testing.T) {
	count := 100000
	type args struct {
		stub       *drivers.Stub
		opt        *Opt
		processors []Processor
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: `basic`,
			args: args{
				stub: drivers.NewStub(count),
				opt: &Opt{
					BulkSize:             1,
					BulkTimeout:          time.Second,
					ConcurrentWorkers:    1,
					ConcurrentOutputters: 1,
				},
				processors: nil,
			},
		},
		{
			name: `basic with processor`,
			args: args{
				stub: drivers.NewStub(count),
				opt: &Opt{
					BulkSize:             1,
					BulkTimeout:          time.Second,
					ConcurrentWorkers:    1,
					ConcurrentOutputters: 1,
				},
				processors: []Processor{func(events chan *events.Events) chan *events.Events {
					return events
				}},
			},
		},
		{
			name: `concurrent with processor`,
			args: args{
				stub: drivers.NewStub(count),
				opt: &Opt{
					BulkSize:             1,
					BulkTimeout:          time.Second,
					ConcurrentWorkers:    20,
					ConcurrentOutputters: 1,
				},
				processors: []Processor{func(events chan *events.Events) chan *events.Events {
					return events
				}},
			},
		},
		{
			name: `concurrent with processor with bulk`,
			args: args{
				stub: drivers.NewStub(count),
				opt: &Opt{
					BulkSize:             20,
					BulkTimeout:          time.Second,
					ConcurrentWorkers:    200,
					ConcurrentOutputters: 20,
				},
				processors: []Processor{func(events chan *events.Events) chan *events.Events {
					return events
				}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, _ := NewPipeline(tt.args.stub, tt.args.stub, tt.args.opt, tt.args.processors...)
			pipeline.Run(context.Background())
			pipeline.Wait()
			if tt.args.stub.OutputCount() != count {
				t.Errorf(`expected 1000 count, got %d`, tt.args.stub.OutputCount())
			}
		})
	}
}
