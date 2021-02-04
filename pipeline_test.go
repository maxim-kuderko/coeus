package coeus

import (
	"fmt"
	"github.com/maxim-kuderko/coeus/Io"
	"github.com/maxim-kuderko/coeus/events"
	"github.com/maxim-kuderko/coeus/processors"
	"testing"
)

func TestPipeline_Run(t *testing.T) {
	n := 10000
	type fields struct {
		input      Io.Input
		processors [][]processors.Processor
		output     Io.Output
	}
	st := Io.NewStub(n)
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: `basic`,
			fields: fields{
				input: st.Input,
				processors: [][]processors.Processor{
					{
						processors.Stub,
					},
				},
				output: st.Output,
			},
		},
		{
			name: `basic mutli proc`,
			fields: fields{
				input: st.Input,
				processors: [][]processors.Processor{
					{
						processors.Stub,
						processors.Stub,
						processors.Stub,
						processors.Stub,
					},
				},
				output: st.Output,
			},
		},
		{
			name: `basic multi mutli proc`,
			fields: fields{
				input: st.Input,
				processors: [][]processors.Processor{
					{
						processors.Stub,
						processors.Stub,
						processors.Stub,
					},
					{
						processors.Stub,
						processors.Stub,
					},

					{
						processors.Stub,
					},
				},
				output: st.Output,
			},
		},
		{
			name: `basic sum`,
			fields: fields{
				input: st.Input,
				processors: [][]processors.Processor{
					{
						processors.Sum(func(events2 *events.Events) int64 {
							output := int64(0)
							for _, es := range events2.Data() {
								output += int64(es.Data.(int))
							}
							return output
						}),
						processors.Sum(func(events2 *events.Events) int64 {
							output := int64(0)
							for _, es := range events2.Data() {
								output += int64(es.Data.(int))
							}
							return output
						}),
					},
					{
						processors.Stub,
					},
				},
				output: st.Output,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pipeline{
				input:      tt.fields.input,
				processors: tt.fields.processors,
				output:     tt.fields.output,
			}
			p.Run()
			fmt.Println(st.OutputCount())
		})
	}
}
