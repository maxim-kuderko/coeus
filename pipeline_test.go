package coeus

import (
	"github.com/maxim-kuderko/coeus/Io"
	"github.com/maxim-kuderko/coeus/events"
	"github.com/maxim-kuderko/coeus/processors"
	"testing"
)

func TestPipeline_Run(t *testing.T) {
	n := 1000000
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Pipeline{
				input:      tt.fields.input,
				processors: tt.fields.processors,
				output:     tt.fields.output,
			}
			p.Run()
			if st.OutputCount() != n {
				t.Errorf(`wrong count want %d got %d`, n, st.OutputCount())
			}
			st.Reset()
		})
	}
}
func TestPipeline_RunAggSum(t *testing.T) {
	n := 10000
	type fields struct {
		input        Io.Input
		processors   [][]processors.Processor
		output       Io.Output
		outputNumber int64
	}
	st := Io.NewStub(n)
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: `basic sum`,
			fields: fields{
				input: st.Input,
				processors: [][]processors.Processor{
					{
						processors.Sum(func(events2 events.Events) int64 {
							output := int64(0)
							for _, es := range events2.Data() {
								output += es.Data.(int64)
							}
							return output
						}),
					},
				},
				output:       st.Output,
				outputNumber: int64(n * (0 + (n - 1)) / 2),
			},
		},
		{
			name: `basic concurrent sum`,
			fields: fields{
				input: st.Input,
				processors: [][]processors.Processor{
					{
						processors.Sum(func(events2 events.Events) int64 {
							output := int64(0)
							for _, es := range events2.Data() {
								output += es.Data.(int64)
							}
							return output
						}),
						processors.Sum(func(events2 events.Events) int64 {
							output := int64(0)
							for _, es := range events2.Data() {
								output += es.Data.(int64)
							}
							return output
						}),
						processors.Sum(func(events2 events.Events) int64 {
							output := int64(0)
							for _, es := range events2.Data() {
								output += es.Data.(int64)
							}
							return output
						}),
						processors.Sum(func(events2 events.Events) int64 {
							output := int64(0)
							for _, es := range events2.Data() {
								output += es.Data.(int64)
							}
							return output
						}),
						processors.Sum(func(events2 events.Events) int64 {
							output := int64(0)
							for _, es := range events2.Data() {
								output += es.Data.(int64)
							}
							return output
						}),
					},
				},
				output:       st.Output,
				outputNumber: int64(n * (0 + (n - 1)) / 2),
			},
		},
		{
			name: `multistage sum`,
			fields: fields{
				input: st.Input,
				processors: [][]processors.Processor{
					{
						processors.Sum(func(events2 events.Events) int64 {
							output := int64(0)
							for _, es := range events2.Data() {
								output += es.Data.(int64)
							}
							return output
						}),
					},
					{
						processors.Sum(func(events2 events.Events) int64 {
							output := int64(0)
							for _, es := range events2.Data() {
								output += es.Data.(int64)
							}
							return output
						}),
					},
				},
				output:       st.Output,
				outputNumber: int64(n * (0 + (n - 1)) / 2),
			},
		},
		{
			name: `multistage concurrent sum`,
			fields: fields{
				input: st.Input,
				processors: [][]processors.Processor{
					{
						processors.Sum(func(events2 events.Events) int64 {
							output := int64(0)
							for _, es := range events2.Data() {
								output += es.Data.(int64)
							}
							return output
						}),
						processors.Sum(func(events2 events.Events) int64 {
							output := int64(0)
							for _, es := range events2.Data() {
								output += es.Data.(int64)
							}
							return output
						}),
						processors.Sum(func(events2 events.Events) int64 {
							output := int64(0)
							for _, es := range events2.Data() {
								output += es.Data.(int64)
							}
							return output
						}),
					},
					{
						processors.Sum(func(events2 events.Events) int64 {
							output := int64(0)
							for _, es := range events2.Data() {
								output += es.Data.(int64)
							}
							return output
						}),
						processors.Sum(func(events2 events.Events) int64 {
							output := int64(0)
							for _, es := range events2.Data() {
								output += es.Data.(int64)
							}
							return output
						}),
					},
					{
						processors.Sum(func(events2 events.Events) int64 {
							output := int64(0)
							for _, es := range events2.Data() {
								output += es.Data.(int64)
							}
							return output
						}),
					},
				},
				output:       st.Output,
				outputNumber: int64(n * (0 + (n - 1)) / 2),
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
			result := int64(0)

			for _, e := range st.OutputEvents() {
				result += e.Data.(int64)
			}
			if result != tt.fields.outputNumber {
				t.Errorf(`wrong count want %d got %d`, tt.fields.outputNumber, result)
			}
			st.Reset()
		})
	}
}

func TestPipeline_RunAggCount(t *testing.T) {
	n := 10000
	type fields struct {
		input        Io.Input
		processors   [][]processors.Processor
		output       Io.Output
		outputNumber int64
	}
	st := Io.NewStub(n)
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: `basic count`,
			fields: fields{
				input: st.Input,
				processors: [][]processors.Processor{
					{
						processors.Count(func(events2 events.Events) int64 {
							output := int64(0)
							for range events2.Data() {
								output += 1
							}
							return output
						}),
					},
				},
				output:       st.Output,
				outputNumber: int64(n),
			},
		},
		{
			name: `basic count concurrent`,
			fields: fields{
				input: st.Input,
				processors: [][]processors.Processor{
					{
						processors.Count(func(events2 events.Events) int64 {
							output := int64(0)
							for range events2.Data() {
								output += 1
							}
							return output
						}),
						processors.Count(func(events2 events.Events) int64 {
							output := int64(0)
							for range events2.Data() {
								output += 1
							}
							return output
						}),
						processors.Count(func(events2 events.Events) int64 {
							output := int64(0)
							for range events2.Data() {
								output += 1
							}
							return output
						}),
						processors.Count(func(events2 events.Events) int64 {
							output := int64(0)
							for range events2.Data() {
								output += 1
							}
							return output
						}),
						processors.Count(func(events2 events.Events) int64 {
							output := int64(0)
							for range events2.Data() {
								output += 1
							}
							return output
						}),
					},
				},
				output:       st.Output,
				outputNumber: int64(n),
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
			result := int64(0)

			for _, e := range st.OutputEvents() {
				result += e.Data.(int64)
			}
			if result != tt.fields.outputNumber {
				t.Errorf(`wrong count want %d got %d`, tt.fields.outputNumber, result)
			}
			st.Reset()
		})
	}
}
