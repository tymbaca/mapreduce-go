package mapreduce

import "context"

type KeyVal struct {
	Key string
	Val string
}

type MapFunc func(key string, val string) (kvs []KeyVal)

type ReduceFunc func(key string, vals []string) (result []string)

type MapReduce struct{}

func New(mapFn MapFunc, reduceFn ReduceFunc) {
}

func (mr *MapReduce) Run(ctx context.Context, input <-chan KeyVal) (output <-chan KeyVal) {
	output = make(<-chan KeyVal)

}
