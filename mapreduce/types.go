package mapreduce

import "context"

type MapFunc func(ctx context.Context, key string, value string) []KeyVal

type ReduceFunc func(ctx context.Context, key string, values []string) []string

type PartitionFunc func(key string) int

type KeyVal struct {
	Key string
	Val string
}

type KeyVals struct {
	Key  string
	Vals []string
}

type toMapperMsg struct {
	ctx context.Context
	kv  KeyVal
}

type toReducerMsg struct {
	ctx context.Context
	kv  KeyVal
}

type resultMsg struct {
	ctx context.Context
	kvs KeyVals
}
