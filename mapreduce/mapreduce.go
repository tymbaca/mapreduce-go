package mapreduce

import (
	"context"

	"github.com/spaolacci/murmur3"
)

type MapReduce struct {
	mapFn       MapFunc
	mapperCount int

	reduceFn     ReduceFunc
	reducerCount int

	partiotionFn PartitionFunc
}

func New(mapperCount, reducerCount int, mapFn MapFunc, reduceFn ReduceFunc) *MapReduce {
	return &MapReduce{
		mapFn:        mapFn,
		mapperCount:  mapperCount,
		reduceFn:     reduceFn,
		reducerCount: reducerCount,
		partiotionFn: func(key string) int {
			return int(murmur3.Sum64([]byte(key))) % reducerCount
		},
	}
}

func (mr *MapReduce) Run(ctx context.Context, input <-chan KeyVal) (output <-chan KeyVals) {
	output = make(<-chan KeyVals)

	inTrans := newTransport[KeyVal](mr.mapperCount)
	middleTrans := newTransport[KeyVal](mr.reducerCount)
	outTrans := newTransport[KeyVal](1)

	for id := range mr.mapperCount {
		forkMapper(ctx, mr.mapFn, mr.partiotionFn, id, inTrans)
	}
}
