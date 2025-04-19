package mapreduce

import (
	"context"
	"math/rand"

	"github.com/spaolacci/murmur3"
)

type MapReduce struct {
	mapFn       MapFunc
	mapperCount int

	reduceFn     ReduceFunc
	reducerCount int
	storage      Storage

	partiotionFn PartitionFunc
}

func New(mapperCount, reducerCount int, mapFn MapFunc, reduceFn ReduceFunc, storage Storage) *MapReduce {
	return &MapReduce{
		mapFn:        mapFn,
		mapperCount:  mapperCount,
		reduceFn:     reduceFn,
		reducerCount: reducerCount,
		storage:      storage,
		partiotionFn: func(key string) int {
			return int(murmur3.Sum64([]byte(key))) % reducerCount
		},
	}
}

// Run blocks untill all input is mapped, then unblocks and starts to
func (mr *MapReduce) Run(ctx context.Context, in <-chan KeyVal) (<-chan KeyVals, error) {
	out := make(chan KeyVals)

	inTrans := newTransport[KeyVal](mr.mapperCount)
	middleTrans := newTransport[KeyVal](mr.reducerCount)
	outTrans := newTransport[KeyVals](1)

	for id := range mr.mapperCount {
		forkMapper(ctx, mr.mapFn, mr.partiotionFn, id, inTrans, middleTrans)
	}

	for id := range mr.mapperCount {
		forkReducer(ctx, mr.storage, mr.reduceFn, id, middleTrans, 0, outTrans)
	}

	// map phase
loop:
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case kv, open := <-in:
			if !open {
				break loop
			}

			id := rand.Intn(mr.mapperCount)
			inTrans.Send(ctx, id, kv)
		}
	}

	// reduce phase
	middleTrans.Close() // this close is a signal to all reducers for them to start reduce phase
	go func() {
		for {
			kvs, open := outTrans.Recv(ctx, 0)
			if !open {
				panic("who closed outTrans?!")
			}

			select {
			case <-ctx.Done():
				outTrans.Close()
				return
			case out <- kvs:
			}
		}
	}()

	return out, nil
}
