package mapreduce

import (
	"context"
	"math/rand"

	"github.com/spaolacci/murmur3"
	"golang.org/x/sync/errgroup"
)

type MapReduce struct {
	mapFn       MapFunc
	mapperCount int

	reduceFn     ReduceFunc
	reducerCount int
	storage      Storage

	partiotionFn PartitionFunc
}

func New(mapFn MapFunc, reduceFn ReduceFunc, storage Storage, mapperCount, reducerCount int) *MapReduce {
	return &MapReduce{
		mapFn:        mapFn,
		mapperCount:  mapperCount,
		reduceFn:     reduceFn,
		reducerCount: reducerCount,
		storage:      storage,
		partiotionFn: func(key string) int {
			sum := murmur3.Sum64([]byte(key))
			rem := sum % uint64(reducerCount)
			return int(rem)
		},
	}
}

// Run blocks untill all input is mapped, then unblocks and starts to
func (mr *MapReduce) Run(ctx context.Context, in <-chan KeyVal) (<-chan KeyVals, error) {
	out := make(chan KeyVals)

	inTrans := newTransport[toMapperMsg](1, mr.mapperCount)
	middleTrans := newTransport[toReducerMsg](mr.mapperCount, mr.reducerCount)
	outTrans := newTransport[resultMsg](mr.reducerCount, 1)

	for id := range mr.mapperCount {
		forkMapper(ctx, mr.mapFn, mr.partiotionFn, id, inTrans, middleTrans)
	}

	for id := range mr.reducerCount {
		forkReducer(ctx, mr.storage, mr.reduceFn, id, middleTrans, 0, outTrans)
	}

	// map phase
	var wg errgroup.Group
loop:
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case kv, open := <-in:
			if !open {
				break loop
			}

			wg.Go(func() error {
				id := rand.Intn(mr.mapperCount)
				inTrans.Send(ctx, id, toMapperMsg{kv: kv})
				return nil
			})
		}
	}
	wg.Wait() // NOTE: do we need this?
	inTrans.Close()

	// reduce phase
	go func() {
		defer close(out)

		for {
			kvs, open := outTrans.Recv(ctx, 0)
			if !open {
				return
			}

			select {
			case <-ctx.Done():
				return
			case out <- kvs.result:
			}
		}
	}()

	return out, nil
}
