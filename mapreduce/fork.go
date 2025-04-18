package mapreduce

import (
	"context"
)

func forkMapper(ctx context.Context, mapFn MapFunc, partiotionFn PartitionFunc, id int, in, out transport[KeyVal]) {
}

type mapper struct {
	id           int
	mapFn        MapFunc
	partiotionFn PartitionFunc

	in  transport[KeyVal]
	out transport[KeyVal]
}

func (m *mapper) run(ctx context.Context) {
	for {
		in, open := m.in.Recv(ctx, m.id)
		if !open {
			return
		}

		out := m.mapFn(ctx, in.Key, in.Val)

		for _, kv := range out {
			m.out.Send(ctx, m.partiotionFn(kv.Key), kv)
		}
	}
}

func forkReducer(ctx context.Context, reduceFn ReduceFunc, id int, in, out transport[KeyVal]) {
}

type reducer struct {
	id       int
	reduceFn ReduceFunc

	in      transport[KeyVal]
	storage storage
	out     transport[KeyVals]
	outID   int
}

func (r *reducer) run(ctx context.Context) {
	for {
		in, open := r.in.Recv(ctx, r.id)
		if !open {
			// mapping phase is over
			// move on to reduce phase
			break
		}

		r.storage.Append(ctx, in.Key, []string{in.Val})
	}

	keys := r.storage.GetKeys(ctx)

	for _, key := range keys {
		vals := r.storage.Get(ctx, key)
		reducedVals := r.reduceFn(ctx, key, vals)
		r.out.Send(ctx, r.outID, KeyVals{Key: key, Vals: reducedVals})
	}
}
