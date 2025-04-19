package mapreduce

import (
	"context"
	"log/slog"
	"strconv"
)

func forkMapper(ctx context.Context, mapFn MapFunc, partiotionFn PartitionFunc, id int, in, out transport[KeyVal]) {
	m := &mapper{
		id:           id,
		mapFn:        mapFn,
		partiotionFn: partiotionFn,
		in:           in,
		out:          out,
	}

	go m.run(ctx)
}

type mapper struct {
	id           int
	mapFn        MapFunc
	partiotionFn PartitionFunc

	in  transport[KeyVal]
	out transport[KeyVal]
}

func (m *mapper) run(ctx context.Context) {
	defer m.out.Close()
	for {
		slog.Info("mapper: receiving...", "id", m.id)
		in, open := m.in.Recv(ctx, m.id)
		if !open {
			slog.Info("mapper: transport closed, starting reduce phase", "id", m.id)
			return
		}
		GlobalStats.MapIn.Add(1)
		slog.Info("mapper: got input", "id", m.id)

		out := m.mapFn(ctx, in.Key, in.Val)

		for _, kv := range out {
			slog.Info("mapper: sending output...", "id", m.id, "kv", kv)
			m.out.Send(ctx, m.partiotionFn(kv.Key), kv)
			GlobalStats.MapOut.Add(1)
			slog.Info("mapper: output sent", "id", m.id, "kv", kv)
		}
	}
}

func forkReducer(ctx context.Context, storage Storage, reduceFn ReduceFunc, id int, in transport[KeyVal], outID int, out transport[KeyVals]) {
	r := &reducer{
		id:       id,
		reduceFn: reduceFn,
		in:       in,
		storage:  storage,
		out:      out,
		outID:    outID,
	}

	go r.run(ctx)
}

type reducer struct {
	id       int
	reduceFn ReduceFunc

	in      transport[KeyVal]
	storage Storage
	out     transport[KeyVals]
	outID   int
}

func (r *reducer) run(ctx context.Context) {
	defer r.out.Close()

	bucket := strconv.Itoa(r.id)

	for {
		slog.Info("reducer: receiving...", "id", r.id)
		in, open := r.in.Recv(ctx, r.id)
		if !open {
			// mapping phase is over
			// move on to reduce phase
			slog.Info("reducer: transport closed, starting reduce phase", "id", r.id)
			break
		}
		GlobalStats.ReduceIn.Add(1)
		slog.Info("reducer: got input", "id", r.id)

		r.storage.Append(ctx, bucket, in.Key, []string{in.Val})
	}

	keys := r.storage.GetKeys(ctx, bucket)

	for _, key := range keys {
		vals := r.storage.Get(ctx, bucket, key)
		reducedVals := r.reduceFn(ctx, key, vals)
		output := KeyVals{Key: key, Vals: reducedVals}

		slog.Info("reducer: sending output...", "id", r.id, "output", output)
		r.out.Send(ctx, r.outID, output)
		GlobalStats.ReduceOut.Add(1)
		slog.Info("reducer: output sent", "id", r.id, "output", output)
	}
}
