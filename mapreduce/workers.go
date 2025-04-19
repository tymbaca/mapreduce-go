package mapreduce

import (
	"context"
	"log/slog"
	"strconv"

	"github.com/tymbaca/mapreduce-go/pkg/caller"
	"github.com/tymbaca/mapreduce-go/pkg/tracer"
	"golang.org/x/sync/errgroup"
)

func forkMapper(ctx context.Context, mapFn MapFunc, partiotionFn PartitionFunc, id int, in transport[toMapperMsg], out transport[toReducerMsg]) {
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

	in  transport[toMapperMsg]
	out transport[toReducerMsg]
}

func (m *mapper) run(ctx context.Context) {
	ctx, span := tracer.Start(ctx, caller.Name())
	defer span.End()

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

		ctx, span = tracer.Start(ctx, "map")
		out := m.mapFn(ctx, in.kv.Key, in.kv.Val)
		span.End()

		var wg errgroup.Group
		for _, kv := range out {
			wg.Go(func() error {
				slog.Info("mapper: sending output...", "id", m.id, "kv", kv)

				m.out.Send(ctx, m.partiotionFn(kv.Key), toReducerMsg{kv: kv})

				GlobalStats.MapOut.Add(1)
				slog.Info("mapper: output sent", "id", m.id, "kv", kv)
				return nil
			})
		}

		wg.Wait()
	}
}

func forkReducer(ctx context.Context, storage Storage, reduceFn ReduceFunc, id int, in transport[toReducerMsg], outID int, out transport[resultMsg]) {
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

	in      transport[toReducerMsg]
	storage Storage
	out     transport[resultMsg]
	outID   int
}

func (r *reducer) run(ctx context.Context) {
	ctx, span := tracer.Start(ctx, caller.Name())
	defer span.End()
	defer r.out.Close()

	bucket := strconv.Itoa(r.id)

	for {
		in, open := r.in.Recv(ctx, r.id)
		if !open {
			// mapping phase is over
			// move on to reduce phase
			slog.Info("reducer: transport closed, starting reduce phase", "id", r.id)
			break
		}
		GlobalStats.ReduceIn.Add(1)
		slog.Info("reducer: got input", "id", r.id)

		r.storage.Append(ctx, bucket, in.kv.Key, []string{in.kv.Val})
	}

	keys := r.storage.GetKeys(ctx, bucket)

	var wg errgroup.Group
	for _, key := range keys {
		vals := r.storage.Get(ctx, bucket, key)

		ctx, span := tracer.Start(ctx, "reduce")
		reducedVals := r.reduceFn(ctx, key, vals)
		span.End()

		wg.Go(func() error {
			output := KeyVals{Key: key, Vals: reducedVals}
			slog.Info("reducer: sending output...", "id", r.id, "output", output)

			r.out.Send(ctx, r.outID, resultMsg{result: output})

			GlobalStats.ReduceOut.Add(1)
			slog.Info("reducer: output sent", "id", r.id, "output", output)
			return nil
		})
	}

	wg.Wait()
}
