package mapreduce

import (
	"context"
	"log"
	"sync"

	"github.com/tymbaca/mapreduce-go/pkg/caller"
	"github.com/tymbaca/mapreduce-go/pkg/tracer"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type transport[T any] interface {
	// Recv receives the data sent to specified id. Blocks until someone
	// calls Send with corresponding id, or if all senders called Close.
	Recv(ctx context.Context, id int) (T, bool)

	// Send sends the data to specified id. Blocks until someone calls Recv
	// with corresponding id.
	Send(ctx context.Context, id int, data T)

	// Close is called by sender, whenever it sent all it's data. It must be
	// called before exiting. Sender must not use transport after calling Close.
	Close()
}

type chanTransport[T any] struct {
	sendersWg *sync.WaitGroup
	peers     map[int]chan T
}

func newTransport[T any](senders, receivers int) transport[T] {
	peers := make(map[int]chan T)

	for i := range receivers {
		ch := make(chan T, 1)
		peers[i] = ch
	}

	sendersWg := &sync.WaitGroup{}
	sendersWg.Add(senders)

	go func() {
		sendersWg.Wait()
		for _, ch := range peers {
			close(ch)
		}
	}()

	return &chanTransport[T]{
		sendersWg: sendersWg,
		peers:     peers,
	}
}

func (t *chanTransport[T]) Recv(ctx context.Context, id int) (data T, open bool) {
	ctx, span := tracer.Start(ctx, caller.Name(), trace.WithAttributes(attribute.Int("id", id)))
	defer span.End()

	ch, ok := t.peers[id]
	if !ok {
		log.Panicf("listen: no peer for id %d", id)
	}

	select {
	case <-ctx.Done():
		return data, false
	case data, open = <-ch:
		return data, open
	}
}

func (t *chanTransport[T]) Send(ctx context.Context, id int, data T) {
	ctx, span := tracer.Start(ctx, caller.Name(), trace.WithAttributes(attribute.Int("id", id)))
	defer span.End()

	ch, ok := t.peers[id]
	if !ok {
		log.Panicf("send: no peer for id %d", id)
	}

	select {
	case <-ctx.Done():
	case ch <- data:
	}
}

func (t *chanTransport[T]) Close() {
	t.sendersWg.Done()
}
