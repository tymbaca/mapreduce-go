package mapreduce

import (
	"context"
	"log"
)

type transport[T any] interface {
	// Recv receives the data sent to specified id. Blocks until someone
	// calls Send with corresponding id, or if someone calls Close
	Recv(ctx context.Context, id int) (T, bool)

	// Send sends the data to specified id. Blocks until someone calls Recv
	// with corresponding id, or if someone calls Close
	Send(ctx context.Context, id int, data T)

	// Close cuts all currently blocked Recv and Send calls and prevends
	// future their calls. It can be called zero, one or multiple times.
	Close()
}

type chanTransport[T any] struct {
	ctx   context.Context
	close func()

	peers map[int]chan T
}

func newTransport[T any](peerCount int) transport[T] {
	peers := make(map[int]chan T)

	for i := range peerCount {
		ch := make(chan T)
		peers[i] = ch
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &chanTransport[T]{
		ctx:   ctx,
		close: cancel,
		peers: peers,
	}
}

func (t *chanTransport[T]) Recv(ctx context.Context, id int) (data T, open bool) {
	if t.ctx.Err() != nil {
		return
	}

	ch, ok := t.peers[id]
	if !ok {
		log.Panicf("listen: no peer for id %d", id)
	}

	select {
	case <-ctx.Done():
		return data, false
	case <-t.ctx.Done():
		return data, false
	case data = <-ch:
		return data, true
	}
}

func (t *chanTransport[T]) Send(ctx context.Context, id int, data T) {
	if t.ctx.Err() != nil {
		return
	}

	ch, ok := t.peers[id]
	if !ok {
		log.Panicf("send: no peer for id %d", id)
	}

	select {
	case <-ctx.Done():
	case <-t.ctx.Done():
	case ch <- data:
	}
}

func (t *chanTransport[T]) Close() {
	t.close()
}
