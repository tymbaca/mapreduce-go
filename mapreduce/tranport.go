package mapreduce

import (
	"context"
	"log"
	"sync"
)

type transport[T any] interface {
	Recv(ctx context.Context, id int) (T, bool)
	Send(ctx context.Context, id int, data T)
	Close()
}

type chanTransport[T any] struct {
	peers  map[int]chan T
	mu     sync.RWMutex
	closed bool
}

func (t *chanTransport[T]) Recv(ctx context.Context, id int) (data T, open bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

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
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return
	}

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
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, ch := range t.peers {
		close(ch)
	}
	t.closed = true
}

func newTransport[T any](peerCount int) transport[T] {
	peers := make(map[int]chan T)

	for i := range peerCount {
		ch := make(chan T)
		peers[i] = ch
	}

	// return &chanTransport[T]{
	// 	peers: peers,
	// }
}
