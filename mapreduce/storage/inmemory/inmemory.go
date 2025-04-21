package inmemory

import (
	"context"
	"sync"

	"github.com/tymbaca/mapreduce-go/pkg/caller"
	"github.com/tymbaca/mapreduce-go/pkg/tracer"
)

type Storage struct {
	mu   sync.RWMutex
	data map[itemKey][]string
}

func (st *Storage) Get(ctx context.Context, bucket string, key string) []string {
	ctx, span := tracer.Start(ctx, caller.Name())
	defer span.End()

	st.mu.RLock()
	defer st.mu.RUnlock()

	return st.data[itemKey{bucket: bucket, key: key}]
}

func (st *Storage) GetKeys(ctx context.Context, bucket string) []string {
	ctx, span := tracer.Start(ctx, caller.Name())
	defer span.End()

	st.mu.RLock()
	defer st.mu.RUnlock()

	var keys []string
	for k := range st.data {
		if k.bucket == bucket {
			keys = append(keys, k.key)
		}
	}

	return keys
}

func (st *Storage) Append(ctx context.Context, bucket string, key string, vals []string) {
	ctx, span := tracer.Start(ctx, caller.Name())
	defer span.End()

	st.mu.Lock()
	defer st.mu.Unlock()

	itemKey := itemKey{bucket: bucket, key: key}
	st.data[itemKey] = append(st.data[itemKey], vals...)
}

type itemKey struct {
	bucket string
	key    string
}

func New() *Storage {
	return &Storage{
		data: make(map[itemKey][]string, 1000),
	}
}
