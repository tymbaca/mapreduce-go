package mapreduce

import "context"

type storage interface {
	Get(ctx context.Context, key string) []string
	GetKeys(ctx context.Context) []string
	Append(ctx context.Context, key string, vals []string)
}
