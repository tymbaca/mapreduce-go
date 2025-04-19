package mapreduce

import "context"

// Storage used to persistently store data. Is used by redusers to
// store incoming intermediate values before reducing them.
//
// Bucket corresponds to reducer's ID (so the their values won't mix)
type Storage interface {
	Get(ctx context.Context, bucket string, key string) []string
	GetKeys(ctx context.Context, bucket string) []string
	Append(ctx context.Context, bucket string, key string, vals []string)
}
