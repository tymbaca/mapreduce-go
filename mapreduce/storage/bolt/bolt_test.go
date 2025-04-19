package bolt

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var nilSlice []string

func TestBolt(t *testing.T) {
	_ = os.Remove("testdata/test.db")
	ctx := context.Background()

	storage, err := New("testdata/test.db")
	require.NoError(t, err)

	storage.Append(ctx, "2", "key1", []string{"val1", "val2"})
	require.Equal(t, []string{"val1", "val2"}, storage.Get(ctx, "2", "key1"))

	storage.Append(ctx, "2", "key1", []string{"val3", "val4", "val5"})
	require.Equal(t, []string{"val1", "val2", "val3", "val4", "val5"}, storage.Get(ctx, "2", "key1"))

	require.Equal(t, nilSlice, storage.Get(ctx, "2", "key2"))
	storage.Append(ctx, "2", "key2", []string{"val1"})
	require.Equal(t, []string{"val1"}, storage.Get(ctx, "2", "key2"))

	require.Equal(t, nilSlice, storage.Get(ctx, "3", "key1"))
	storage.Append(ctx, "3", "key1", []string{"val3", "val4", "val5"})
	require.Equal(t, []string{"val3", "val4", "val5"}, storage.Get(ctx, "3", "key1"))

	storage.Destroy()

	storage, err = New("testdata/test.db")
	require.NoError(t, err)

	storage.Append(ctx, "2", "key1", []string{"val1", "val2"})
	require.Equal(t, []string{"val1", "val2"}, storage.Get(ctx, "2", "key1"))
}
