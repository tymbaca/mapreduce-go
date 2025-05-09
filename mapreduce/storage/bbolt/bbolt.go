package bbolt

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/tymbaca/mapreduce-go/pkg/caller"
	"github.com/tymbaca/mapreduce-go/pkg/tracer"
	"go.etcd.io/bbolt"
)

type Storage struct {
	db *bbolt.DB
}

func New(path string) (*Storage, error) {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: 30 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("create bolt storage: %w", err)
	}

	return &Storage{
		db: db,
	}, nil
}

func (s *Storage) Get(ctx context.Context, bucket string, key string) []string {
	ctx, span := tracer.Start(ctx, caller.Name())
	defer span.End()

	var vals []string

	err := s.db.Update(func(tx *bbolt.Tx) error {
		buck, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}

		vals = get(buck, key)

		return nil
	})
	if err != nil {
		panic(err)
	}

	return vals
}

func (s *Storage) GetKeys(ctx context.Context, bucket string) []string {
	ctx, span := tracer.Start(ctx, caller.Name())
	defer span.End()

	var keys []string

	err := s.db.Update(func(tx *bbolt.Tx) error {
		buck, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}

		c := buck.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			keys = append(keys, string(k))
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	return keys
}

func (s *Storage) Append(ctx context.Context, bucket string, key string, newVals []string) {
	ctx, span := tracer.Start(ctx, caller.Name())
	defer span.End()

	err := s.db.Update(func(tx *bbolt.Tx) error {
		buck, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			log.Panicf("key: '%s', newVals: %v, err: %s", key, newVals, err)
		}

		vals := get(buck, key)
		vals = append(vals, newVals...)

		data, err := json.Marshal(vals)
		if err != nil {
			log.Panicf("key: '%s', newVals: %v, err: %s", key, newVals, err)
		}

		err = buck.Put([]byte(key), data)
		if err != nil {
			log.Panicf("key: '%s', newVals: %v, err: %s", key, newVals, err)
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	return
}

// Close must be call to release database connection.
func (s *Storage) Close() error {
	return s.db.Close()
}

// Destroy closes the database and removes the file.
func (s *Storage) Destroy() error {
	path := s.db.Path()
	_ = s.Close()
	return os.Remove(path)
}

func get(buck *bbolt.Bucket, key string) []string {
	data := buck.Get([]byte(key))

	if len(data) == 0 {
		return nil
	}

	var vals []string
	err := json.Unmarshal(data, &vals)
	if err != nil {
		panic(err)
	}

	return vals
}
