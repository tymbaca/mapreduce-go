package bbolt

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"go.etcd.io/bbolt"
)

type BboltStorage struct {
	db *bbolt.DB
}

func New(path string) (*BboltStorage, error) {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: 30 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("create bolt storage: %w", err)
	}

	return &BboltStorage{
		db: db,
	}, nil
}

func (s *BboltStorage) Get(ctx context.Context, bucket string, key string) []string {
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

func (s *BboltStorage) GetKeys(ctx context.Context, bucket string) []string {
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

func (s *BboltStorage) Append(ctx context.Context, bucket string, key string, newVals []string) {
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
func (s *BboltStorage) Close() error {
	return s.db.Close()
}

// Destroy closes the database and removes the file.
func (s *BboltStorage) Destroy() error {
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
