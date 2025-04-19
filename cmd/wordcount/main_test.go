package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/tymbaca/mapreduce-go/mapreduce"
	"github.com/tymbaca/mapreduce-go/mapreduce/storage/bbolt"
	"github.com/tymbaca/mapreduce-go/pkg/tracer"
)

func TestWordCount(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	slog.SetDefault(logger)

	tracer.Init("localhost:4318")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	dbPath := "tymbaca.db"
	os.Remove(dbPath)
	storage, err := bbolt.New(dbPath)
	if err != nil {
		panic(err)
	}

	mr := mapreduce.New(countMap, countReduce, storage, 20, 9)

	inCh := make(chan mapreduce.KeyVal)
	go func() {
		for i := range 10 {
			text := gofakeit.Sentence(gofakeit.IntRange(100, 200))
			inCh <- mapreduce.KeyVal{Val: text}
			slog.Warn("client: sent", "n", i)
		}
		close(inCh)
	}()

	start := time.Now()

	outCh, err := mr.Run(ctx, inCh)
	if err != nil {
		panic(err)
	}

	os.Remove("tymbaca.out.log")
	toLog("tymbaca.out.log", outCh)

	fmt.Printf("time elapsed: %s\n", time.Since(start))
	fmt.Printf("stats: %s\n", mapreduce.GlobalStats)
}

func toLog(path string, outCh <-chan mapreduce.KeyVals) {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}

	for kvs := range outCh {
		_, err = fmt.Fprintf(f, "%v\n", kvs)
		if err != nil {
			panic(err)
		}
	}
}

func countMap(ctx context.Context, _, value string) []mapreduce.KeyVal {
	// time.Sleep(40 * time.Millisecond)

	wordCount := make(map[string]int)

	for _, word := range strings.Split(value, " ") {
		if len(word) == 0 {
			continue
		}

		wordCount[strings.ToLower(word)] += 1
	}

	kvs := make([]mapreduce.KeyVal, 0, len(wordCount))
	for word, count := range wordCount {
		kvs = append(kvs, mapreduce.KeyVal{
			Key: word,
			Val: strconv.Itoa(count),
		})
	}

	return kvs
}

func countReduce(ctx context.Context, _ string, counts []string) []string {
	// time.Sleep(10 * time.Millisecond)

	total := 0
	for _, countStr := range counts {
		count, err := strconv.Atoi(countStr)
		if err != nil {
			panic(err)
		}

		total += count
	}

	return []string{strconv.Itoa(total)}
}
