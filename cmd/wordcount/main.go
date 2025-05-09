package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/tymbaca/mapreduce-go/mapreduce"
	"github.com/tymbaca/mapreduce-go/mapreduce/storage/inmemory"
	"github.com/tymbaca/mapreduce-go/pkg/caller"
	"github.com/tymbaca/mapreduce-go/pkg/tracer"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	slog.SetDefault(logger)

	tracer.Init("localhost:4318")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// dbPath := "tymbaca.db"
	// os.Remove(dbPath)
	// storage, err := bbolt.New(dbPath)
	// if err != nil {
	// 	panic(err)
	// }
	storage := inmemory.New()

	mr := mapreduce.New(countMap, countReduce, storage, 1, 1)

	inCh := make(chan mapreduce.KeyVal)
	go func() {
		for i := range 1000 {
			text := _lorem
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

	<-ctx.Done()
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
	ctx, span := tracer.Start(ctx, caller.Name())
	defer span.End()

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
	ctx, span := tracer.Start(ctx, caller.Name())
	defer span.End()

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

const _lorem = `Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do
eiusmod tempor incididunt ut labore et dolore magna aliqua.Ut enim ad minim veniam,
quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.
Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat
nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia
deserunt mollit anim id est laborum` /*.*/
