package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/tymbaca/mapreduce-go/mapreduce"
	"github.com/tymbaca/mapreduce-go/mapreduce/storage/bbolt"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	storage, err := bbolt.New("tymbaca.db")
	if err != nil {
		panic(err)
	}

	mr := mapreduce.New(countMap, countReduce, storage, 10, 5)

	inCh := make(chan mapreduce.KeyVal)
	go func() {
		for range 10 {
			text := gofakeit.Sentence(gofakeit.IntRange(10, 20))
			inCh <- mapreduce.KeyVal{Val: text}
		}
		close(inCh)
	}()

	outCh, err := mr.Run(ctx, inCh)
	if err != nil {
		panic(err)
	}

	toLog("tymbaca.out.log", outCh)
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
