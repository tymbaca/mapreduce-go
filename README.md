# MapReduce

Это учебная реализация MapReduce, согласно официальной статье
[MapReduce: Simplified Data Processing on Large
Clusters](https://research.google.com/archive/mapreduce-osdi04.pdf).

Реализация является локальной (что во многом невелируем весь смысл MarReduce'а). По сути представляет из себя
библиотеку. Пример(-ы) использования приведен в папке `cmd`.

## Использование

Для использования пользователю нужно сделать два шага:

**Создать** инстант `MapReduce` через `mapreduce.New`. Сигнатурa: 

```go
func New(mapFn MapFunc, reduceFn ReduceFunc, storage Storage, mapperCount, reducerCount int) *MapReduce
```

Хранилище `Storage` используется редюсерами, для накопления промежуточных
результатов перед запуском reduce фазы.

**Запустить** через метод `(*MapReduce).Run`, сигнатура: 

```go
func Run(ctx context.Context, in <-chan KeyVal) (<-chan KeyVals, error)
```

Принимает и отдает канал, для потоковой обработки. В случае если входных
данных много, пользователь может "скармливать" их постепенно, чтобы не выбиваться
по памяти. То же актуально и для выходного канала.

## Storage

Принимаемое в `New` хранилище является интерфейсом:

```go
// Storage used to persistently store data. Is used by redusers to
// store incoming intermediate values before reducing them.
//
// Bucket corresponds to reducer's ID (so the their values won't mix)
type Storage interface {
	Get(ctx context.Context, bucket string, key string) []string
	GetKeys(ctx context.Context, bucket string) []string
	Append(ctx context.Context, bucket string, key string, vals []string)
}
```

В репозитории, в `/mapreduce/storage` есть две реализации хранилища: один
персистентный, другой in-memory. Пользователь может реализовать свое хранилище
при необходимости.

## Transport 

Воркер общаются между собой через специальный интерфейс `transport`:

```go
type transport[T any] interface {
	// Recv receives the data sent to specified id. Blocks until someone
	// calls Send with corresponding id, or if all senders called Close.
	Recv(ctx context.Context, id int) (T, bool)

	// Send sends the data to specified id. Blocks until someone calls Recv
	// with corresponding id.
	Send(ctx context.Context, id int, data T)

	// Close is called by sender, whenever it sent all it's data. It must be
	// called before exiting. Sender must not use transport after calling Close.
	Close()
}
```

Сейчас в проекте используется локальная имплементация с использованием go'шных
каналов, но наличие установленного интерфейса позволяет в будущем менее
болезненно сделать destributed версию, например реализовав этот интерфейс поверх
TCP.
