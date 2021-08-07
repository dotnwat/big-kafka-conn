package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	brokers      = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic        = flag.String("topic", "", "topic to produce to or consume from")
	clients      = flag.Int("num-clients", 1, "how many instances of client workload to run")
	recordSize   = flag.Int("record-size", 100, "bytes per record")
	compression  = flag.String("compression", "none", "compression algorithm to use (none,gzip,snappy,lz4,zstd, for producing)")
	linger       = flag.Duration("linger", 0, "if non-zero, linger to use when producing")
	maxBatchSize = flag.Int("max-batch-size", 1000000, "the maximum batch size to allow per-partition")
	logLevel     = flag.String("log-level", "", "if non-empty, use a basic logger with this log level (debug, info, warn, error)")

	rateRecs  int64
	rateBytes int64
)

func die(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func chk(err error, msg string, args ...interface{}) {
	if err != nil {
		die(msg, args...)
	}
}

func formatValue(num int64, v []byte) {
	var buf [20]byte // max int64 takes 19 bytes, then we add a space
	b := strconv.AppendInt(buf[:0], num, 10)
	b = append(b, ' ')

	n := copy(v, b)
	for n != len(v) {
		n += copy(v[n:], b)
	}
}

func printRate() {
	for range time.Tick(time.Second) {
		recs := atomic.SwapInt64(&rateRecs, 0)
		bytes := atomic.SwapInt64(&rateBytes, 0)
		fmt.Printf("%0.2f MiB/s; %0.2fk records/s\n", float64(bytes)/(1024*1024), float64(recs)/1000)
	}
}

func main() {
	flag.Parse()

	if *recordSize <= 0 {
		die("record bytes must be larger than zero")
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
		kgo.MaxBufferedRecords(50<<20 / *recordSize + 1),
		kgo.BatchMaxBytes(int32(*maxBatchSize)),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}

	switch strings.ToLower(*logLevel) {
	case "":
	case "debug":
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
	case "info":
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)))
	case "warn":
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelWarn, nil)))
	case "error":
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelError, nil)))
	default:
		die("unrecognized log level %s", *logLevel)
	}

	if *linger != 0 {
		opts = append(opts, kgo.Linger(*linger))
	}

	switch strings.ToLower(*compression) {
	case "none":
		opts = append(opts, kgo.BatchCompression(kgo.NoCompression()))
	case "gzip":
		opts = append(opts, kgo.BatchCompression(kgo.GzipCompression()))
	case "snappy":
		opts = append(opts, kgo.BatchCompression(kgo.SnappyCompression()))
	case "lz4":
		opts = append(opts, kgo.BatchCompression(kgo.Lz4Compression()))
	case "zstd":
		opts = append(opts, kgo.BatchCompression(kgo.ZstdCompression()))
	default:
		die("unrecognized compression %s", *compression)
	}

	if *clients <= 0 {
		die("number of clients must be positive")
	}

	var wg sync.WaitGroup

	go printRate()

	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			client, err := kgo.NewClient(opts...)
			chk(err, "unable to initialize client: %v", err)

			var num int64
			for {
				r := kgo.SliceRecord(make([]byte, *recordSize))
				formatValue(num, r.Value)
				client.Produce(context.Background(), r, func(r *kgo.Record, err error) {
					chk(err, "produce error: %v", err)
					atomic.AddInt64(&rateRecs, 1)
					atomic.AddInt64(&rateBytes, int64(*recordSize))
				})
				num++
			}
		}()
	}

	wg.Wait()
}
