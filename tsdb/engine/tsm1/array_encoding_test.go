package tsm1_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

func TestDecodeFloatArrayBlock(t *testing.T) {
	valueCount := 1000
	times := getTimes(valueCount, 60, time.Second)
	values := make(tsm1.FloatValues, len(times))
	for i, t := range times {
		values[i] = tsm1.NewFloatValue(t, float64(i)).(tsm1.FloatValue)
	}
	exp := tsm1.NewFloatArrayFromValues(values)

	b, err := values.Encode(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := tsdb.NewFloatArrayLen(exp.Len())
	tsm1.DecodeFloatArrayBlock(b, got)
	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func BenchmarkDecodeBooleanArrayBlock(b *testing.B) {
	cases := []int{
		5,
		55,
		555,
		1000,
	}
	for _, n := range cases {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			valueCount := n
			times := getTimes(valueCount, 60, time.Second)
			values := make([]tsm1.Value, len(times))
			for i, t := range times {
				values[i] = tsm1.NewValue(t, true)
			}

			bytes, err := tsm1.Values(values).Encode(nil)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(tsm1.Values(values).Size()))

			b.RunParallel(func(pb *testing.PB) {
				decodedValues := tsdb.NewBooleanArrayLen(len(values))

				for pb.Next() {
					err = tsm1.DecodeBooleanArrayBlock(bytes, decodedValues)
					if err != nil {
						b.Fatalf("unexpected error decoding block: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkDecodeFloatArrayBlock(b *testing.B) {
	cases := []int{
		5,
		55,
		555,
		1000,
	}
	for _, n := range cases {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			valueCount := n
			times := getTimes(valueCount, 60, time.Second)
			values := make([]tsm1.Value, len(times))
			for i, t := range times {
				values[i] = tsm1.NewValue(t, float64(i))
			}

			bytes, err := tsm1.Values(values).Encode(nil)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(tsm1.Values(values).Size()))

			b.RunParallel(func(pb *testing.PB) {
				decodedValues := tsdb.NewFloatArrayLen(len(values))

				for pb.Next() {
					err = tsm1.DecodeFloatArrayBlock(bytes, decodedValues)
					if err != nil {
						b.Fatalf("unexpected error decoding block: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkDecodeIntegerArrayBlock(b *testing.B) {
	rle := func(i int) int64 { return int64(i) }
	s8b := func(i int) int64 { return int64(i + int(rand.Int31n(10))) }

	cases := []struct {
		enc string
		gen func(i int) int64
		n   int
	}{
		{enc: "rle", gen: rle, n: 5},
		{enc: "rle", gen: rle, n: 55},
		{enc: "rle", gen: rle, n: 555},
		{enc: "rle", gen: rle, n: 1000},
		{enc: "s8b", gen: s8b, n: 5},
		{enc: "s8b", gen: s8b, n: 55},
		{enc: "s8b", gen: s8b, n: 555},
		{enc: "s8b", gen: s8b, n: 1000},
	}
	for _, bm := range cases {
		b.Run(fmt.Sprintf("%s_%d", bm.enc, bm.n), func(b *testing.B) {
			rand.New(rand.NewSource(int64(bm.n * 1e3)))

			valueCount := bm.n
			times := getTimes(valueCount, 60, time.Second)
			values := make([]tsm1.Value, len(times))
			for i, t := range times {
				values[i] = tsm1.NewValue(t, bm.gen(i))
			}

			bytes, err := tsm1.Values(values).Encode(nil)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(tsm1.Values(values).Size()))

			b.RunParallel(func(pb *testing.PB) {
				decodedValues := tsdb.NewIntegerArrayLen(len(values))

				for pb.Next() {
					err = tsm1.DecodeIntegerArrayBlock(bytes, decodedValues)
					if err != nil {
						b.Fatalf("unexpected error decoding block: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkDecodeStringArrayBlock(b *testing.B) {
	cases := []int{
		5,
		55,
		555,
		1000,
	}
	for _, n := range cases {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			valueCount := n
			times := getTimes(valueCount, 60, time.Second)
			values := make([]tsm1.Value, len(times))
			for i, t := range times {
				values[i] = tsm1.NewValue(t, fmt.Sprintf("value %d", i))
			}

			bytes, err := tsm1.Values(values).Encode(nil)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(tsm1.Values(values).Size()))

			b.RunParallel(func(pb *testing.PB) {
				decodedValues := tsdb.NewStringArrayLen(len(values))

				for pb.Next() {
					err = tsm1.DecodeStringArrayBlock(bytes, decodedValues)
					if err != nil {
						b.Fatalf("unexpected error decoding block: %v", err)
					}
				}
			})
		})
	}
}
