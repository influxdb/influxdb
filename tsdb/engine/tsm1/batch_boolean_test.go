package tsm1_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

func TestBooleanArrayEncodeAll_NoValues(t *testing.T) {
	b, err := tsm1.BooleanArrayEncodeAll(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var dec tsm1.BooleanDecoder
	dec.SetBytes(b)
	if dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}
}

func TestBooleanArrayEncodeAll_Single(t *testing.T) {
	src := []bool{true}

	b, err := tsm1.BooleanArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var dec tsm1.BooleanDecoder
	dec.SetBytes(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got false, exp true")
	}

	if src[0] != dec.Read() {
		t.Fatalf("unexpected value: got %v, exp %v", dec.Read(), src[0])
	}
}

func TestBooleanArrayEncodeAll_Compare(t *testing.T) {
	// generate random values
	input := make([]bool, 1000)
	for i := 0; i < len(input); i++ {
		input[i] = rand.Int63n(2) == 1
	}

	s := tsm1.NewBooleanEncoder(1000)
	for _, v := range input {
		s.Write(v)
	}
	s.Flush()

	buf1, err := s.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buf2 := append([]byte("this is some jibberish"), make([]byte, 100, 200)...)
	buf2, err = tsm1.BooleanArrayEncodeAll(input, buf2)
	if err != nil {
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	result, err := tsm1.BooleanArrayDecodeAll(buf2, nil)
	if err != nil {
		dumpBufs(buf1, buf2)
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	if got, exp := result, input; !reflect.DeepEqual(got, exp) {
		dumpBufs(buf1, buf2)
		t.Fatalf("got result %v, expected %v", got, exp)
	}

	// Check that the encoders are byte for byte the same...
	if !bytes.Equal(buf1, buf2) {
		dumpBufs(buf1, buf2)
		t.Fatalf("Raw bytes differ for encoders")
	}
}

func TestBooleanArrayEncodeAll_Multi_Compressed(t *testing.T) {
	src := make([]bool, 10)
	for i := range src {
		src[i] = i%2 == 0
	}

	b, err := tsm1.BooleanArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if exp := 4; len(b) != exp {
		t.Fatalf("unexpected length: got %v, exp %v", len(b), exp)
	}

	var dec tsm1.BooleanDecoder
	dec.SetBytes(b)

	for i, v := range src {
		if !dec.Next() {
			t.Fatalf("unexpected next value: got false, exp true")
		}
		if v != dec.Read() {
			t.Fatalf("unexpected value at pos %d: got %v, exp %v", i, dec.Read(), v)
		}
	}

	if dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}
}

func TestBooleanArrayEncodeAll_Quick(t *testing.T) {
	if err := quick.Check(func(values []bool) bool {
		src := values
		if values == nil {
			src = []bool{}
		}

		// Retrieve compressed bytes.
		buf, err := tsm1.BooleanArrayEncodeAll(src, nil)
		if err != nil {
			t.Fatal(err)
		}

		// Read values out of decoder.
		got := make([]bool, 0, len(values))
		var dec tsm1.BooleanDecoder
		dec.SetBytes(buf)
		for dec.Next() {
			got = append(got, dec.Read())
		}

		// Verify that input and output values match.
		if !reflect.DeepEqual(src, got) {
			t.Fatalf("mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", src, got)
		}

		return true
	}, nil); err != nil {
		t.Fatal(err)
	}
}

func Test_BooleanArrayDecodeAll_Single(t *testing.T) {
	enc := tsm1.NewBooleanEncoder(1)
	exp := true
	enc.Write(exp)
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, _ := tsm1.BooleanArrayDecodeAll(b, nil)
	if len(got) != 1 {
		t.Fatalf("expected 1 value")
	}
	if got := got[0]; got != exp {
		t.Fatalf("unexpected value -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func Test_BooleanArrayDecodeAll_Multi_Compressed(t *testing.T) {
	cases := []struct {
		n int
		p float64 // probability of a true value
	}{
		{10, 0.33},
		{100, 0.55},
		{1000, 0.68},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("%d_%0.2f", tc.n, tc.p), func(t *testing.T) {
			rand.New(rand.NewSource(int64(tc.n * tc.n)))

			enc := tsm1.NewBooleanEncoder(tc.n)
			values := make([]bool, tc.n)
			for i := range values {
				values[i] = rand.Float64() < tc.p
				enc.Write(values[i])
			}

			b, err := enc.Bytes()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			got, err := tsm1.BooleanArrayDecodeAll(b, nil)
			if err != nil {
				t.Fatalf("unexpected error %q", err.Error())
			}

			if !cmp.Equal(got, values) {
				t.Fatalf("unexpected values, -got/+exp\n%s", cmp.Diff(got, values))
			}
		})
	}
}

func Test_BooleanBatchDecoder_Corrupt(t *testing.T) {
	cases := []struct {
		name string
		d    string
	}{
		{"empty", ""},
		{"invalid count", "\x10\x90"},
		{"count greater than remaining bits, multiple bytes expected", "\x10\x7f"},
		{"count greater than remaining bits, one byte expected", "\x10\x01"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dst, _ := tsm1.BooleanArrayDecodeAll([]byte(c.d), nil)
			if len(dst) != 0 {
				t.Fatalf("unexpected result -got/+want\n%s", cmp.Diff(dst, nil))
			}
		})
	}
}

func BenchmarkEncodeBooleans(b *testing.B) {
	var err error
	cases := []int{10, 100, 1000}

	for _, n := range cases {
		enc := tsm1.NewBooleanEncoder(n)
		b.Run(fmt.Sprintf("%d_ran", n), func(b *testing.B) {
			input := make([]bool, n)
			for i := 0; i < n; i++ {
				input[i] = rand.Int63n(2) == 1
			}

			b.Run("itr", func(b *testing.B) {
				b.ReportAllocs()
				enc.Reset()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					enc.Reset()
					for _, x := range input {
						enc.Write(x)
					}
					enc.Flush()
					if bufResult, err = enc.Bytes(); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("batch", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					if bufResult, err = tsm1.BooleanArrayEncodeAll(input, bufResult); err != nil {
						b.Fatal(err)
					}
				}
			})

		})
	}
}

func BenchmarkBooleanArrayDecodeAll(b *testing.B) {
	benchmarks := []struct {
		n int
	}{
		{1},
		{55},
		{555},
		{1000},
	}
	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("%d", bm.n), func(b *testing.B) {
			size := bm.n
			e := tsm1.NewBooleanEncoder(size)
			for i := 0; i < size; i++ {
				e.Write(i&1 == 1)
			}
			bytes, err := e.Bytes()
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}

			b.SetBytes(int64(len(bytes)))
			b.ResetTimer()

			dst := make([]bool, size)
			for i := 0; i < b.N; i++ {
				res, _ := tsm1.BooleanArrayDecodeAll(bytes, dst)
				if len(res) != size {
					b.Fatalf("expected to read %d booleans, but read %d", size, len(res))
				}
			}
		})
	}
}
