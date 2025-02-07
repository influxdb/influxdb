package tsm1

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"testing/quick"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/internal/testutil"
	"github.com/influxdata/influxdb/uuid"
)

func equalError(a, b error) bool {
	return a == nil && b == nil || a != nil && b != nil && a.Error() == b.Error()
}

func TestStringArrayEncodeAll_NoValues(t *testing.T) {
	b, err := StringArrayEncodeAll(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var dec StringDecoder
	if err := dec.SetBytes(b); err != nil {
		t.Fatalf("unexpected error creating string decoder: %v", err)
	}
	if dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}
}

func TestStringArrayEncodeAll_ExceedsMaxEncodedLen(t *testing.T) {
	str := strings.Repeat(" ", 1<<23) // 8MB string
	var s []string
	for i := 0; i < 512; i++ {
		s = append(s, str)
	}

	_, got := StringArrayEncodeAll(s, nil)
	if !cmp.Equal(got, ErrStringArrayEncodeTooLarge, cmp.Comparer(equalError)) {
		t.Fatalf("expected error, got: %v", got)
	}
}

func TestStringArrayEncodeAll_Single(t *testing.T) {
	src := []string{"v1"}
	b, err := StringArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var dec StringDecoder
	if dec.SetBytes(b); err != nil {
		t.Fatalf("unexpected error creating string decoder: %v", err)
	}
	if !dec.Next() {
		t.Fatalf("unexpected next value: got false, exp true")
	}

	if src[0] != dec.Read() {
		t.Fatalf("unexpected value: got %v, exp %v", dec.Read(), src[0])
	}
}

func TestStringArrayEncode_Compare(t *testing.T) {
	// generate random values
	input := make([]string, 1000)
	for i := 0; i < len(input); i++ {
		input[i] = uuid.TimeUUID().String()
	}

	// Example from the paper
	s := NewStringEncoder(1000)
	for _, v := range input {
		s.Write(v)
	}
	s.Flush()

	buf1, err := s.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buf2 := append([]byte("this is some jibberish"), make([]byte, 100, 200)...)
	buf2, err = StringArrayEncodeAll(input, buf2)
	if err != nil {
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	result, err := StringArrayDecodeAll(buf2, nil)
	if err != nil {
		dumpBufs(buf1, buf2)
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	if got, exp := result, input; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got result %v, expected %v", got, exp)
	}

	// Check that the encoders are byte for byte the same...
	if !bytes.Equal(buf1, buf2) {
		dumpBufs(buf1, buf2)
		t.Fatalf("Raw bytes differ for encoders")
	}
}

func TestStringArrayEncodeAll_Multi_Compressed(t *testing.T) {
	src := make([]string, 10)
	for i := range src {
		src[i] = fmt.Sprintf("value %d", i)
	}

	b, err := StringArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != stringCompressedSnappy {
		t.Fatalf("unexpected encoding: got %v, exp %v", b[0], stringCompressedSnappy)
	}

	if exp := 51; len(b) != exp {
		t.Fatalf("unexpected length: got %v, exp %v", len(b), exp)
	}

	var dec StringDecoder
	if err := dec.SetBytes(b); err != nil {
		t.Fatalf("unexpected erorr creating string decoder: %v", err)
	}

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

func TestStringArrayEncodeAll_Quick(t *testing.T) {
	var base []byte
	quick.Check(func(values []string) bool {
		src := values
		if values == nil {
			src = []string{}
		}

		// Retrieve encoded bytes from encoder.
		buf, err := StringArrayEncodeAll(src, base)
		if err != nil {
			t.Fatal(err)
		}

		// Read values out of decoder.
		got := make([]string, 0, len(src))
		var dec StringDecoder
		if err := dec.SetBytes(buf); err != nil {
			t.Fatal(err)
		}
		for dec.Next() {
			if err := dec.Error(); err != nil {
				t.Fatal(err)
			}
			got = append(got, dec.Read())
		}

		// Verify that input and output values match.
		if !reflect.DeepEqual(src, got) {
			t.Fatalf("mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", src, got)
		}

		return true
	}, nil)
}

func TestStringArrayDecodeAll_NoValues(t *testing.T) {
	enc := NewStringEncoder(1024)
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := StringArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected error creating string decoder: %v", err)
	}

	exp := []string{}
	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestStringArrayDecodeAll_Single(t *testing.T) {
	enc := NewStringEncoder(1024)
	v1 := "v1"
	enc.Write(v1)
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := StringArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected error creating string decoder: %v", err)
	}

	exp := []string{"v1"}
	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestStringArrayDecodeAll_Multi_Compressed(t *testing.T) {
	enc := NewStringEncoder(1024)

	exp := make([]string, 10)
	for i := range exp {
		exp[i] = fmt.Sprintf("value %d", i)
		enc.Write(exp[i])
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != stringCompressedSnappy {
		t.Fatalf("unexpected encoding: got %v, exp %v", b[0], stringCompressedSnappy)
	}

	if exp := 51; len(b) != exp {
		t.Fatalf("unexpected length: got %v, exp %v", len(b), exp)
	}

	got, err := StringArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected error creating string decoder: %v", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestStringArrayDecodeAll_Quick(t *testing.T) {
	quick.Check(func(values []string) bool {
		exp := values
		if values == nil {
			exp = []string{}
		}
		// Write values to encoder.
		enc := NewStringEncoder(1024)
		for _, v := range values {
			enc.Write(v)
		}

		// Retrieve encoded bytes from encoder.
		buf, err := enc.Bytes()
		if err != nil {
			t.Fatal(err)
		}

		// Read values out of decoder.
		got, err := StringArrayDecodeAll(buf, nil)
		if err != nil {
			t.Fatalf("unexpected error creating string decoder: %v", err)
		}

		if !cmp.Equal(got, exp) {
			t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
		}

		return true
	}, nil)
}

func TestStringArrayDecodeAll_Empty(t *testing.T) {
	got, err := StringArrayDecodeAll([]byte{}, nil)
	if err != nil {
		t.Fatalf("unexpected error creating string decoder: %v", err)
	}

	exp := []string{}
	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestStringArrayDecodeAll_CorruptBytes(t *testing.T) {
	cases := []string{
		"\x10\x03\b\x03Hi", // Higher length than actual data
		"\x10\x1dp\x9c\x90\x90\x90\x90\x90\x90\x90\x90\x90length overflow----",
		"0t\x00\x01\x000\x00\x01\x000\x00\x01\x000\x00\x01\x000\x00\x01" +
			"\x000\x00\x01\x000\x00\x01\x000\x00\x00\x00\xff:\x01\x00\x01\x00\x01" +
			"\x00\x01\x00\x01\x00\x01\x00\x010\x010\x000\x010\x010\x010\x01" +
			"0\x010\x010\x010\x010\x010\x010\x010\x010\x010\x010", // Upper slice bounds overflows negative
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%q", c), func(t *testing.T) {
			got, err := StringArrayDecodeAll([]byte(c), nil)
			if err == nil {
				t.Fatal("exp an err, got nil")
			}

			exp := []string{}
			if !cmp.Equal(got, exp) {
				t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})
	}
}

func BenchmarkEncodeStrings(b *testing.B) {
	var err error
	cases := []int{10, 100, 1000}

	for _, n := range cases {
		enc := NewStringEncoder(n)
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			input := make([]string, n)
			for i := 0; i < n; i++ {
				input[i] = uuid.TimeUUID().String()
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
					if bufResult, err = StringArrayEncodeAll(input, bufResult); err != nil {
						b.Fatal(err)
					}
				}
			})

		})
	}
}

func BenchmarkStringArrayDecodeAll(b *testing.B) {
	benchmarks := []struct {
		n int
		w int
	}{
		{1, 10},
		{55, 10},
		{550, 10},
		{1000, 10},
	}
	for _, bm := range benchmarks {
		rand.New(rand.NewSource(int64(bm.n * 1e3)))

		s := NewStringEncoder(bm.n)
		for c := 0; c < bm.n; c++ {
			s.Write(testutil.MakeSentence(bm.w))
		}
		s.Flush()
		bytes, err := s.Bytes()
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}

		b.Run(fmt.Sprintf("%d", bm.n), func(b *testing.B) {
			b.SetBytes(int64(len(bytes)))
			b.ReportAllocs()

			dst := make([]string, bm.n)
			for i := 0; i < b.N; i++ {
				got, err := StringArrayDecodeAll(bytes, dst)
				if err != nil {
					b.Fatalf("unexpected length -got/+exp\n%s", cmp.Diff(len(dst), bm.n))
				}
				if len(got) != bm.n {
					b.Fatalf("unexpected length -got/+exp\n%s", cmp.Diff(len(dst), bm.n))
				}
			}
		})
	}
}
