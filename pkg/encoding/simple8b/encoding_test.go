package simple8b_test

import (
	"math/rand"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/pkg/encoding/simple8b"
)

func Test_Encode_NoValues(t *testing.T) {
	var in []uint64
	encoded, _ := simple8b.EncodeAll(in)

	decoded := make([]uint64, len(in))
	n, _ := simple8b.DecodeAll(decoded, encoded)

	if len(in) != len(decoded[:n]) {
		t.Fatalf("Len mismatch: got %v, exp %v", len(decoded), len(in))
	}
}

func ones(n int) func() []uint64 {
	return func() []uint64 {
		in := make([]uint64, n)
		for i := 0; i < n; i++ {
			in[i] = 1
		}
		return in
	}
}

func bitsN(b int) func(n int) func() []uint64 {
	return func(n int) func() []uint64 {
		return bits(n, b)
	}
}

func combineN(fns ...func(n int) func() []uint64) func(n int) func() []uint64 {
	return func(n int) func() []uint64 {
		var out []func() []uint64
		for _, fn := range fns {
			out = append(out, fn(n))
		}
		return combine(out...)
	}
}

// bits generates sequence of n numbers with max bits,
// ensuring max bit is set for 50% of the values.
func bits(n, bits int) func() []uint64 {
	return func() []uint64 {
		out := make([]uint64, n)
		maxVal := uint64(1 << uint8(bits))
		for i := range out {
			topBit := uint64((i & 1) << uint8(bits-1))
			out[i] = uint64(rand.Int63n(int64(maxVal))) | topBit
			if out[i] >= maxVal {
				panic("max")
			}
		}
		return out
	}
}

func combine(fns ...func() []uint64) func() []uint64 {
	return func() []uint64 {
		var out []uint64
		for _, fn := range fns {
			out = append(out, fn()...)
		}
		return out
	}
}

// TestEncodeAll ensures 100% test coverage of simple8b.EncodeAll and
// verifies all output by comparing the original input with the output of simple8b.DecodeAll
func TestEncodeAll(t *testing.T) {
	rand.New(rand.NewSource(0))

	tests := []struct {
		name string
		in   []uint64
		fn   func() []uint64
		err  error
	}{
		{name: "no values", in: []uint64{}},
		{name: "mixed sizes", in: []uint64{7, 6, 256, 4, 3, 2, 1}},
		{name: "too big", in: []uint64{7, 6, 2<<61 - 1, 4, 3, 2, 1}, err: simple8b.ErrValueOutOfBounds},
		{name: "1 bit", fn: bits(100, 1)},
		{name: "2 bits", fn: bits(100, 2)},
		{name: "3 bits", fn: bits(100, 3)},
		{name: "4 bits", fn: bits(100, 4)},
		{name: "5 bits", fn: bits(100, 5)},
		{name: "6 bits", fn: bits(100, 6)},
		{name: "7 bits", fn: bits(100, 7)},
		{name: "8 bits", fn: bits(100, 8)},
		{name: "10 bits", fn: bits(100, 10)},
		{name: "12 bits", fn: bits(100, 12)},
		{name: "15 bits", fn: bits(100, 15)},
		{name: "20 bits", fn: bits(100, 20)},
		{name: "30 bits", fn: bits(100, 30)},
		{name: "60 bits", fn: bits(100, 60)},
		{name: "combination", fn: combine(
			bits(100, 1),
			bits(100, 2),
			bits(100, 3),
			bits(100, 4),
			bits(100, 5),
			bits(100, 6),
			bits(100, 7),
			bits(100, 8),
			bits(100, 10),
			bits(100, 12),
			bits(100, 15),
			bits(100, 20),
			bits(100, 30),
			bits(100, 60),
		)},
		{name: "240 ones", fn: ones(240)},
		{name: "120 ones", fn: func() []uint64 {
			in := ones(240)()
			in[120] = 5
			return in
		}},
		{name: "119 ones", fn: func() []uint64 {
			in := ones(240)()
			in[119] = 5
			return in
		}},
		{name: "239 ones", fn: func() []uint64 {
			in := ones(241)()
			in[239] = 5
			return in
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.fn != nil {
				test.in = test.fn()
			}

			encoded, err := simple8b.EncodeAll(append(make([]uint64, 0, len(test.in)), test.in...))
			if test.err != nil {
				if err != test.err {
					t.Fatalf("expected encode error, got\n%s", err)
				}
				return
			}

			decoded := make([]uint64, len(test.in))
			n, err := simple8b.DecodeAll(decoded, encoded)
			if err != nil {
				t.Fatalf("unexpected decode error\n%s", err)
			}

			if !cmp.Equal(decoded[:n], test.in) {
				t.Fatalf("unexpected values; +got/-exp\n%s", cmp.Diff(decoded, test.in))
			}
		})
	}
}

func Test_FewValues(t *testing.T) {
	testEncode(t, 20, 2)
}

func Test_Encode_Multiple_Zeros(t *testing.T) {
	testEncode(t, 250, 0)
}

func Test_Encode_Multiple_Ones(t *testing.T) {
	testEncode(t, 250, 1)
}

func Test_Encode_Multiple_Large(t *testing.T) {
	testEncode(t, 250, 134)
}

func Test_Encode_240Ones(t *testing.T) {
	testEncode(t, 240, 1)
}

func Test_Encode_120Ones(t *testing.T) {
	testEncode(t, 120, 1)
}

func Test_Encode_60(t *testing.T) {
	testEncode(t, 60, 1)
}

func Test_Encode_30(t *testing.T) {
	testEncode(t, 30, 3)
}

func Test_Encode_20(t *testing.T) {
	testEncode(t, 20, 7)
}

func Test_Encode_15(t *testing.T) {
	testEncode(t, 15, 15)
}

func Test_Encode_12(t *testing.T) {
	testEncode(t, 12, 31)
}

func Test_Encode_10(t *testing.T) {
	testEncode(t, 10, 63)
}

func Test_Encode_8(t *testing.T) {
	testEncode(t, 8, 127)
}

func Test_Encode_7(t *testing.T) {
	testEncode(t, 7, 255)
}

func Test_Encode_6(t *testing.T) {
	testEncode(t, 6, 1023)
}

func Test_Encode_5(t *testing.T) {
	testEncode(t, 5, 4095)
}

func Test_Encode_4(t *testing.T) {
	testEncode(t, 4, 32767)
}

func Test_Encode_3(t *testing.T) {
	testEncode(t, 3, 1048575)
}

func Test_Encode_2(t *testing.T) {
	testEncode(t, 2, 1073741823)
}

func Test_Encode_1(t *testing.T) {
	testEncode(t, 1, 1152921504606846975)
}

func testEncode(t *testing.T, n int, val uint64) {
	enc := simple8b.NewEncoder()
	in := make([]uint64, n)
	for i := 0; i < n; i++ {
		in[i] = val
		enc.Write(in[i])
	}

	encoded, err := enc.Bytes()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	dec := simple8b.NewDecoder(encoded)
	i := 0
	for dec.Next() {
		if i >= len(in) {
			t.Fatalf("Decoded too many values: got %v, exp %v", i, len(in))
		}

		if dec.Read() != in[i] {
			t.Fatalf("Decoded[%d] != %v, got %v", i, in[i], dec.Read())
		}
		i += 1
	}

	if exp, got := n, i; got != exp {
		t.Fatalf("Decode len mismatch: exp %v, got %v", exp, got)
	}

	got, err := simple8b.CountBytes(encoded)
	if err != nil {
		t.Fatalf("Unexpected error in Count: %v", err)
	}
	if got != n {
		t.Fatalf("Count mismatch: got %v, exp %v", got, n)
	}

}

func Test_Bytes(t *testing.T) {
	enc := simple8b.NewEncoder()
	for i := 0; i < 30; i++ {
		enc.Write(uint64(i))
	}
	b, _ := enc.Bytes()

	dec := simple8b.NewDecoder(b)
	x := uint64(0)
	for dec.Next() {
		if x != dec.Read() {
			t.Fatalf("mismatch: got %v, exp %v", dec.Read(), x)
		}
		x += 1
	}
}

func Test_Encode_ValueTooLarge(t *testing.T) {
	enc := simple8b.NewEncoder()

	values := []uint64{
		1442369134000000000, 0,
	}

	for _, v := range values {
		enc.Write(v)
	}

	_, err := enc.Bytes()
	if err == nil {
		t.Fatalf("Expected error, got nil")

	}
}

func Test_Decode_NotEnoughBytes(t *testing.T) {
	dec := simple8b.NewDecoder([]byte{0})
	if dec.Next() {
		t.Fatalf("Expected Next to return false but it returned true")
	}
}

func TestCountBytesBetween(t *testing.T) {
	enc := simple8b.NewEncoder()
	in := make([]uint64, 8)
	for i := 0; i < len(in); i++ {
		in[i] = uint64(i)
		enc.Write(in[i])
	}

	encoded, err := enc.Bytes()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	dec := simple8b.NewDecoder(encoded)
	i := 0
	for dec.Next() {
		if i >= len(in) {
			t.Fatalf("Decoded too many values: got %v, exp %v", i, len(in))
		}

		if dec.Read() != in[i] {
			t.Fatalf("Decoded[%d] != %v, got %v", i, in[i], dec.Read())
		}
		i += 1
	}

	if exp, got := len(in), i; got != exp {
		t.Fatalf("Decode len mismatch: exp %v, got %v", exp, got)
	}

	got, err := simple8b.CountBytesBetween(encoded, 2, 6)
	if err != nil {
		t.Fatalf("Unexpected error in Count: %v", err)
	}
	if got != 4 {
		t.Fatalf("Count mismatch: got %v, exp %v", got, 4)
	}
}

func TestCountBytesBetween_SkipMin(t *testing.T) {
	enc := simple8b.NewEncoder()
	in := make([]uint64, 8)
	for i := 0; i < len(in); i++ {
		in[i] = uint64(i)
		enc.Write(in[i])
	}
	in = append(in, 100000)
	enc.Write(100000)

	encoded, err := enc.Bytes()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	dec := simple8b.NewDecoder(encoded)
	i := 0
	for dec.Next() {
		if i >= len(in) {
			t.Fatalf("Decoded too many values: got %v, exp %v", i, len(in))
		}

		if dec.Read() != in[i] {
			t.Fatalf("Decoded[%d] != %v, got %v", i, in[i], dec.Read())
		}
		i += 1
	}

	if exp, got := len(in), i; got != exp {
		t.Fatalf("Decode len mismatch: exp %v, got %v", exp, got)
	}

	got, err := simple8b.CountBytesBetween(encoded, 100000, 100001)
	if err != nil {
		t.Fatalf("Unexpected error in Count: %v", err)
	}
	if got != 1 {
		t.Fatalf("Count mismatch: got %v, exp %v", got, 1)
	}
}

func BenchmarkEncodeAll(b *testing.B) {
	benchmarks := []struct {
		name string
		fn   func(n int) func() []uint64
	}{
		{name: "1 bit", fn: bitsN(1)},
		{name: "2 bits", fn: bitsN(2)},
		{name: "3 bits", fn: bitsN(3)},
		{name: "4 bits", fn: bitsN(4)},
		{name: "5 bits", fn: bitsN(5)},
		{name: "6 bits", fn: bitsN(6)},
		{name: "7 bits", fn: bitsN(7)},
		{name: "8 bits", fn: bitsN(8)},
		{name: "10 bits", fn: bitsN(10)},
		{name: "12 bits", fn: bitsN(12)},
		{name: "15 bits", fn: bitsN(15)},
		{name: "20 bits", fn: bitsN(20)},
		{name: "30 bits", fn: bitsN(30)},
		{name: "60 bits", fn: bitsN(60)},
		{name: "combination", fn: combineN(
			bitsN(1),
			bitsN(2),
			bitsN(3),
			bitsN(4),
			bitsN(5),
			bitsN(6),
			bitsN(7),
			bitsN(8),
			bitsN(10),
			bitsN(12),
			bitsN(15),
			bitsN(20),
			bitsN(30),
			bitsN(60),
		)},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				in := bm.fn(1000)()
				simple8b.EncodeAll(append(make([]uint64, 0, len(in)), in...))
			}
		})
	}
}

func BenchmarkEncode(b *testing.B) {
	x := make([]uint64, 1024)
	for i := 0; i < len(x); i++ {
		x[i] = uint64(15)
	}

	in := make([]uint64, 1024)

	b.SetBytes(int64(len(x) * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copy(in, x)
		simple8b.EncodeAll(in)
	}
}

func BenchmarkEncoder(b *testing.B) {
	x := make([]uint64, 1024)
	for i := 0; i < len(x); i++ {
		x[i] = uint64(15)
	}

	enc := simple8b.NewEncoder()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc.SetValues(x)
		enc.Bytes()
		b.SetBytes(int64(len(x)) * 8)
	}
}
func BenchmarkDecode(b *testing.B) {
	total := 0

	x := make([]uint64, 1024)
	for i := 0; i < len(x); i++ {
		x[i] = uint64(10)
	}
	y, _ := simple8b.EncodeAll(x)

	decoded := make([]uint64, len(x))

	b.SetBytes(int64(len(decoded) * 8))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = simple8b.DecodeAll(decoded, y)
		total += len(decoded)
	}
}

func BenchmarkDecoder(b *testing.B) {
	enc := simple8b.NewEncoder()
	x := make([]uint64, 1024)
	for i := 0; i < len(x); i++ {
		x[i] = uint64(10)
		enc.Write(x[i])
	}
	y, _ := enc.Bytes()

	b.ResetTimer()

	dec := simple8b.NewDecoder(y)
	for i := 0; i < b.N; i++ {
		dec.SetBytes(y)
		j := 0
		for dec.Next() {
			j += 1
		}
		b.SetBytes(int64(j * 8))
	}
}
