package tsm1

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"github.com/google/go-cmp/cmp"
)

func dumpBufs(a, b []byte) {
	longest := len(a)
	if len(b) > longest {
		longest = len(b)
	}

	for i := 0; i < longest; i++ {
		var as, bs string
		if i < len(a) {
			as = fmt.Sprintf("%08[1]b (%[1]d)", a[i])
		}
		if i < len(b) {
			bs = fmt.Sprintf("%08[1]b (%[1]d)", b[i])
		}

		same := as == bs
		fmt.Printf("%d (%d) %s - %s :: %v\n", i, i*8, as, bs, same)
	}
	fmt.Println()
}

func dumpBuf(b []byte) {
	for i, v := range b {
		fmt.Printf("%[1]d %08[2]b (%[2]d)\n", i, v)
	}
	fmt.Println()
}

func TestIntegerArrayEncodeAll_NoValues(t *testing.T) {
	b, err := IntegerArrayEncodeAll(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(b) > 0 {
		t.Fatalf("unexpected length: exp 0, got %v", len(b))
	}

	var dec IntegerDecoder
	dec.SetBytes(b)
	if dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}
}

func TestIntegerArrayEncodeAll_Compare(t *testing.T) {
	// generate random values (should use simple8b)
	input := make([]int64, 1000)
	for i := 0; i < len(input); i++ {
		input[i] = rand.Int63n(100000) - 50000
	}
	sort.Slice(input, func(i int, j int) bool { return input[i] < input[j] })
	testIntegerArrayEncodeAll_Compare(t, input, intCompressedSimple)

	// Generate same values (should use RLE)
	for i := 0; i < len(input); i++ {
		input[i] = 1232342341234
	}
	testIntegerArrayEncodeAll_Compare(t, input, intCompressedRLE)

	// Generate large random values that are not sorted. The deltas will be large
	// and the values should be stored uncompressed.
	for i := 0; i < len(input); i++ {
		input[i] = int64(rand.Uint64())
	}
	testIntegerArrayEncodeAll_Compare(t, input, intUncompressed)
}

func testIntegerArrayEncodeAll_Compare(t *testing.T, input []int64, encoding byte) {
	exp := make([]int64, len(input))
	copy(exp, input)

	s := NewIntegerEncoder(1000)
	for _, v := range input {
		s.Write(v)
	}

	buf1, err := s.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got, exp := buf1[0]>>4, encoding; got != exp {
		t.Fatalf("got encoding %v, expected %v", got, encoding)
	}

	var buf2 []byte
	buf2, err = IntegerArrayEncodeAll(input, buf2)
	if err != nil {
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	if got, exp := buf2[0]>>4, encoding; got != exp {
		t.Fatalf("got encoding %v, expected %v", got, encoding)
	}

	result, err := IntegerArrayDecodeAll(buf2, nil)
	if err != nil {
		dumpBufs(buf1, buf2)
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	if got := result; !reflect.DeepEqual(got, exp) {
		t.Fatalf("-got/+exp\n%s", cmp.Diff(got, exp))
	}

	// Check that the encoders are byte for byte the same...
	if !bytes.Equal(buf1, buf2) {
		dumpBufs(buf1, buf2)
		t.Fatalf("Raw bytes differ for encoders")
	}
}

func TestUnsignedArrayEncodeAll_Compare(t *testing.T) {
	// generate random values (should use simple8b)
	input := make([]uint64, 1000)
	for i := 0; i < len(input); i++ {
		input[i] = uint64(rand.Int63n(100000))
	}
	sort.Slice(input, func(i int, j int) bool { return input[i] < input[j] })
	testUnsignedArrayEncodeAll_Compare(t, input, intCompressedSimple)

	// Generate same values (should use RLE)
	for i := 0; i < len(input); i++ {
		input[i] = 1232342341234
	}
	testUnsignedArrayEncodeAll_Compare(t, input, intCompressedRLE)

	// Generate large random values that are not sorted. The deltas will be large
	// and the values should be stored uncompressed.
	for i := 0; i < len(input); i++ {
		input[i] = rand.Uint64()
	}
	testUnsignedArrayEncodeAll_Compare(t, input, intUncompressed)
}

func testUnsignedArrayEncodeAll_Compare(t *testing.T, input []uint64, encoding byte) {
	exp := make([]uint64, len(input))
	copy(exp, input)

	s := NewIntegerEncoder(1000)
	for _, v := range input {
		s.Write(int64(v))
	}

	buf1, err := s.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got, exp := buf1[0]>>4, encoding; got != exp {
		t.Fatalf("got encoding %v, expected %v", got, encoding)
	}

	var buf2 []byte
	buf2, err = UnsignedArrayEncodeAll(input, buf2)
	if err != nil {
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	if got, exp := buf2[0]>>4, encoding; got != exp {
		t.Fatalf("got encoding %v, expected %v", got, encoding)
	}

	result, err := UnsignedArrayDecodeAll(buf2, nil)
	if err != nil {
		dumpBufs(buf1, buf2)
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	if got := result; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got result %v, expected %v", got, exp)
	}

	// Check that the encoders are byte for byte the same...
	if !bytes.Equal(buf1, buf2) {
		dumpBufs(buf1, buf2)
		t.Fatalf("Raw bytes differ for encoders")
	}
}

func TestIntegerArrayEncodeAll_One(t *testing.T) {
	v1 := int64(1)

	src := []int64{1}
	b, err := IntegerArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; intCompressedSimple != got {
		t.Fatalf("encoding type mismatch: exp uncompressed, got %v", got)
	}

	var dec IntegerDecoder
	dec.SetBytes(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v1 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v1)
	}
}

func TestIntegerArrayEncodeAll_Two(t *testing.T) {
	var v1, v2 int64 = 1, 2

	src := []int64{v1, v2}
	b, err := IntegerArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; intCompressedSimple != got {
		t.Fatalf("encoding type mismatch: exp uncompressed, got %v", got)
	}

	var dec IntegerDecoder
	dec.SetBytes(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v1 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v1)
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v2 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v2)
	}
}

func TestIntegerArrayEncodeAll_Negative(t *testing.T) {
	var v1, v2, v3 int64 = -2, 0, 1

	src := []int64{v1, v2, v3}
	b, err := IntegerArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; intCompressedSimple != got {
		t.Fatalf("encoding type mismatch: exp uncompressed, got %v", got)
	}

	var dec IntegerDecoder
	dec.SetBytes(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v1 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v1)
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v2 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v2)
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v3 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v3)
	}
}

func TestIntegerArrayEncodeAll_Large_Range(t *testing.T) {
	exp := []int64{math.MaxInt64, 0, math.MaxInt64}

	b, err := IntegerArrayEncodeAll(append([]int64{}, exp...), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; intUncompressed != got {
		t.Fatalf("encoding type mismatch: exp uncompressed, got %v", got)
	}

	var dec IntegerDecoder
	dec.SetBytes(b)

	var got []int64
	for dec.Next() {
		got = append(got, dec.Read())
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unxpected result, -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestIntegerArrayEncodeAll_Uncompressed(t *testing.T) {
	var v1, v2, v3 int64 = 0, 1, 1 << 60

	src := []int64{v1, v2, v3}
	b, err := IntegerArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("expected error: %v", err)
	}

	// 1 byte header + 3 * 8 byte values
	if exp := 25; len(b) != exp {
		t.Fatalf("length mismatch: got %v, exp %v", len(b), exp)
	}

	if got := b[0] >> 4; intUncompressed != got {
		t.Fatalf("encoding type mismatch: exp uncompressed, got %v", got)
	}

	var dec IntegerDecoder
	dec.SetBytes(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v1 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v1)
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v2 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v2)
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if v3 != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), v3)
	}
}

func TestIntegerArrayEncodeAll_NegativeUncompressed(t *testing.T) {
	src := []int64{
		-2352281900722994752, 1438442655375607923, -4110452567888190110,
		-1221292455668011702, -1941700286034261841, -2836753127140407751,
		1432686216250034552, 3663244026151507025, -3068113732684750258,
		-1949953187327444488, 3713374280993588804, 3226153669854871355,
		-2093273755080502606, 1006087192578600616, -2272122301622271655,
		2533238229511593671, -4450454445568858273, 2647789901083530435,
		2761419461769776844, -1324397441074946198, -680758138988210958,
		94468846694902125, -2394093124890745254, -2682139311758778198,
	}
	exp := make([]int64, len(src))
	copy(exp, src)

	b, err := IntegerArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("expected error: %v", err)
	}

	if got := b[0] >> 4; intUncompressed != got {
		t.Fatalf("encoding type mismatch: exp uncompressed, got %v", got)
	}

	var dec IntegerDecoder
	dec.SetBytes(b)

	i := 0
	for dec.Next() {
		if i > len(src) {
			t.Fatalf("read too many values: got %v, exp %v", i, len(exp))
		}

		if exp[i] != dec.Read() {
			t.Fatalf("read value %d mismatch: got %v, exp %v", i, dec.Read(), exp[i])
		}
		i += 1
	}

	if i != len(exp) {
		t.Fatalf("failed to read enough values: got %v, exp %v", i, len(exp))
	}
}

func TestIntegerArrayEncodeAll_AllNegative(t *testing.T) {
	src := []int64{
		-10, -5, -1,
	}
	exp := make([]int64, len(src))
	copy(exp, src)

	b, err := IntegerArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; intCompressedSimple != got {
		t.Fatalf("encoding type mismatch: exp uncompressed, got %v", got)
	}

	var dec IntegerDecoder
	dec.SetBytes(b)
	i := 0
	for dec.Next() {
		if i > len(exp) {
			t.Fatalf("read too many values: got %v, exp %v", i, len(exp))
		}

		if exp[i] != dec.Read() {
			t.Fatalf("read value %d mismatch: got %v, exp %v", i, dec.Read(), exp[i])
		}
		i += 1
	}

	if i != len(exp) {
		t.Fatalf("failed to read enough values: got %v, exp %v", i, len(exp))
	}
}

func TestIntegerArrayEncodeAll_CounterPacked(t *testing.T) {
	src := []int64{
		1e15, 1e15 + 1, 1e15 + 2, 1e15 + 3, 1e15 + 4, 1e15 + 6,
	}
	exp := make([]int64, len(src))
	copy(exp, src)

	b, err := IntegerArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != intCompressedSimple {
		t.Fatalf("unexpected encoding format: expected simple, got %v", b[0]>>4)
	}

	// Should use 1 header byte + 2, 8 byte words if delta-encoding is used based on
	// values sizes.  Without delta-encoding, we'd get 49 bytes.
	if exp := 17; len(b) != exp {
		t.Fatalf("encoded length mismatch: got %v, exp %v", len(b), exp)
	}

	var dec IntegerDecoder
	dec.SetBytes(b)
	i := 0
	for dec.Next() {
		if i > len(exp) {
			t.Fatalf("read too many values: got %v, exp %v", i, len(exp))
		}

		if exp[i] != dec.Read() {
			t.Fatalf("read value %d mismatch: got %v, exp %v", i, dec.Read(), exp[i])
		}
		i += 1
	}

	if i != len(exp) {
		t.Fatalf("failed to read enough values: got %v, exp %v", i, len(exp))
	}
}

func TestIntegerArrayEncodeAll_CounterRLE(t *testing.T) {
	src := []int64{
		1e15, 1e15 + 1, 1e15 + 2, 1e15 + 3, 1e15 + 4, 1e15 + 5,
	}
	exp := make([]int64, len(src))
	copy(exp, src)

	b, err := IntegerArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != intCompressedRLE {
		t.Fatalf("unexpected encoding format: expected RLE, got %v", b[0]>>4)
	}

	// Should use 1 header byte, 8 byte first value, 1 var-byte for delta and 1 var-byte for
	// count of deltas in this particular RLE.
	if exp := 11; len(b) != exp {
		t.Fatalf("encoded length mismatch: got %v, exp %v", len(b), exp)
	}

	var dec IntegerDecoder
	dec.SetBytes(b)
	i := 0
	for dec.Next() {
		if i > len(exp) {
			t.Fatalf("read too many values: got %v, exp %v", i, len(exp))
		}

		if exp[i] != dec.Read() {
			t.Fatalf("read value %d mismatch: got %v, exp %v", i, dec.Read(), exp[i])
		}
		i += 1
	}

	if i != len(exp) {
		t.Fatalf("failed to read enough values: got %v, exp %v", i, len(exp))
	}
}

func TestIntegerArrayEncodeAll_Descending(t *testing.T) {
	src := []int64{
		7094, 4472, 1850,
	}
	exp := make([]int64, len(src))
	copy(exp, src)

	b, err := IntegerArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != intCompressedRLE {
		t.Fatalf("unexpected encoding format: expected simple, got %v", b[0]>>4)
	}

	// Should use 1 header byte, 8 byte first value, 1 var-byte for delta and 1 var-byte for
	// count of deltas in this particular RLE.
	if exp := 12; len(b) != exp {
		t.Fatalf("encoded length mismatch: got %v, exp %v", len(b), exp)
	}

	var dec IntegerDecoder
	dec.SetBytes(b)
	i := 0
	for dec.Next() {
		if i > len(exp) {
			t.Fatalf("read too many values: got %v, exp %v", i, len(exp))
		}

		if exp[i] != dec.Read() {
			t.Fatalf("read value %d mismatch: got %v, exp %v", i, dec.Read(), exp[i])
		}
		i += 1
	}

	if i != len(exp) {
		t.Fatalf("failed to read enough values: got %v, exp %v", i, len(exp))
	}
}

func TestIntegerArrayEncodeAll_Flat(t *testing.T) {
	src := []int64{
		1, 1, 1, 1,
	}
	exp := make([]int64, len(src))
	copy(exp, src)

	b, err := IntegerArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != intCompressedRLE {
		t.Fatalf("unexpected encoding format: expected simple, got %v", b[0]>>4)
	}

	// Should use 1 header byte, 8 byte first value, 1 var-byte for delta and 1 var-byte for
	// count of deltas in this particular RLE.
	if exp := 11; len(b) != exp {
		t.Fatalf("encoded length mismatch: got %v, exp %v", len(b), exp)
	}

	var dec IntegerDecoder
	dec.SetBytes(b)
	i := 0
	for dec.Next() {
		if i > len(exp) {
			t.Fatalf("read too many values: got %v, exp %v", i, len(exp))
		}

		if exp[i] != dec.Read() {
			t.Fatalf("read value %d mismatch: got %v, exp %v", i, dec.Read(), exp[i])
		}
		i += 1
	}

	if i != len(exp) {
		t.Fatalf("failed to read enough values: got %v, exp %v", i, len(exp))
	}
}

func TestIntegerArrayEncodeAll_MinMax(t *testing.T) {
	src := []int64{
		math.MinInt64, math.MaxInt64,
	}
	exp := make([]int64, len(src))
	copy(exp, src)

	b, err := IntegerArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != intCompressedSimple {
		t.Fatalf("unexpected encoding format: expected simple, got %v", b[0]>>4)
	}

	if exp := 17; len(b) != exp {
		t.Fatalf("encoded length mismatch: got %v, exp %v", len(b), exp)
	}

	var dec IntegerDecoder
	dec.SetBytes(b)
	i := 0
	for dec.Next() {
		if i > len(exp) {
			t.Fatalf("read too many values: got %v, exp %v", i, len(exp))
		}

		if exp[i] != dec.Read() {
			t.Fatalf("read value %d mismatch: got %v, exp %v", i, dec.Read(), exp[i])
		}
		i += 1
	}

	if i != len(exp) {
		t.Fatalf("failed to read enough values: got %v, exp %v", i, len(exp))
	}
}

func TestIntegerArrayEncodeAll_Quick(t *testing.T) {
	quick.Check(func(values []int64) bool {
		src := values
		if values == nil {
			src = []int64{} // is this really expected?
		}

		// Copy over values to compare result—src is modified...
		exp := make([]int64, 0, len(src))
		for _, v := range src {
			exp = append(exp, v)
		}

		// Retrieve encoded bytes from encoder.
		b, err := IntegerArrayEncodeAll(src, nil)
		if err != nil {
			t.Fatal(err)
		}

		// Read values out of decoder.
		got := make([]int64, 0, len(src))
		var dec IntegerDecoder
		dec.SetBytes(b)
		for dec.Next() {
			if err := dec.Error(); err != nil {
				t.Fatal(err)
			}
			got = append(got, dec.Read())
		}

		// Verify that input and output values match.
		if !reflect.DeepEqual(exp, got) {
			t.Fatalf("mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", src, got)
		}

		return true
	}, nil)
}

func TestIntegerArrayDecodeAll_NegativeUncompressed(t *testing.T) {
	exp := []int64{
		-2352281900722994752, 1438442655375607923, -4110452567888190110,
		-1221292455668011702, -1941700286034261841, -2836753127140407751,
		1432686216250034552, 3663244026151507025, -3068113732684750258,
		-1949953187327444488, 3713374280993588804, 3226153669854871355,
		-2093273755080502606, 1006087192578600616, -2272122301622271655,
		2533238229511593671, -4450454445568858273, 2647789901083530435,
		2761419461769776844, -1324397441074946198, -680758138988210958,
		94468846694902125, -2394093124890745254, -2682139311758778198,
	}
	enc := NewIntegerEncoder(256)
	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("expected error: %v", err)
	}

	if got := b[0] >> 4; intUncompressed != got {
		t.Fatalf("encoding type mismatch: exp uncompressed, got %v", got)
	}

	got, err := IntegerArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestIntegerArrayDecodeAll_AllNegative(t *testing.T) {
	enc := NewIntegerEncoder(3)
	exp := []int64{
		-10, -5, -1,
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; intCompressedSimple != got {
		t.Fatalf("encoding type mismatch: exp uncompressed, got %v", got)
	}

	got, err := IntegerArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestIntegerArrayDecodeAll_CounterPacked(t *testing.T) {
	enc := NewIntegerEncoder(16)
	exp := []int64{
		1e15, 1e15 + 1, 1e15 + 2, 1e15 + 3, 1e15 + 4, 1e15 + 6,
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != intCompressedSimple {
		t.Fatalf("unexpected encoding format: expected simple, got %v", b[0]>>4)
	}

	// Should use 1 header byte + 2, 8 byte words if delta-encoding is used based on
	// values sizes.  Without delta-encoding, we'd get 49 bytes.
	if exp := 17; len(b) != exp {
		t.Fatalf("encoded length mismatch: got %v, exp %v", len(b), exp)
	}

	got, err := IntegerArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestIntegerArrayDecodeAll_CounterRLE(t *testing.T) {
	enc := NewIntegerEncoder(16)
	exp := []int64{
		1e15, 1e15 + 1, 1e15 + 2, 1e15 + 3, 1e15 + 4, 1e15 + 5,
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != intCompressedRLE {
		t.Fatalf("unexpected encoding format: expected RLE, got %v", b[0]>>4)
	}

	// Should use 1 header byte, 8 byte first value, 1 var-byte for delta and 1 var-byte for
	// count of deltas in this particular RLE.
	if exp := 11; len(b) != exp {
		t.Fatalf("encoded length mismatch: got %v, exp %v", len(b), exp)
	}

	got, err := IntegerArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestIntegerArrayDecodeAll_Descending(t *testing.T) {
	enc := NewIntegerEncoder(16)
	exp := []int64{
		7094, 4472, 1850,
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != intCompressedRLE {
		t.Fatalf("unexpected encoding format: expected simple, got %v", b[0]>>4)
	}

	// Should use 1 header byte, 8 byte first value, 1 var-byte for delta and 1 var-byte for
	// count of deltas in this particular RLE.
	if exp := 12; len(b) != exp {
		t.Fatalf("encoded length mismatch: got %v, exp %v", len(b), exp)
	}

	got, err := IntegerArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestIntegerArrayDecodeAll_Flat(t *testing.T) {
	enc := NewIntegerEncoder(16)
	exp := []int64{
		1, 1, 1, 1,
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != intCompressedRLE {
		t.Fatalf("unexpected encoding format: expected simple, got %v", b[0]>>4)
	}

	// Should use 1 header byte, 8 byte first value, 1 var-byte for delta and 1 var-byte for
	// count of deltas in this particular RLE.
	if exp := 11; len(b) != exp {
		t.Fatalf("encoded length mismatch: got %v, exp %v", len(b), exp)
	}

	got, err := IntegerArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestIntegerArrayDecodeAll_MinMax(t *testing.T) {
	enc := NewIntegerEncoder(2)
	exp := []int64{
		math.MinInt64, math.MaxInt64,
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if b[0]>>4 != intUncompressed {
		t.Fatalf("unexpected encoding format: expected simple, got %v", b[0]>>4)
	}

	if exp := 17; len(b) != exp {
		t.Fatalf("encoded length mismatch: got %v, exp %v", len(b), exp)
	}

	got, err := IntegerArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestIntegerArrayDecodeAll_Quick(t *testing.T) {
	quick.Check(func(values []int64) bool {
		exp := values
		if values == nil {
			exp = []int64{} // is this really expected?
		}

		// Write values to encoder.
		enc := NewIntegerEncoder(1024)
		for _, v := range values {
			enc.Write(v)
		}

		// Retrieve encoded bytes from encoder.
		buf, err := enc.Bytes()
		if err != nil {
			t.Fatal(err)
		}

		// Read values out of decoder.
		got, err := IntegerArrayDecodeAll(buf, nil)
		if err != nil {
			t.Fatalf("unexpected decode error %q", err)
		}

		if !cmp.Equal(got, exp) {
			t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
		}

		return true
	}, nil)
}

var bufResult []byte

func BenchmarkEncodeIntegers(b *testing.B) {
	var err error
	cases := []int{10, 100, 1000}

	for _, n := range cases {
		enc := NewIntegerEncoder(n)

		b.Run(fmt.Sprintf("%d_seq", n), func(b *testing.B) {
			src := make([]int64, n)
			for i := 0; i < n; i++ {
				src[i] = int64(i)
			}

			input := make([]int64, len(src))
			copy(input, src)

			b.Run("itr", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					enc.Reset()
					for _, x := range src {
						enc.Write(x)
					}
					enc.Flush()
					if bufResult, err = enc.Bytes(); err != nil {
						b.Fatal(err)
					}

					// Since the batch encoder needs to do a copy to reset the
					// input, we will add a copy here too.
					copy(input, src)
				}
			})

			b.Run("batch", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					if bufResult, err = IntegerArrayEncodeAll(input, bufResult); err != nil {
						b.Fatal(err)
					}
					copy(input, src) // Reset input that gets modified in IntegerArrayEncodeAll
				}
			})

		})

		b.Run(fmt.Sprintf("%d_ran", n), func(b *testing.B) {
			src := make([]int64, n)
			for i := 0; i < n; i++ {
				src[i] = rand.Int63n(100)
			}

			input := make([]int64, len(src))
			copy(input, src)

			b.Run("itr", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					enc.Reset()
					for _, x := range src {
						enc.Write(x)
					}
					enc.Flush()
					if bufResult, err = enc.Bytes(); err != nil {
						b.Fatal(err)
					}

					// Since the batch encoder needs to do a copy to reset the
					// input, we will add a copy here too.
					copy(input, src)
				}
			})

			b.Run("batch", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					if bufResult, err = IntegerArrayEncodeAll(input, bufResult); err != nil {
						b.Fatal(err)
					}
					copy(input, src) // Reset input that gets modified in IntegerArrayEncodeAll
				}
			})
		})

		b.Run(fmt.Sprintf("%d_dup", n), func(b *testing.B) {
			src := make([]int64, n)
			for i := 0; i < n; i++ {
				src[i] = 1233242
			}

			input := make([]int64, len(src))
			copy(input, src)

			b.Run("itr", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					enc.Reset()
					for _, x := range src {
						enc.Write(x)
					}
					enc.Flush()
					if bufResult, err = enc.Bytes(); err != nil {
						b.Fatal(err)
					}

					// Since the batch encoder needs to do a copy to reset the
					// input, we will add a copy here too.
					copy(input, src)
				}
			})

			b.Run("batch", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					if bufResult, err = IntegerArrayEncodeAll(input, bufResult); err != nil {
						b.Fatal(err)
					}
					copy(input, src) // Reset input that gets modified in IntegerArrayEncodeAll
				}
			})
		})
	}
}

func BenchmarkIntegerArrayDecodeAllUncompressed(b *testing.B) {
	benchmarks := []int{
		5,
		55,
		555,
		1000,
	}

	values := []int64{
		-2352281900722994752, 1438442655375607923, -4110452567888190110,
		-1221292455668011702, -1941700286034261841, -2836753127140407751,
		1432686216250034552, 3663244026151507025, -3068113732684750258,
		-1949953187327444488, 3713374280993588804, 3226153669854871355,
		-2093273755080502606, 1006087192578600616, -2272122301622271655,
		2533238229511593671, -4450454445568858273, 2647789901083530435,
		2761419461769776844, -1324397441074946198, -680758138988210958,
		94468846694902125, -2394093124890745254, -2682139311758778198,
	}

	for _, size := range benchmarks {
		rand.New(rand.NewSource(int64(size * 1e3)))

		enc := NewIntegerEncoder(size)
		for i := 0; i < size; i++ {
			enc.Write(values[rand.Int()%len(values)])
		}
		bytes, _ := enc.Bytes()

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.SetBytes(int64(len(bytes)))
			b.ReportAllocs()

			dst := make([]int64, size)
			for i := 0; i < b.N; i++ {
				dst, _ = IntegerArrayDecodeAll(bytes, dst)
			}
		})
	}
}

func BenchmarkIntegerArrayDecodeAllPackedSimple(b *testing.B) {
	benchmarks := []int{
		5,
		55,
		555,
		1000,
	}
	for _, size := range benchmarks {
		rand.New(rand.NewSource(int64(size * 1e3)))

		enc := NewIntegerEncoder(size)
		for i := 0; i < size; i++ {
			// Small amount of randomness prevents RLE from being used
			enc.Write(int64(i) + int64(rand.Intn(10)))
		}
		bytes, _ := enc.Bytes()

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.SetBytes(int64(len(bytes)))
			b.ReportAllocs()

			dst := make([]int64, size)
			for i := 0; i < b.N; i++ {
				IntegerArrayDecodeAll(bytes, dst)
			}
		})
	}
}

func BenchmarkIntegerArrayDecodeAllRLE(b *testing.B) {
	benchmarks := []struct {
		n     int
		delta int64
	}{
		{5, 1},
		{55, 1},
		{555, 1},
		{1000, 1},
		{1000, 0},
	}
	for _, bm := range benchmarks {
		rand.New(rand.NewSource(int64(bm.n * 1e3)))

		enc := NewIntegerEncoder(bm.n)
		acc := int64(0)
		for i := 0; i < bm.n; i++ {
			enc.Write(acc)
			acc += bm.delta
		}
		bytes, _ := enc.Bytes()

		b.Run(fmt.Sprintf("%d_delta_%d", bm.n, bm.delta), func(b *testing.B) {
			b.SetBytes(int64(len(bytes)))
			b.ReportAllocs()

			dst := make([]int64, bm.n)
			for i := 0; i < b.N; i++ {
				IntegerArrayDecodeAll(bytes, dst)
			}
		})
	}
}
