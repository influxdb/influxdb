package tsm1

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"testing/quick"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestTimeArrayEncodeAll(t *testing.T) {
	now := time.Unix(0, 0)
	src := []int64{now.UnixNano()}

	for i := 1; i < 4; i++ {
		src = append(src, now.Add(time.Duration(i)*time.Second).UnixNano())
	}

	exp := make([]int64, len(src))
	copy(exp, src)

	b, err := TimeArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	var dec TimeDecoder
	dec.Init(b)
	for i, v := range exp {
		if !dec.Next() {
			t.Fatalf("Next == false, expected true")
		}

		if v != dec.Read() {
			t.Fatalf("Item %d mismatch, got %v, exp %v", i, dec.Read(), v)
		}
	}
}

// This test compares the ArrayEncoder to the original iterator encoder, byte for
// byte.
func TestTimeArrayEncodeAll_Compare(t *testing.T) {
	// generate random values (should use simple8b)
	input := make([]int64, 1000)
	for i := 0; i < len(input); i++ {
		input[i] = rand.Int63n(100000) - 50000
	}
	sort.Slice(input, func(i int, j int) bool { return input[i] < input[j] })
	testTimeArrayEncodeAll_Compare(t, input, timeCompressedPackedSimple)

	// Generate same values (should use RLE)
	for i := 0; i < len(input); i++ {
		input[i] = 1232342341234
	}
	testTimeArrayEncodeAll_Compare(t, input, timeCompressedRLE)

	// Generate large random values that are not sorted. The deltas will be large
	// and the values should be stored uncompressed.
	for i := 0; i < len(input); i++ {
		input[i] = int64(rand.Uint64())
	}
	testTimeArrayEncodeAll_Compare(t, input, timeUncompressed)
}

func testTimeArrayEncodeAll_Compare(t *testing.T, input []int64, encoding byte) {
	exp := make([]int64, len(input))
	copy(exp, input)

	s := NewTimeEncoder(1000)
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
	buf2, err = TimeArrayEncodeAll(input, buf2)
	if err != nil {
		t.Fatalf("unexpected error: %v\nbuf: %db %x", err, len(buf2), buf2)
	}

	if got, exp := buf2[0]>>4, encoding; got != exp {
		t.Fatalf("got encoding %v, expected %v", got, encoding)
	}

	result, err := TimeArrayDecodeAll(buf2, nil)
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

func TestTimeArrayEncodeAll_NoValues(t *testing.T) {
	b, err := TimeArrayEncodeAll(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var dec TimeDecoder
	dec.Init(b)
	if dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}
}

func TestTimeArrayEncodeAll_One(t *testing.T) {
	src := []int64{0}
	exp := make([]int64, len(src))
	copy(exp, src)

	b, err := TimeArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedPackedSimple {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	var dec TimeDecoder
	dec.Init(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[0] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[0])
	}
}

func TestTimeArrayEncodeAll_Two(t *testing.T) {
	src := []int64{0, 1}
	exp := make([]int64, len(src))
	copy(exp, src)

	b, err := TimeArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	var dec TimeDecoder
	dec.Init(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[0] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[0])
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[1] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[1])
	}
}

func TestTimeArrayEncodeAll_Three(t *testing.T) {
	src := []int64{0, 1, 3}
	exp := make([]int64, len(src))
	copy(exp, src)

	b, err := TimeArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedPackedSimple {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	var dec TimeDecoder
	dec.Init(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[0] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[0])
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[1] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[1])
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[2] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[2])
	}
}

func TestTimeArrayEncodeAll_Large_Range(t *testing.T) {
	src := []int64{1442369134000000000, 1442369135000000000}
	exp := make([]int64, len(src))
	copy(exp, src)

	b, err := TimeArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	var dec TimeDecoder
	dec.Init(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[0] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[2])
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[1] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[1])
	}
}

func TestTimeArrayEncodeAll_Uncompressed(t *testing.T) {
	src := []int64{time.Unix(0, 0).UnixNano(), time.Unix(1, 0).UnixNano()}

	// about 36.5yrs in NS resolution is max range for compressed format
	// This should cause the encoding to fallback to raw points
	src = append(src, time.Unix(2, (2<<59)).UnixNano())
	exp := make([]int64, len(src))
	copy(exp, src)

	b, err := TimeArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("expected error: %v", err)
	}

	if exp := 25; len(b) != exp {
		t.Fatalf("length mismatch: got %v, exp %v", len(b), exp)
	}

	if got := b[0] >> 4; got != timeUncompressed {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	var dec TimeDecoder
	dec.Init(b)
	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[0] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[0])
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[1] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[1])
	}

	if !dec.Next() {
		t.Fatalf("unexpected next value: got true, exp false")
	}

	if exp[2] != dec.Read() {
		t.Fatalf("read value mismatch: got %v, exp %v", dec.Read(), exp[2])
	}
}

func TestTimeArrayEncodeAll_RLE(t *testing.T) {
	var src []int64
	for i := 0; i < 500; i++ {
		src = append(src, int64(i))
	}
	exp := make([]int64, len(src))
	copy(exp, src)

	b, err := TimeArrayEncodeAll(src, nil)
	if exp := 12; len(b) != exp {
		t.Fatalf("length mismatch: got %v, exp %v", len(b), exp)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var dec TimeDecoder
	dec.Init(b)
	for i, v := range exp {
		if !dec.Next() {
			t.Fatalf("Next == false, expected true")
		}

		if v != dec.Read() {
			t.Fatalf("Item %d mismatch, got %v, exp %v", i, dec.Read(), v)
		}
	}

	if dec.Next() {
		t.Fatalf("unexpected extra values")
	}
}

func TestTimeArrayEncodeAll_Reverse(t *testing.T) {
	src := []int64{3, 2, 0}
	exp := make([]int64, len(src))
	copy(exp, src)

	b, err := TimeArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeUncompressed {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	var dec TimeDecoder
	dec.Init(b)
	i := 0
	for dec.Next() {
		if exp[i] != dec.Read() {
			t.Fatalf("read value %d mismatch: got %v, exp %v", i, dec.Read(), exp[i])
		}
		i++
	}
}

func TestTimeArrayEncodeAll_220SecondDelta(t *testing.T) {
	var src []int64
	now := time.Now()

	for i := 0; i < 220; i++ {
		src = append(src, now.Add(time.Duration(i*60)*time.Second).UnixNano())
	}
	exp := make([]int64, len(src))
	copy(exp, src)

	b, err := TimeArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Using RLE, should get 12 bytes
	if exp := 12; len(b) != exp {
		t.Fatalf("unexpected length: got %v, exp %v", len(b), exp)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	var dec TimeDecoder
	dec.Init(b)
	i := 0
	for dec.Next() {
		if exp[i] != dec.Read() {
			t.Fatalf("read value %d mismatch: got %v, exp %v", i, dec.Read(), exp[i])
		}
		i++
	}

	if i != len(exp) {
		t.Fatalf("Read too few values: exp %d, got %d", len(exp), i)
	}

	if dec.Next() {
		t.Fatalf("expecte Next() = false, got true")
	}
}

func TestTimeArrayEncodeAll_Quick(t *testing.T) {
	quick.Check(func(values []int64) bool {
		// Write values to encoder.

		exp := make([]int64, len(values))
		for i, v := range values {
			exp[i] = int64(v)
		}

		// Retrieve encoded bytes from encoder.
		b, err := TimeArrayEncodeAll(values, nil)
		if err != nil {
			t.Fatal(err)
		}

		// Read values out of decoder.
		got := make([]int64, 0, len(values))
		var dec TimeDecoder
		dec.Init(b)
		for dec.Next() {
			if err := dec.Error(); err != nil {
				t.Fatal(err)
			}
			got = append(got, dec.Read())
		}

		// Verify that input and output values match.
		if !reflect.DeepEqual(exp, got) {
			t.Fatalf("mismatch:\n\nexp=%+v\n\ngot=%+v\n\n", exp, got)
		}

		return true
	}, nil)
}

func TestTimeArrayEncodeAll_RLESeconds(t *testing.T) {
	src := []int64{
		1444448158000000000,
		1444448168000000000,
		1444448178000000000,
		1444448188000000000,
		1444448198000000000,
		1444448208000000000,
	}
	exp := make([]int64, len(src))
	copy(exp, src)

	b, err := TimeArrayEncodeAll(src, nil)
	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var dec TimeDecoder
	dec.Init(b)
	for i, v := range exp {
		if !dec.Next() {
			t.Fatalf("Next == false, expected true")
		}

		if v != dec.Read() {
			t.Fatalf("Item %d mismatch, got %v, exp %v", i, dec.Read(), v)
		}
	}

	if dec.Next() {
		t.Fatalf("unexpected extra values")
	}
}

func TestTimeArrayEncodeAll_Count_Uncompressed(t *testing.T) {
	src := []int64{time.Unix(0, 0).UnixNano(),
		time.Unix(1, 0).UnixNano(),
	}

	// about 36.5yrs in NS resolution is max range for compressed format
	// This should cause the encoding to fallback to raw points
	src = append(src, time.Unix(2, (2<<59)).UnixNano())
	exp := make([]int64, len(src))
	copy(exp, src)

	b, err := TimeArrayEncodeAll(src, nil)
	if got := b[0] >> 4; got != timeUncompressed {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got, exp := CountTimestamps(b), 3; got != exp {
		t.Fatalf("count mismatch: got %v, exp %v", got, exp)
	}
}

func TestTimeArrayEncodeAll_Count_RLE(t *testing.T) {
	src := []int64{
		1444448158000000000,
		1444448168000000000,
		1444448178000000000,
		1444448188000000000,
		1444448198000000000,
		1444448208000000000,
	}
	exp := make([]int64, len(src))
	copy(exp, src)

	b, err := TimeArrayEncodeAll(src, nil)
	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got, exp := CountTimestamps(b), len(exp); got != exp {
		t.Fatalf("count mismatch: got %v, exp %v", got, exp)
	}
}

func TestTimeArrayEncodeAll_Count_Simple8(t *testing.T) {
	src := []int64{0, 1, 3}

	b, err := TimeArrayEncodeAll(src, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedPackedSimple {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got, exp := CountTimestamps(b), 3; got != exp {
		t.Fatalf("count mismatch: got %v, exp %v", got, exp)
	}
}

func TestTimeArrayDecodeAll_NoValues(t *testing.T) {
	enc := NewTimeEncoder(0)
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	exp := []int64{}
	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_One(t *testing.T) {
	enc := NewTimeEncoder(1)
	exp := []int64{0}
	for _, v := range exp {
		enc.Write(v)
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedPackedSimple {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_Two(t *testing.T) {
	enc := NewTimeEncoder(2)
	exp := []int64{0, 1}
	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_Three(t *testing.T) {
	enc := NewTimeEncoder(3)
	exp := []int64{0, 1, 3}
	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedPackedSimple {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_Large_Range(t *testing.T) {
	enc := NewTimeEncoder(2)
	exp := []int64{1442369134000000000, 1442369135000000000}
	for _, v := range exp {
		enc.Write(v)
	}
	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_Uncompressed(t *testing.T) {
	enc := NewTimeEncoder(3)
	exp := []int64{
		time.Unix(0, 0).UnixNano(),
		time.Unix(1, 0).UnixNano(),
		// about 36.5yrs in NS resolution is max range for compressed format
		// This should cause the encoding to fallback to raw points
		time.Unix(2, 2<<59).UnixNano(),
	}
	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("expected error: %v", err)
	}

	if exp := 25; len(b) != exp {
		t.Fatalf("length mismatch: got %v, exp %v", len(b), exp)
	}

	if got := b[0] >> 4; got != timeUncompressed {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_RLE(t *testing.T) {
	enc := NewTimeEncoder(512)
	var exp []int64
	for i := 0; i < 500; i++ {
		exp = append(exp, int64(i))
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if exp := 12; len(b) != exp {
		t.Fatalf("length mismatch: got %v, exp %v", len(b), exp)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_Reverse(t *testing.T) {
	enc := NewTimeEncoder(3)
	exp := []int64{
		int64(3),
		int64(2),
		int64(0),
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeUncompressed {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_Negative(t *testing.T) {
	enc := NewTimeEncoder(3)
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

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := b[0] >> 4; got != timeUncompressed {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_220SecondDelta(t *testing.T) {
	enc := NewTimeEncoder(256)
	var exp []int64
	now := time.Now()
	for i := 0; i < 220; i++ {
		exp = append(exp, now.Add(time.Duration(i*60)*time.Second).UnixNano())
	}

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Using RLE, should get 12 bytes
	if exp := 12; len(b) != exp {
		t.Fatalf("unexpected length: got %v, exp %v", len(b), exp)
	}

	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected uncompressed, got %v", got)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_Quick(t *testing.T) {
	quick.Check(func(values []int64) bool {
		// Write values to encoder.
		enc := NewTimeEncoder(1024)
		exp := make([]int64, len(values))
		for i, v := range values {
			exp[i] = int64(v)
			enc.Write(exp[i])
		}

		// Retrieve encoded bytes from encoder.
		buf, err := enc.Bytes()
		if err != nil {
			t.Fatal(err)
		}

		got, err := TimeArrayDecodeAll(buf, nil)
		if err != nil {
			t.Fatalf("unexpected decode error %q", err)
		}

		if !cmp.Equal(got, exp) {
			t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
		}

		return true
	}, nil)
}

func TestTimeArrayDecodeAll_RLESeconds(t *testing.T) {
	enc := NewTimeEncoder(6)
	exp := make([]int64, 6)

	exp[0] = int64(1444448158000000000)
	exp[1] = int64(1444448168000000000)
	exp[2] = int64(1444448178000000000)
	exp[3] = int64(1444448188000000000)
	exp[4] = int64(1444448198000000000)
	exp[5] = int64(1444448208000000000)

	for _, v := range exp {
		enc.Write(v)
	}

	b, err := enc.Bytes()
	if got := b[0] >> 4; got != timeCompressedRLE {
		t.Fatalf("Wrong encoding used: expected rle, got %v", got)
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := TimeArrayDecodeAll(b, nil)
	if err != nil {
		t.Fatalf("unexpected decode error %q", err)
	}

	if !cmp.Equal(got, exp) {
		t.Fatalf("unexpected values: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func TestTimeArrayDecodeAll_Corrupt(t *testing.T) {
	cases := []string{
		"\x10\x14",         // Packed: not enough data
		"\x20\x00",         // RLE: not enough data for starting timestamp
		"\x2012345678\x90", // RLE: initial timestamp but invalid uvarint encoding
		"\x2012345678\x7f", // RLE: timestamp, RLE but invalid repeat
		"\x00123",          // Raw: data length not multiple of 8
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%q", c), func(t *testing.T) {
			got, err := TimeArrayDecodeAll([]byte(c), nil)
			if err == nil {
				t.Fatal("exp an err, got nil")
			}

			exp := []int64{}
			if !cmp.Equal(got, exp) {
				t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})
	}
}

func BenchmarkEncodeTimestamps(b *testing.B) {
	var err error
	cases := []int{10, 100, 1000}

	for _, n := range cases {
		enc := NewTimeEncoder(n)

		b.Run(fmt.Sprintf("%d_seq", n), func(b *testing.B) {
			src := make([]int64, n)
			for i := 0; i < n; i++ {
				src[i] = int64(i)
			}
			sort.Slice(src, func(i int, j int) bool { return src[i] < src[j] })

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
					if bufResult, err = TimeArrayEncodeAll(input, bufResult); err != nil {
						b.Fatal(err)
					}
					copy(input, src) // Reset input that gets modified in IntegerArrayEncodeAll
				}
			})

		})

		b.Run(fmt.Sprintf("%d_ran", n), func(b *testing.B) {
			src := make([]int64, n)
			for i := 0; i < n; i++ {
				src[i] = int64(rand.Uint64())
			}
			sort.Slice(src, func(i int, j int) bool { return src[i] < src[j] })

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
					if bufResult, err = TimeArrayEncodeAll(input, bufResult); err != nil {
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
					if bufResult, err = TimeArrayEncodeAll(input, bufResult); err != nil {
						b.Fatal(err)
					}
					copy(input, src) // Reset input that gets modified in IntegerArrayEncodeAll
				}
			})
		})
	}
}

func BenchmarkTimeArrayDecodeAllUncompressed(b *testing.B) {
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

		enc := NewTimeEncoder(size)
		for i := 0; i < size; i++ {
			enc.Write(values[rand.Int()%len(values)])
		}
		bytes, _ := enc.Bytes()

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.SetBytes(int64(len(bytes)))
			b.ReportAllocs()

			dst := make([]int64, size)
			for i := 0; i < b.N; i++ {
				dst, _ = TimeArrayDecodeAll(bytes, dst)
			}
		})
	}
}

func BenchmarkTimeArrayDecodeAllPackedSimple(b *testing.B) {
	benchmarks := []int{
		5,
		55,
		555,
		1000,
	}
	for _, size := range benchmarks {
		rand.New(rand.NewSource(int64(size * 1e3)))

		enc := NewTimeEncoder(size)
		for i := 0; i < size; i++ {
			// Small amount of randomness prevents RLE from being used
			enc.Write(int64(i*1000) + int64(rand.Intn(10)))
		}
		bytes, _ := enc.Bytes()

		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			b.SetBytes(int64(len(bytes)))
			b.ReportAllocs()

			dst := make([]int64, size)
			for i := 0; i < b.N; i++ {
				dst, _ = TimeArrayDecodeAll(bytes, dst)
			}
		})
	}
}

func BenchmarkTimeArrayDecodeAllRLE(b *testing.B) {
	benchmarks := []struct {
		n     int
		delta int64
	}{
		{5, 10},
		{55, 10},
		{555, 10},
		{1000, 10},
	}
	for _, bm := range benchmarks {
		enc := NewTimeEncoder(bm.n)
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
				dst, _ = TimeArrayDecodeAll(bytes, dst)
			}
		})
	}
}
