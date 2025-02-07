package models_test

import (
	"bytes"
	"math/rand"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/models"
)

func TestTagKeysSet_UnionKeys(t *testing.T) {
	tests := []struct {
		name string
		tags []models.Tags
		exp  string
	}{
		{
			name: "mixed",
			tags: []models.Tags{
				models.ParseTags([]byte("foo,tag0=v0,tag1=v0,tag2=v0")),
				models.ParseTags([]byte("foo,tag0=v0,tag1=v0,tag2=v1")),
				models.ParseTags([]byte("foo,tag0=v0")),
				models.ParseTags([]byte("foo,tag0=v0,tag3=v0")),
			},
			exp: "tag0,tag1,tag2,tag3",
		},
		{
			name: "mixed 2",
			tags: []models.Tags{
				models.ParseTags([]byte("foo,tag0=v0")),
				models.ParseTags([]byte("foo,tag0=v0,tag3=v0")),
				models.ParseTags([]byte("foo,tag0=v0,tag1=v0,tag2=v0")),
				models.ParseTags([]byte("foo,tag0=v0,tag1=v0,tag2=v1")),
			},
			exp: "tag0,tag1,tag2,tag3",
		},
		{
			name: "all different",
			tags: []models.Tags{
				models.ParseTags([]byte("foo,tag0=v0")),
				models.ParseTags([]byte("foo,tag1=v0")),
				models.ParseTags([]byte("foo,tag2=v1")),
				models.ParseTags([]byte("foo,tag3=v0")),
			},
			exp: "tag0,tag1,tag2,tag3",
		},
		{
			name: "new tags,verify clear",
			tags: []models.Tags{
				models.ParseTags([]byte("foo,tag9=v0")),
				models.ParseTags([]byte("foo,tag8=v0")),
			},
			exp: "tag8,tag9",
		},
	}

	var km models.TagKeysSet
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			km.Clear()
			for _, tags := range tt.tags {
				km.UnionKeys(tags)
			}

			if got := km.String(); !cmp.Equal(got, tt.exp) {
				t.Errorf("unexpected keys -got/+exp\n%s", cmp.Diff(got, tt.exp))
			}
		})
	}
}

func TestTagKeysSet_IsSuperset(t *testing.T) {
	var km models.TagKeysSet
	km.UnionBytes(bytes.Split([]byte("tag0,tag3,tag5,tag7"), commaB))

	tests := []struct {
		name string
		tags models.Tags
		exp  bool
	}{
		{
			tags: models.ParseTags([]byte("foo,tag0=v,tag3=v")),
			exp:  true,
		},
		{
			tags: models.ParseTags([]byte("foo,tag3=v")),
			exp:  true,
		},
		{
			tags: models.ParseTags([]byte("foo,tag7=v")),
			exp:  true,
		},
		{
			tags: models.ParseTags([]byte("foo,tag3=v,tag7=v")),
			exp:  true,
		},
		{
			tags: models.ParseTags([]byte("foo,tag0=v,tag3=v,tag5=v,tag7=v")),
			exp:  true,
		},
		{
			tags: models.ParseTags([]byte("foo")),
			exp:  true,
		},
		{
			tags: models.ParseTags([]byte("foo,tag0=v,tag2=v")),
			exp:  false,
		},
		{
			tags: models.ParseTags([]byte("foo,tag1=v")),
			exp:  false,
		},
		{
			tags: models.ParseTags([]byte("foo,tag6=v")),
			exp:  false,
		},
		{
			tags: models.ParseTags([]byte("foo,tag8=v")),
			exp:  false,
		},
		{
			tags: models.ParseTags([]byte("foo,tag0=v,tag3=v,tag5=v,tag8=v")),
			exp:  false,
		},
		{
			tags: models.ParseTags([]byte("foo,tag0=v,tag3=v,tag5=v,tag6=v")),
			exp:  false,
		},
		{
			tags: models.ParseTags([]byte("foo,tag0=v,tag3=v,tag5=v,tag7=v,tag8=v")),
			exp:  false,
		},
	}

	for _, tt := range tests {
		t.Run("tags/"+tt.name, func(t *testing.T) {
			if got := km.IsSupersetKeys(tt.tags); got != tt.exp {
				t.Errorf("unexpected IsSuperset -got/+exp\n%s", cmp.Diff(got, tt.exp))
			}
		})
	}

	for _, tt := range tests {
		t.Run("bytes/"+tt.name, func(t *testing.T) {
			var keys [][]byte
			for i := range tt.tags {
				keys = append(keys, tt.tags[i].Key)
			}
			if got := km.IsSupersetBytes(keys); got != tt.exp {
				t.Errorf("unexpected IsSupersetBytes -got/+exp\n%s", cmp.Diff(got, tt.exp))
			}
		})
	}
}

var commaB = []byte(",")

func TestTagKeysSet_UnionBytes(t *testing.T) {

	tests := []struct {
		name string
		keys [][][]byte
		exp  string
	}{
		{
			name: "mixed",
			keys: [][][]byte{
				bytes.Split([]byte("tag0,tag1,tag2"), commaB),
				bytes.Split([]byte("tag0,tag1,tag2"), commaB),
				bytes.Split([]byte("tag0"), commaB),
				bytes.Split([]byte("tag0,tag3"), commaB),
			},
			exp: "tag0,tag1,tag2,tag3",
		},
		{
			name: "mixed 2",
			keys: [][][]byte{
				bytes.Split([]byte("tag0"), commaB),
				bytes.Split([]byte("tag0,tag3"), commaB),
				bytes.Split([]byte("tag0,tag1,tag2"), commaB),
				bytes.Split([]byte("tag0,tag1,tag2"), commaB),
			},
			exp: "tag0,tag1,tag2,tag3",
		},
		{
			name: "all different",
			keys: [][][]byte{
				bytes.Split([]byte("tag0"), commaB),
				bytes.Split([]byte("tag3"), commaB),
				bytes.Split([]byte("tag1"), commaB),
				bytes.Split([]byte("tag2"), commaB),
			},
			exp: "tag0,tag1,tag2,tag3",
		},
		{
			name: "new tags,verify clear",
			keys: [][][]byte{
				bytes.Split([]byte("tag9"), commaB),
				bytes.Split([]byte("tag8"), commaB),
			},
			exp: "tag8,tag9",
		},
	}

	var km models.TagKeysSet
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			km.Clear()
			for _, keys := range tt.keys {
				km.UnionBytes(keys)
			}

			if got := km.String(); !cmp.Equal(got, tt.exp) {
				t.Errorf("unexpected keys -got/+exp\n%s", cmp.Diff(got, tt.exp))
			}
		})
	}
}

func BenchmarkTagKeysSet_UnionBytes(b *testing.B) {
	keys := [][][]byte{
		bytes.Split([]byte("tag00,tag01,tag02"), commaB),
		bytes.Split([]byte("tag00,tag01,tag02"), commaB),
		bytes.Split([]byte("tag00,tag01,tag05,tag06,tag10,tag11,tag12,tag13,tag14,tag15"), commaB),
		bytes.Split([]byte("tag00"), commaB),
		bytes.Split([]byte("tag00,tag03"), commaB),
		bytes.Split([]byte("tag01,tag03,tag13,tag14,tag15"), commaB),
		bytes.Split([]byte("tag04,tag05"), commaB),
	}

	rand.New(rand.NewSource(20040409))

	tests := []int{
		10,
		1000,
		1000000,
	}

	for _, n := range tests {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ResetTimer()

			var km models.TagKeysSet
			for i := 0; i < b.N; i++ {
				for j := 0; j < n; j++ {
					km.UnionBytes(keys[rand.Int()%len(keys)])
				}
				km.Clear()
			}
		})
	}
}

type XorShift64Star struct {
	state uint64
}

func (x *XorShift64Star) Next() uint64 {
	x.state ^= x.state >> 12
	x.state ^= x.state << 25
	x.state ^= x.state >> 27
	return x.state * 2685821657736338717
}

func BenchmarkTagKeysSet_UnionKeys(b *testing.B) {
	tags := []models.Tags{
		models.ParseTags([]byte("foo,tag00=v0,tag01=v0,tag02=v0")),
		models.ParseTags([]byte("foo,tag00=v0,tag01=v0,tag02=v0")),
		models.ParseTags([]byte("foo,tag00=v0,tag01=v0,tag05=v0,tag06=v0,tag10=v0,tag11=v0,tag12=v0,tag13=v0,tag14=v0,tag15=v0")),
		models.ParseTags([]byte("foo,tag00=v0")),
		models.ParseTags([]byte("foo,tag00=v0,tag03=v0")),
		models.ParseTags([]byte("foo,tag01=v0,tag03=v0,tag13=v0,tag14=v0,tag15=v0")),
		models.ParseTags([]byte("foo,tag04=v0,tag05=v0")),
	}

	rnd := XorShift64Star{state: 20040409}

	tests := []int{
		10,
		1000,
		1000000,
	}

	for _, n := range tests {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ResetTimer()

			var km models.TagKeysSet
			for i := 0; i < b.N; i++ {
				for j := 0; j < n; j++ {
					km.UnionKeys(tags[rnd.Next()%uint64(len(tags))])
				}
				km.Clear()
			}
		})
	}
}

func BenchmarkTagKeysSet_IsSuperset(b *testing.B) {
	var km models.TagKeysSet
	km.UnionBytes(bytes.Split([]byte("tag0,tag3,tag5,tag7"), commaB))

	tests := []struct {
		name string
		tags models.Tags
	}{
		{name: "last/true", tags: models.ParseTags([]byte("foo,tag7=v"))},
		{name: "last/false", tags: models.ParseTags([]byte("foo,tag8=v"))},
		{name: "first_last/true", tags: models.ParseTags([]byte("foo,tag0=v,tag7=v"))},
		{name: "all/true", tags: models.ParseTags([]byte("foo,tag0=v,tag3=v,tag5=v,tag7=v"))},
		{name: "first not last/false", tags: models.ParseTags([]byte("foo,tag0=v,tag8=v"))},
		{name: "all but last/false", tags: models.ParseTags([]byte("foo,tag0=v,tag3=v,tag5=v,tag7=v,tag8=v"))},
	}

	for _, n := range tests {
		b.Run(n.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				km.IsSupersetKeys(n.tags)
			}
		})
	}
}
