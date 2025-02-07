package reads

import (
	"bytes"
	"math/rand"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/models"
)

func TestKeyMerger_MergeTagKeys(t *testing.T) {
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

	var km KeyMerger
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			km.Clear()
			for _, tags := range tt.tags {
				km.MergeTagKeys(tags)
			}

			if got := km.String(); !cmp.Equal(got, tt.exp) {
				t.Errorf("unexpected keys -got/+exp\n%s", cmp.Diff(got, tt.exp))
			}
		})
	}
}

var commaB = []byte(",")

func TestKeyMerger_MergeKeys(t *testing.T) {

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

	var km KeyMerger
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			km.Clear()
			for _, keys := range tt.keys {
				km.MergeKeys(keys)
			}

			if got := km.String(); !cmp.Equal(got, tt.exp) {
				t.Errorf("unexpected keys -got/+exp\n%s", cmp.Diff(got, tt.exp))
			}
		})
	}
}

func BenchmarkKeyMerger_MergeKeys(b *testing.B) {
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

			var km KeyMerger
			for i := 0; i < b.N; i++ {
				for j := 0; j < n; j++ {
					km.MergeKeys(keys[rand.Int()%len(keys)])
				}
				km.Clear()
			}
		})
	}
}

func BenchmarkKeyMerger_MergeTagKeys(b *testing.B) {
	tags := []models.Tags{
		models.ParseTags([]byte("foo,tag00=v0,tag01=v0,tag02=v0")),
		models.ParseTags([]byte("foo,tag00=v0,tag01=v0,tag02=v0")),
		models.ParseTags([]byte("foo,tag00=v0,tag01=v0,tag05=v0,tag06=v0,tag10=v0,tag11=v0,tag12=v0,tag13=v0,tag14=v0,tag15=v0")),
		models.ParseTags([]byte("foo,tag00=v0")),
		models.ParseTags([]byte("foo,tag00=v0,tag03=v0")),
		models.ParseTags([]byte("foo,tag01=v0,tag03=v0,tag13=v0,tag14=v0,tag15=v0")),
		models.ParseTags([]byte("foo,tag04=v0,tag05=v0")),
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

			var km KeyMerger
			for i := 0; i < b.N; i++ {
				for j := 0; j < n; j++ {
					km.MergeTagKeys(tags[rand.Int()%len(tags)])
				}
				km.Clear()
			}
		})
	}
}
