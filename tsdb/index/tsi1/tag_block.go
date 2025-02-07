package tsi1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/influxdata/influxdb/v2/pkg/rhh"
	"github.com/influxdata/influxdb/v2/tsdb"
)

// TagBlockVersion is the version of the tag block.
const TagBlockVersion = 1

// Tag key flag constants.
const (
	TagKeyTombstoneFlag = 0x01
)

// Tag value flag constants.
const (
	TagValueTombstoneFlag   = 0x01
	TagValueSeriesIDSetFlag = 0x02
)

// TagBlock variable size constants.
const (
	// TagBlock key block fields.
	TagKeyNSize      = 8
	TagKeyOffsetSize = 8

	// TagBlock value block fields.
	TagValueNSize      = 8
	TagValueOffsetSize = 8
)

// TagBlock errors.
var (
	ErrUnsupportedTagBlockVersion = errors.New("unsupported tag block version")
	ErrTagBlockSizeMismatch       = errors.New("tag block size mismatch")
)

// TagBlock represents tag key/value block for a single measurement.
type TagBlock struct {
	data []byte

	valueData []byte
	keyData   []byte
	hashData  []byte

	version int // tag block version
}

// Version returns the encoding version parsed from the data.
// Only valid after UnmarshalBinary() has been successfully invoked.
func (blk *TagBlock) Version() int { return blk.version }

// UnmarshalBinary unpacks data into the tag block. Tag block is not copied so data
// should be retained and unchanged after being passed into this function.
func (blk *TagBlock) UnmarshalBinary(data []byte) error {
	// Read trailer.
	t, err := ReadTagBlockTrailer(data)
	if err != nil {
		return err
	}

	// Verify data size is correct.
	if int64(len(data)) != t.Size {
		return ErrTagBlockSizeMismatch
	}

	// Save data section.
	blk.valueData = data[t.ValueData.Offset:]
	blk.valueData = blk.valueData[:t.ValueData.Size]

	// Save key data section.
	blk.keyData = data[t.KeyData.Offset:]
	blk.keyData = blk.keyData[:t.KeyData.Size]

	// Save hash index block.
	blk.hashData = data[t.HashIndex.Offset:]
	blk.hashData = blk.hashData[:t.HashIndex.Size]

	// Save entire block.
	blk.data = data

	return nil
}

// TagKeyElem returns an element for a tag key.
// Returns an element with a nil key if not found.
func (blk *TagBlock) TagKeyElem(key []byte) TagKeyElem {
	var elem TagBlockKeyElem
	if !blk.DecodeTagKeyElem(key, &elem) {
		return nil
	}
	return &elem
}

func (blk *TagBlock) DecodeTagKeyElem(key []byte, elem *TagBlockKeyElem) bool {
	keyN := int64(binary.BigEndian.Uint64(blk.hashData[:TagKeyNSize]))
	hash := rhh.HashKey(key)
	pos := hash % keyN

	// Track current distance
	var d int64
	for {
		// Find offset of tag key.
		offset := binary.BigEndian.Uint64(blk.hashData[TagKeyNSize+(pos*TagKeyOffsetSize):])
		if offset == 0 {
			return false
		}

		// Parse into element.
		elem.unmarshal(blk.data[offset:], blk.data)

		// Return if keys match.
		if bytes.Equal(elem.key, key) {
			return true
		}

		// Check if we've exceeded the probe distance.
		if d > rhh.Dist(rhh.HashKey(elem.key), pos, keyN) {
			return false
		}

		// Move position forward.
		pos = (pos + 1) % keyN
		d++

		if d > keyN {
			return false
		}
	}
}

// TagValueElem returns an element for a tag value.
func (blk *TagBlock) TagValueElem(key, value []byte) TagValueElem {
	var valueElem TagBlockValueElem
	if !blk.DecodeTagValueElem(key, value, &valueElem) {
		return nil
	}
	return &valueElem
}

// DecodeTagValueElem returns an element for a tag value.
func (blk *TagBlock) DecodeTagValueElem(key, value []byte, valueElem *TagBlockValueElem) bool {
	// Find key element, exit if not found.
	var keyElem TagBlockKeyElem
	if !blk.DecodeTagKeyElem(key, &keyElem) {
		return false
	}

	// Slice hash index data.
	hashData := keyElem.hashIndex.buf

	valueN := int64(binary.BigEndian.Uint64(hashData[:TagValueNSize]))
	hash := rhh.HashKey(value)
	pos := hash % valueN

	// Track current distance
	var d int64
	for {
		// Find offset of tag value.
		offset := binary.BigEndian.Uint64(hashData[TagValueNSize+(pos*TagValueOffsetSize):])
		if offset == 0 {
			return false
		}

		// Parse into element.
		valueElem.unmarshal(blk.data[offset:])

		// Return if values match.
		if bytes.Equal(valueElem.value, value) {
			return true
		}

		// Check if we've exceeded the probe distance.
		max := rhh.Dist(rhh.HashKey(valueElem.value), pos, valueN)
		if d > max {
			return false
		}

		// Move position forward.
		pos = (pos + 1) % valueN
		d++

		if d > valueN {
			return false
		}
	}
}

// TagKeyIterator returns an iterator over all the keys in the block.
func (blk *TagBlock) TagKeyIterator() TagKeyIterator {
	return &tagBlockKeyIterator{
		blk:     blk,
		keyData: blk.keyData,
	}
}

// tagBlockKeyIterator represents an iterator over all keys in a TagBlock.
type tagBlockKeyIterator struct {
	blk     *TagBlock
	keyData []byte
	e       TagBlockKeyElem
}

// Next returns the next element in the iterator.
func (itr *tagBlockKeyIterator) Next() TagKeyElem {
	// Exit when there is no data left.
	if len(itr.keyData) == 0 {
		return nil
	}

	// Unmarshal next element & move data forward.
	itr.e.unmarshal(itr.keyData, itr.blk.data)
	itr.keyData = itr.keyData[itr.e.size:]

	assert(len(itr.e.Key()) > 0, "invalid zero-length tag key")
	return &itr.e
}

// tagBlockValueIterator represents an iterator over all values for a tag key.
type tagBlockValueIterator struct {
	data []byte
	e    TagBlockValueElem
}

// Next returns the next element in the iterator.
func (itr *tagBlockValueIterator) Next() TagValueElem {
	// Exit when there is no data left.
	if len(itr.data) == 0 {
		return nil
	}

	// Unmarshal next element & move data forward.
	itr.e.unmarshal(itr.data)
	itr.data = itr.data[itr.e.size:]

	assert(len(itr.e.Value()) > 0, "invalid zero-length tag value")
	return &itr.e
}

// TagBlockKeyElem represents a tag key element in a TagBlock.
type TagBlockKeyElem struct {
	flag byte
	key  []byte

	// Value data
	data struct {
		offset uint64
		size   uint64
		buf    []byte
	}

	// Value hash index data
	hashIndex struct {
		offset uint64
		size   uint64
		buf    []byte
	}

	size int
}

// Deleted returns true if the key has been tombstoned.
func (e *TagBlockKeyElem) Deleted() bool { return (e.flag & TagKeyTombstoneFlag) != 0 }

// Key returns the key name of the element.
func (e *TagBlockKeyElem) Key() []byte { return e.key }

// TagValueIterator returns an iterator over the key's values.
func (e *TagBlockKeyElem) TagValueIterator() TagValueIterator {
	return &tagBlockValueIterator{data: e.data.buf}
}

// unmarshal unmarshals buf into e.
// The data argument represents the entire block data.
func (e *TagBlockKeyElem) unmarshal(buf, data []byte) {
	start := len(buf)

	// Parse flag data.
	e.flag, buf = buf[0], buf[1:]

	// Parse data offset/size.
	e.data.offset, buf = binary.BigEndian.Uint64(buf), buf[8:]
	e.data.size, buf = binary.BigEndian.Uint64(buf), buf[8:]

	// Slice data.
	e.data.buf = data[e.data.offset:]
	e.data.buf = e.data.buf[:e.data.size]

	// Parse hash index offset/size.
	e.hashIndex.offset, buf = binary.BigEndian.Uint64(buf), buf[8:]
	e.hashIndex.size, buf = binary.BigEndian.Uint64(buf), buf[8:]

	// Slice hash index data.
	e.hashIndex.buf = data[e.hashIndex.offset:]
	e.hashIndex.buf = e.hashIndex.buf[:e.hashIndex.size]

	// Parse key.
	n, sz := binary.Uvarint(buf)
	e.key, buf = buf[sz:sz+int(n)], buf[int(n)+sz:]

	// Save length of elem.
	e.size = start - len(buf)
}

// TagBlockValueElem represents a tag value element.
type TagBlockValueElem struct {
	flag  byte
	value []byte

	// Legacy uvarint-encoded series data.
	// Mutually exclusive with seriesIDSetData field.
	series struct {
		n    uint64 // Series count
		data []byte // Raw series data
	}

	// Roaring bitmap encoded series data.
	// Mutually exclusive with series.data field.
	seriesIDSetData []byte

	size int
}

// Deleted returns true if the element has been tombstoned.
func (e *TagBlockValueElem) Deleted() bool { return (e.flag & TagValueTombstoneFlag) != 0 }

// Value returns the value for the element.
func (e *TagBlockValueElem) Value() []byte { return e.value }

// SeriesN returns the series count.
func (e *TagBlockValueElem) SeriesN() uint64 { return e.series.n }

// SeriesData returns the raw series data.
func (e *TagBlockValueElem) SeriesData() []byte { return e.series.data }

// SeriesID returns series ID at an index.
func (e *TagBlockValueElem) SeriesID(i int) uint64 {
	return binary.BigEndian.Uint64(e.series.data[i*SeriesIDSize:])
}

// SeriesIDs returns a list decoded series ids.
func (e *TagBlockValueElem) SeriesIDs() ([]uint64, error) {
	if e.seriesIDSetData != nil {
		ss, err := e.SeriesIDSet()
		if err != nil {
			return nil, err
		}
		return ss.Slice(), nil
	}

	a := make([]uint64, 0, e.series.n)
	var prev uint64
	for data := e.series.data; len(data) > 0; {
		delta, n, err := uvarint(data)
		if err != nil {
			return nil, err
		}
		data = data[n:]

		seriesID := prev + uint64(delta)
		a = append(a, seriesID)
		prev = seriesID
	}
	return a, nil
}

// SeriesIDSet returns a set of series ids.
func (e *TagBlockValueElem) SeriesIDSet() (*tsdb.SeriesIDSet, error) {
	ss := tsdb.NewSeriesIDSet()

	// Read bitmap data directly from mmap, if available.
	if e.seriesIDSetData != nil {
		if err := ss.UnmarshalBinaryUnsafe(e.seriesIDSetData); err != nil {
			return nil, err
		}
		return ss, nil
	}

	// Otherwise decode series ids from uvarint encoding.
	var prev uint64
	for data := e.series.data; len(data) > 0; {
		delta, n, err := uvarint(data)
		if err != nil {
			return nil, err
		}
		data = data[n:]

		seriesID := prev + uint64(delta)
		ss.AddNoLock(seriesID)
		prev = seriesID
	}
	return ss, nil
}

// Size returns the size of the element.
func (e *TagBlockValueElem) Size() int { return e.size }

// unmarshal unmarshals buf into e.
func (e *TagBlockValueElem) unmarshal(buf []byte) {
	start := len(buf)

	// Parse flag data.
	e.flag, buf = buf[0], buf[1:]

	// Parse value.
	sz, n := binary.Uvarint(buf)
	e.value, buf = buf[n:n+int(sz)], buf[n+int(sz):]

	// Parse series count.
	v, n := binary.Uvarint(buf)
	e.series.n = uint64(v)
	buf = buf[n:]

	// Parse data block size.
	sz, n = binary.Uvarint(buf)
	buf = buf[n:]

	// Parse series data (original uvarint encoded or roaring bitmap).
	if e.flag&TagValueSeriesIDSetFlag == 0 {
		e.series.data, buf = buf[:sz], buf[sz:]
	} else {
		// buf = memalign(buf)
		e.seriesIDSetData, buf = buf, buf[sz:]
	}

	// Save length of elem.
	e.size = start - len(buf)
}

// TagBlockTrailerSize is the total size of the on-disk trailer.
const TagBlockTrailerSize = 0 +
	8 + 8 + // value data offset/size
	8 + 8 + // key data offset/size
	8 + 8 + // hash index offset/size
	8 + // size
	2 // version

// TagBlockTrailer represents meta data at the end of a TagBlock.
type TagBlockTrailer struct {
	Version int   // Encoding version
	Size    int64 // Total size w/ trailer

	// Offset & size of value data section.
	ValueData struct {
		Offset int64
		Size   int64
	}

	// Offset & size of key data section.
	KeyData struct {
		Offset int64
		Size   int64
	}

	// Offset & size of hash map section.
	HashIndex struct {
		Offset int64
		Size   int64
	}
}

// WriteTo writes the trailer to w.
func (t *TagBlockTrailer) WriteTo(w io.Writer) (n int64, err error) {
	// Write data info.
	if err := writeUint64To(w, uint64(t.ValueData.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.ValueData.Size), &n); err != nil {
		return n, err
	}

	// Write key data info.
	if err := writeUint64To(w, uint64(t.KeyData.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.KeyData.Size), &n); err != nil {
		return n, err
	}

	// Write hash index info.
	if err := writeUint64To(w, uint64(t.HashIndex.Offset), &n); err != nil {
		return n, err
	} else if err := writeUint64To(w, uint64(t.HashIndex.Size), &n); err != nil {
		return n, err
	}

	// Write total size & encoding version.
	if err := writeUint64To(w, uint64(t.Size), &n); err != nil {
		return n, err
	} else if err := writeUint16To(w, TagBlockVersion, &n); err != nil {
		return n, err
	}

	return n, nil
}

// ReadTagBlockTrailer returns the tag block trailer from data.
func ReadTagBlockTrailer(data []byte) (TagBlockTrailer, error) {
	var t TagBlockTrailer

	// Read version.
	t.Version = int(binary.BigEndian.Uint16(data[len(data)-2:]))
	if t.Version != TagBlockVersion {
		return t, ErrUnsupportedTagBlockVersion
	}

	// Slice trailer data.
	buf := data[len(data)-TagBlockTrailerSize:]

	// Read data section info.
	t.ValueData.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.ValueData.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read key section info.
	t.KeyData.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.KeyData.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read hash section info.
	t.HashIndex.Offset, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]
	t.HashIndex.Size, buf = int64(binary.BigEndian.Uint64(buf[0:8])), buf[8:]

	// Read total size.
	t.Size = int64(binary.BigEndian.Uint64(buf[0:8]))

	return t, nil
}

// TagBlockEncoder encodes a tags to a TagBlock section.
type TagBlockEncoder struct {
	w   io.Writer
	buf bytes.Buffer

	// Track value offsets.
	offsets *rhh.HashMap

	// Track bytes written, sections.
	n       int64
	trailer TagBlockTrailer

	// Track tag keys.
	keys      []tagKeyEncodeEntry
	prevValue []byte
}

// NewTagBlockEncoder returns a new TagBlockEncoder.
func NewTagBlockEncoder(w io.Writer) *TagBlockEncoder {
	return &TagBlockEncoder{
		w:       w,
		offsets: rhh.NewHashMap(rhh.Options{LoadFactor: LoadFactor}),
		trailer: TagBlockTrailer{
			Version: TagBlockVersion,
		},
	}
}

// N returns the number of bytes written.
func (enc *TagBlockEncoder) N() int64 { return enc.n }

// EncodeKey writes a tag key to the underlying writer.
func (enc *TagBlockEncoder) EncodeKey(key []byte, deleted bool) error {
	// An initial empty byte must be written.
	if err := enc.ensureHeaderWritten(); err != nil {
		return err
	}

	// Verify key is lexicographically after previous key.
	if len(enc.keys) > 0 {
		prev := enc.keys[len(enc.keys)-1].key
		if cmp := bytes.Compare(prev, key); cmp == 1 {
			return fmt.Errorf("tag key out of order: prev=%s, new=%s", prev, key)
		} else if cmp == 0 {
			return fmt.Errorf("tag key already encoded: %s", key)
		}
	}

	// Flush values section for key.
	if err := enc.flushValueHashIndex(); err != nil {
		return err
	}

	// Append key on to the end of the key list.
	entry := tagKeyEncodeEntry{
		key:     key,
		deleted: deleted,
	}
	entry.data.offset = enc.n

	enc.keys = append(enc.keys, entry)

	// Clear previous value.
	enc.prevValue = nil

	return nil
}

// EncodeValue writes a tag value to the underlying writer.
// The tag key must be lexicographical sorted after the previous encoded tag key.
func (enc *TagBlockEncoder) EncodeValue(value []byte, deleted bool, ss *tsdb.SeriesIDSet) error {
	if len(enc.keys) == 0 {
		return fmt.Errorf("tag key must be encoded before encoding values")
	} else if len(value) == 0 {
		return fmt.Errorf("zero length tag value not allowed")
	}

	// Validate that keys are in-order.
	if cmp := bytes.Compare(enc.prevValue, value); cmp == 1 {
		return fmt.Errorf("tag value out of order: prev=%s, new=%s", enc.prevValue, value)
	} else if cmp == 0 {
		return fmt.Errorf("tag value already encoded: %s", value)
	}

	// Save offset to hash map.
	enc.offsets.Put(value, enc.n)

	// Write flag.
	if err := writeUint8To(enc.w, encodeTagValueFlag(deleted), &enc.n); err != nil {
		return err
	}

	// Write value.
	if err := writeUvarintTo(enc.w, uint64(len(value)), &enc.n); err != nil {
		return err
	} else if err := writeTo(enc.w, value, &enc.n); err != nil {
		return err
	}

	// Build series data in buffer.
	enc.buf.Reset()
	if _, err := ss.WriteTo(&enc.buf); err != nil {
		return err
	}

	// Write series count.
	if err := writeUvarintTo(enc.w, uint64(ss.Cardinality()), &enc.n); err != nil {
		return err
	}

	// Write data size & buffer.
	if err := writeUvarintTo(enc.w, uint64(enc.buf.Len()), &enc.n); err != nil {
		return err
	}

	// Word align bitmap data.
	// if offset := (enc.n) % 8; offset != 0 {
	// 	if err := writeTo(enc.w, make([]byte, 8-offset), &enc.n); err != nil {
	// 		return err
	// 	}
	// }

	nn, err := enc.buf.WriteTo(enc.w)
	if enc.n += nn; err != nil {
		return err
	}

	// Save previous value.
	enc.prevValue = value

	return nil
}

// Close flushes the trailer of the encoder to the writer.
func (enc *TagBlockEncoder) Close() error {
	// Flush last value set.
	if err := enc.ensureHeaderWritten(); err != nil {
		return err
	} else if err := enc.flushValueHashIndex(); err != nil {
		return err
	}

	// Save ending position of entire data block.
	enc.trailer.ValueData.Size = enc.n - enc.trailer.ValueData.Offset

	// Write key block to point to value blocks.
	if err := enc.encodeTagKeyBlock(); err != nil {
		return err
	}

	// Compute total size w/ trailer.
	enc.trailer.Size = enc.n + TagBlockTrailerSize

	// Write trailer.
	nn, err := enc.trailer.WriteTo(enc.w)
	enc.n += nn
	return err
}

// ensureHeaderWritten writes a single byte to offset the rest of the block.
func (enc *TagBlockEncoder) ensureHeaderWritten() error {
	if enc.n > 0 {
		return nil
	} else if _, err := enc.w.Write([]byte{0}); err != nil {
		return err
	}

	enc.n++
	enc.trailer.ValueData.Offset = enc.n

	return nil
}

// flushValueHashIndex builds writes the hash map at the end of a value set.
func (enc *TagBlockEncoder) flushValueHashIndex() error {
	// Ignore if no keys have been written.
	if len(enc.keys) == 0 {
		return nil
	}
	key := &enc.keys[len(enc.keys)-1]

	// Save size of data section.
	key.data.size = enc.n - key.data.offset

	// Encode hash map length.
	key.hashIndex.offset = enc.n
	if err := writeUint64To(enc.w, uint64(enc.offsets.Cap()), &enc.n); err != nil {
		return err
	}

	// Encode hash map offset entries.
	for i := int64(0); i < enc.offsets.Cap(); i++ {
		_, v := enc.offsets.Elem(i)
		offset, _ := v.(int64)
		if err := writeUint64To(enc.w, uint64(offset), &enc.n); err != nil {
			return err
		}
	}
	key.hashIndex.size = enc.n - key.hashIndex.offset

	// Clear offsets.
	enc.offsets = rhh.NewHashMap(rhh.Options{LoadFactor: LoadFactor})

	return nil
}

// encodeTagKeyBlock encodes the keys section to the writer.
func (enc *TagBlockEncoder) encodeTagKeyBlock() error {
	offsets := rhh.NewHashMap(rhh.Options{Capacity: int64(len(enc.keys)), LoadFactor: LoadFactor})

	// Encode key list in sorted order.
	enc.trailer.KeyData.Offset = enc.n
	for i := range enc.keys {
		entry := &enc.keys[i]

		// Save current offset so we can use it in the hash index.
		offsets.Put(entry.key, enc.n)

		if err := writeUint8To(enc.w, encodeTagKeyFlag(entry.deleted), &enc.n); err != nil {
			return err
		}

		// Write value data offset & size.
		if err := writeUint64To(enc.w, uint64(entry.data.offset), &enc.n); err != nil {
			return err
		} else if err := writeUint64To(enc.w, uint64(entry.data.size), &enc.n); err != nil {
			return err
		}

		// Write value hash index offset & size.
		if err := writeUint64To(enc.w, uint64(entry.hashIndex.offset), &enc.n); err != nil {
			return err
		} else if err := writeUint64To(enc.w, uint64(entry.hashIndex.size), &enc.n); err != nil {
			return err
		}

		// Write key length and data.
		if err := writeUvarintTo(enc.w, uint64(len(entry.key)), &enc.n); err != nil {
			return err
		} else if err := writeTo(enc.w, entry.key, &enc.n); err != nil {
			return err
		}
	}
	enc.trailer.KeyData.Size = enc.n - enc.trailer.KeyData.Offset

	// Encode hash map length.
	enc.trailer.HashIndex.Offset = enc.n
	if err := writeUint64To(enc.w, uint64(offsets.Cap()), &enc.n); err != nil {
		return err
	}

	// Encode hash map offset entries.
	for i := int64(0); i < offsets.Cap(); i++ {
		_, v := offsets.Elem(i)
		offset, _ := v.(int64)
		if err := writeUint64To(enc.w, uint64(offset), &enc.n); err != nil {
			return err
		}
	}
	enc.trailer.HashIndex.Size = enc.n - enc.trailer.HashIndex.Offset

	return nil
}

type tagKeyEncodeEntry struct {
	key     []byte
	deleted bool

	data struct {
		offset int64
		size   int64
	}
	hashIndex struct {
		offset int64
		size   int64
	}
}

func encodeTagKeyFlag(deleted bool) byte {
	var flag byte
	if deleted {
		flag |= TagKeyTombstoneFlag
	}
	return flag
}

func encodeTagValueFlag(deleted bool) byte {
	flag := byte(TagValueSeriesIDSetFlag)
	if deleted {
		flag |= TagValueTombstoneFlag
	}
	return flag
}
