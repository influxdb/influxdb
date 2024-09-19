package parquet

import (
	"fmt"

	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

type valueCursor interface {
	next() (int64, interface{})
	peek() (int64, bool)
	close()
}

func newValueCursor(cursor cursors.Cursor) (valueCursor, error) {
	switch c := cursor.(type) {
	case tsdb.FloatArrayCursor:
		return &floatValueCursor{cur: c}, nil
	case tsdb.UnsignedArrayCursor:
		return &uintValueCursor{cur: c}, nil
	case tsdb.IntegerArrayCursor:
		return &intValueCursor{cur: c}, nil
	case tsdb.BooleanArrayCursor:
		return &boolValueCursor{cur: c}, nil
	case tsdb.StringArrayCursor:
		return &stringValueCursor{cur: c}, nil
	}
	return nil, fmt.Errorf("unexpected type %T", cursor)

}

type floatValueCursor struct {
	cur tsdb.FloatArrayCursor
	arr *cursors.FloatArray
	idx int
}

func (c *floatValueCursor) next() (int64, interface{}) {
	// Initialize the array on first call
	if c.arr == nil {
		c.idx = 0
		c.arr = c.cur.Next()
	}
	// Indicate no elements early
	if c.arr.Len() == 0 || c.idx >= c.arr.Len() {
		return 0, nil
	}

	defer func() { c.idx++ }()
	return c.arr.Timestamps[c.idx], c.arr.Values[c.idx]
}

func (c *floatValueCursor) peek() (int64, bool) {
	// Initialize the array on first call
	if c.arr == nil {
		c.idx = 0
		c.arr = c.cur.Next()
	}
	// Indicate no elements early
	if c.arr.Len() == 0 || c.idx >= c.arr.Len() {
		return 0, false
	}
	return c.arr.Timestamps[c.idx], true
}

func (c *floatValueCursor) close() {
	c.cur.Close()
}

type uintValueCursor struct {
	cur tsdb.UnsignedArrayCursor
	arr *cursors.UnsignedArray
	idx int
}

func (c *uintValueCursor) next() (int64, interface{}) {
	// Initialize the array on first call
	if c.arr == nil {
		c.arr = c.cur.Next()
	}
	// Indicate no elements early
	if c.arr.Len() == 0 || c.idx >= c.arr.Len() {
		return 0, nil
	}
	defer func() { c.idx++ }()
	return c.arr.Timestamps[c.idx], c.arr.Values[c.idx]
}

func (c *uintValueCursor) peek() (int64, bool) {
	// Initialize the array on first call
	if c.arr == nil {
		c.idx = 0
		c.arr = c.cur.Next()
	}
	// Indicate no elements early
	if c.arr.Len() == 0 || c.idx >= c.arr.Len() {
		return 0, false
	}
	return c.arr.Timestamps[c.idx], true
}

func (c *uintValueCursor) close() {
	c.cur.Close()
}

type intValueCursor struct {
	cur tsdb.IntegerArrayCursor
	arr *cursors.IntegerArray
	idx int
}

func (c *intValueCursor) next() (int64, interface{}) {
	// Initialize the array on first call
	if c.arr == nil {
		c.arr = c.cur.Next()
	}
	// Indicate no elements early
	if c.arr.Len() == 0 || c.idx >= c.arr.Len() {
		return 0, nil
	}
	defer func() { c.idx++ }()
	return c.arr.Timestamps[c.idx], c.arr.Values[c.idx]
}

func (c *intValueCursor) peek() (int64, bool) {
	// Initialize the array on first call
	if c.arr == nil {
		c.idx = 0
		c.arr = c.cur.Next()
	}
	// Indicate no elements early
	if c.arr.Len() == 0 || c.idx >= c.arr.Len() {
		return 0, false
	}
	return c.arr.Timestamps[c.idx], true
}

func (c *intValueCursor) close() {
	c.cur.Close()
}

type boolValueCursor struct {
	cur tsdb.BooleanArrayCursor
	arr *cursors.BooleanArray
	idx int
}

func (c *boolValueCursor) next() (int64, interface{}) {
	// Initialize the array on first call
	if c.arr == nil {
		c.arr = c.cur.Next()
	}
	// Indicate no elements early
	if c.arr.Len() == 0 || c.idx >= c.arr.Len() {
		return 0, nil
	}
	defer func() { c.idx++ }()
	return c.arr.Timestamps[c.idx], c.arr.Values[c.idx]
}

func (c *boolValueCursor) peek() (int64, bool) {
	// Initialize the array on first call
	if c.arr == nil {
		c.idx = 0
		c.arr = c.cur.Next()
	}
	// Indicate no elements early
	if c.arr.Len() == 0 || c.idx >= c.arr.Len() {
		return 0, false
	}
	return c.arr.Timestamps[c.idx], true
}

func (c *boolValueCursor) close() {
	c.cur.Close()
}

type stringValueCursor struct {
	cur tsdb.StringArrayCursor
	arr *cursors.StringArray
	idx int
}

func (c *stringValueCursor) next() (int64, interface{}) {
	// Initialize the array on first call
	if c.arr == nil {
		c.arr = c.cur.Next()
	}
	// Indicate no elements early
	if c.arr.Len() == 0 || c.idx >= c.arr.Len() {
		return 0, nil
	}
	defer func() { c.idx++ }()
	return c.arr.Timestamps[c.idx], c.arr.Values[c.idx]
}

func (c *stringValueCursor) peek() (int64, bool) {
	// Initialize the array on first call
	if c.arr == nil {
		c.idx = 0
		c.arr = c.cur.Next()
	}
	// Indicate no elements early
	if c.arr.Len() == 0 || c.idx >= c.arr.Len() {
		return 0, false
	}
	return c.arr.Timestamps[c.idx], true
}

func (c *stringValueCursor) close() {
	c.cur.Close()
}
