package kv

import (
	"context"
	"errors"
)

var (
	// ErrKeyNotFound is the error returned when the key requested is not found.
	ErrKeyNotFound = errors.New("key not found")
	// ErrTxNotWritable is the error returned when an mutable operation is called during
	// a non-writable transaction.
	ErrTxNotWritable = errors.New("transaction is not writable")
)

// IsNotFound returns a boolean indicating whether the error is known to report that a key or was not found.
func IsNotFound(err error) bool {
	return err == ErrKeyNotFound
}

// Store is an interface for a generic key value store. It is modeled after
// the boltdb database struct.
type Store interface {
	// View opens up a transaction that will not write to any data. Implementing interfaces
	// should take care to ensure that all view transactions do not mutate any data.
	View(context.Context, func(Tx) error) error
	// Update opens up a transaction that will mutate data.
	Update(context.Context, func(Tx) error) error
}

// Tx is a transaction in the store.
type Tx interface {
	// Bucket possibly creates and returns bucket, b.
	Bucket(b []byte) (Bucket, error)
	// Context returns the context associated with this Tx.
	Context() context.Context
	// WithContext associates a context with this Tx.
	WithContext(ctx context.Context)
}

type CursorPredicateFunc func(key, value []byte) bool

type CursorHints struct {
	KeyPrefix   *string
	KeyStart    *string
	PredicateFn CursorPredicateFunc
}

// CursorHint configures CursorHints
type CursorHint func(*CursorHints)

// WithCursorHintPrefix is a hint to the store
// that the caller is only interested keys with the
// specified prefix.
func WithCursorHintPrefix(prefix string) CursorHint {
	return func(o *CursorHints) {
		o.KeyPrefix = &prefix
	}
}

// WithCursorHintKeyStart is a hint to the store
// that the caller is interested in reading keys from
// start.
func WithCursorHintKeyStart(start string) CursorHint {
	return func(o *CursorHints) {
		o.KeyStart = &start
	}
}

// WithCursorHintPredicate is a hint to the store
// to return only key / values which return true for the
// f.
//
// The primary concern of the predicate is to improve performance.
// Therefore, it should perform tests on the data at minimal cost.
// If the predicate has no meaningful impact on reducing memory or
// CPU usage, there is no benefit to using it.
func WithCursorHintPredicate(f CursorPredicateFunc) CursorHint {
	return func(o *CursorHints) {
		o.PredicateFn = f
	}
}

// Bucket is the abstraction used to perform get/put/delete/get-many operations
// in a key value store.
type Bucket interface {
	// TODO context?
	// Get returns a key within this bucket. Errors if key does not exist.
	Get(key []byte) ([]byte, error)
	// Cursor returns a cursor at the beginning of this bucket optionally
	// using the provided hints to improve performance.
	Cursor(hints ...CursorHint) (Cursor, error)
	// Put should error if the transaction it was called in is not writable.
	Put(key, value []byte) error
	// Delete should error if the transaction it was called in is not writable.
	Delete(key []byte) error
	// ForwardCursor returns a forward cursor from the seek position provided.
	// Other options can be supplied to provide direction and hints.
	ForwardCursor(seek []byte, opts ...CursorOption) (ForwardCursor, error)
}

// Cursor is an abstraction for iterating/ranging through data. A concrete implementation
// of a cursor can be found in cursor.go.
type Cursor interface {
	ForwardCursor
	// Seek moves the cursor forward until reaching prefix in the key name.
	Seek(prefix []byte) (k []byte, v []byte)
	// First moves the cursor to the first key in the bucket.
	First() (k []byte, v []byte)
	// Last moves the cursor to the last key in the bucket.
	Last() (k []byte, v []byte)
	// Prev moves the cursor to the prev key in the bucket.
	Prev() (k []byte, v []byte)
}

// ForwardCursor is an abstraction for interacting/ranging through data in one direction.
type ForwardCursor interface {
	// Next moves the cursor to the next key in the bucket.
	Next() (k, v []byte)
}

// CursorDirection is an integer used to define the direction
// a request cursor operates in.
type CursorDirection int

const (
	// CursorAscending directs a cursor to range in ascending order
	CursorAscending CursorDirection = iota
	// CursorAscending directs a cursor to range in descending order
	CursorDescending
)

// CursorConfig is a type used to configure a new forward cursor.
// It includes a direction and a set of hints
type CursorConfig struct {
	Direction CursorDirection
	Hints     CursorHints
}

// NewCursorConfig constructs and configures a CursorConfig used to configure
// a forward cursor.
func NewCursorConfig(opts ...CursorOption) CursorConfig {
	conf := CursorConfig{}
	for _, opt := range opts {
		opt(&conf)
	}
	return conf
}

// CursorOption is a functional option for configuring a forward cursor
type CursorOption func(*CursorConfig)

// WithCursorDirection sets the cursor direction on a provided cursor config
func WithCursorDirection(direction CursorDirection) CursorOption {
	return func(c *CursorConfig) {
		c.Direction = direction
	}
}

// WithCursorHints configs the provided hints on the cursor config
func WithCursorHints(hints ...CursorHint) CursorOption {
	return func(c *CursorConfig) {
		for _, hint := range hints {
			hint(&c.Hints)
		}
	}
}
