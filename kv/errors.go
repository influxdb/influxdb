package kv

import (
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

// UnexpectedIndexError is used when the error comes from an internal system.
func UnexpectedIndexError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving index; Err: %v", err),
		Op:   "kv/index",
		Err:  err,
	}
}

// NotUniqueError is used when attempting to create a resource that already
// exists.
var NotUniqueError = &errors.Error{
	Code: errors.EConflict,
	Msg:  "name already exists",
}
