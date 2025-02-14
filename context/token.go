package context

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

type contextKey string

const (
	authorizerCtxKey    contextKey = "influx/authorizer/v1"
	authorizerCtxPtrKey contextKey = "influx/authorizer/pointer"
)

// SetAuthorizer sets an authorizer on context.
func SetAuthorizer(ctx context.Context, a influxdb.Authorizer) context.Context {
	return context.WithValue(ctx, authorizerCtxKey, a)
}

// GetAuthorizer retrieves an authorizer from context.
func GetAuthorizer(ctx context.Context) (influxdb.Authorizer, error) {
	a, ok := ctx.Value(authorizerCtxKey).(influxdb.Authorizer)
	if !ok {
		return nil, &errors.Error{
			Msg:  "authorizer not found on context",
			Code: errors.EInternal,
		}
	}
	if a == nil {
		return nil, &errors.Error{
			Code: errors.EInternal,
			Msg:  "unexpected invalid authorizer",
		}
	}

	return a, nil
}

// HasToken determines if a context has a token. Return is nil if token found from the context; errors if no token.
func HasToken(ctx context.Context) error {
	a, ok := ctx.Value(authorizerCtxKey).(influxdb.Authorizer)
	if !ok {
		return &errors.Error{
			Msg:  "authorizer not found on context",
			Code: errors.EInternal,
		}
	}

	_, ok = a.(*influxdb.Authorization)
	if !ok {
		return &errors.Error{
			Msg:  fmt.Sprintf("authorizer not an authorization but a %T", a),
			Code: errors.EInternal,
		}
	}

	return nil
}

// GetUserID retrieves the user ID from the authorizer on the context.
func GetUserID(ctx context.Context) (platform.ID, error) {
	a, err := GetAuthorizer(ctx)
	if err != nil {
		return 0, err
	}
	return a.GetUserID(), nil
}

// ProvideAuthorizerStorage puts a pointer to an Authorizer in the context.
// This is used to pass an Authorizer up the stack for logging purposes
func ProvideAuthorizerStorage(ctx context.Context, ap *influxdb.Authorizer) context.Context {
	return context.WithValue(ctx, authorizerCtxPtrKey, ap)
}

// StoreAuthorizer stores an Authorizer in a pointer from the Context.
// This permits functions deep in the stack to set the pointer to return
// values up the call chain
func StoreAuthorizer(ctx context.Context, auth influxdb.Authorizer) bool {
	ap, ok := ctx.Value(authorizerCtxPtrKey).(*influxdb.Authorizer)
	if ok && (ap != nil) {
		(*ap) = auth
		return true
	} else {
		return false
	}
}
