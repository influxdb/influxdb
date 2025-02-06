package authorization

import (
	"context"
	"encoding/json"
	errors2 "errors"

	"github.com/buger/jsonparser"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kv"
	jsonp "github.com/influxdata/influxdb/v2/pkg/jsonparser"
	"github.com/influxdata/influxdb/v2/tenant"
)

func authIndexKey(n string) []byte {
	return []byte(n)
}

func authIndexBucket(tx kv.Tx) (kv.Bucket, error) {
	b, err := tx.Bucket([]byte(authIndex))
	if err != nil {
		return nil, UnexpectedAuthIndexError(err)
	}

	return b, nil
}

func hashedAuthIndexKey(n string) []byte {
	return []byte(n)
}

func hashedAuthIndexBucket(tx kv.Tx) (kv.Bucket, error) {
	b, err := tx.Bucket([]byte(hashedAuthIndex))
	if err != nil {
		return nil, UnexpectedAuthIndexError(err)
	}

	return b, nil
}

func encodeAuthorization(a *influxdb.Authorization) ([]byte, error) {
	if len(a.Token) > 0 && len(a.HashedToken) > 0 {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "authorization can not contained Token and HashedToken",
		}
	}

	switch a.Status {
	case influxdb.Active, influxdb.Inactive:
	case "":
		a.Status = influxdb.Active
	default:
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "unknown authorization status",
		}
	}

	return json.Marshal(a)
}

func decodeAuthorization(b []byte, a *influxdb.Authorization) error {
	if err := json.Unmarshal(b, a); err != nil {
		return err
	}
	if a.Status == "" {
		a.Status = influxdb.Active
	}
	return nil
}

// CreateAuthorization takes an Authorization object and saves it in storage using its token
// using its token property as an index
func (s *Store) CreateAuthorization(ctx context.Context, tx kv.Tx, a *influxdb.Authorization) error {
	// if the provided ID is invalid, or already maps to an existing Auth, then generate a new one
	if !a.ID.Valid() {
		id, err := s.generateSafeID(ctx, tx, authBucket)
		if err != nil {
			return nil
		}
		a.ID = id
	} else if err := uniqueID(ctx, tx, a.ID); err != nil {
		id, err := s.generateSafeID(ctx, tx, authBucket)
		if err != nil {
			return nil
		}
		a.ID = id
	}

	ts := tenant.NewStore(s.kvStore)
	for _, p := range a.Permissions {
		if p.Resource.ID == nil || p.Resource.Type != influxdb.BucketsResourceType {
			continue
		}
		_, err := ts.GetBucket(ctx, tx, *p.Resource.ID)
		if errors2.Is(err, tenant.ErrBucketNotFound) {
			return ErrBucketNotFound
		}
	}

	if err := s.uniqueAuthToken(ctx, tx, a); err != nil {
		return ErrTokenAlreadyExistsError
	}

	v, err := encodeAuthorization(a)
	if err != nil {
		return &errors.Error{
			Code: errors.EInvalid,
			Err:  err,
		}
	}

	encodedID, err := a.ID.Encode()
	if err != nil {
		return ErrInvalidAuthIDError(err)
	}

	idx, err := authIndexBucket(tx)
	if err != nil {
		return err
	}

	if err := idx.Put(authIndexKey(a.Token), encodedID); err != nil {
		return &errors.Error{
			Code: errors.EInternal,
			Err:  err,
		}
	}

	b, err := tx.Bucket(authBucket)
	if err != nil {
		return err
	}

	if err := b.Put(encodedID, v); err != nil {
		return &errors.Error{
			Err: err,
		}
	}

	return nil
}

// GetAuthorization gets an authorization by its ID from the auth bucket in kv
func (s *Store) GetAuthorizationByID(ctx context.Context, tx kv.Tx, id platform.ID) (*influxdb.Authorization, error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, ErrInvalidAuthID
	}

	b, err := tx.Bucket(authBucket)
	if err != nil {
		return nil, errors.ErrInternalServiceError(err)
	}

	v, err := b.Get(encodedID)
	if kv.IsNotFound(err) {
		return nil, ErrAuthNotFound
	}

	if err != nil {
		return nil, errors.ErrInternalServiceError(err)
	}

	a := &influxdb.Authorization{}
	if err := decodeAuthorization(v, a); err != nil {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Err:  err,
		}
	}

	return a, nil
}

func (s *Store) GetAuthorizationByToken(ctx context.Context, tx kv.Tx, token string) (*influxdb.Authorization, error) {
	idx, err := authIndexBucket(tx)
	if err != nil {
		return nil, err
	}

	// use the token to look up the authorization's ID
	idKey, err := idx.Get(authIndexKey(token))
	if kv.IsNotFound(err) {
		return nil, &errors.Error{
			Code: errors.ENotFound,
			Msg:  "authorization not found",
		}
	}

	var id platform.ID
	if err := id.Decode(idKey); err != nil {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Err:  err,
		}
	}

	return s.GetAuthorizationByID(ctx, tx, id)
}

// ListAuthorizations returns all the authorizations matching a set of FindOptions. This function is used for
// FindAuthorizationByID, FindAuthorizationByToken, and FindAuthorizations in the AuthorizationService implementation
func (s *Store) ListAuthorizations(ctx context.Context, tx kv.Tx, f influxdb.AuthorizationFilter) ([]*influxdb.Authorization, error) {
	var as []*influxdb.Authorization
	pred := authorizationsPredicateFn(f)
	filterFn := filterAuthorizationsFn(f)
	err := s.forEachAuthorization(ctx, tx, pred, func(a *influxdb.Authorization) bool {
		if filterFn(a) {
			as = append(as, a)
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	return as, nil
}

// forEachAuthorization will iterate through all authorizations while fn returns true.
func (s *Store) forEachAuthorization(ctx context.Context, tx kv.Tx, pred kv.CursorPredicateFunc, fn func(*influxdb.Authorization) bool) error {
	b, err := tx.Bucket(authBucket)
	if err != nil {
		return err
	}

	var cur kv.Cursor
	if pred != nil {
		cur, err = b.Cursor(kv.WithCursorHintPredicate(pred))
	} else {
		cur, err = b.Cursor()
	}
	if err != nil {
		return err
	}

	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		// preallocate Permissions to reduce multiple slice re-allocations
		a := &influxdb.Authorization{
			Permissions: make([]influxdb.Permission, 64),
		}

		if err := decodeAuthorization(v, a); err != nil {
			return err
		}
		if !fn(a) {
			break
		}
	}

	return nil
}

// UpdateAuthorization updates the status and description only of an authorization
func (s *Store) UpdateAuthorization(ctx context.Context, tx kv.Tx, id platform.ID, a *influxdb.Authorization) (*influxdb.Authorization, error) {
	v, err := encodeAuthorization(a)
	if err != nil {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Err:  err,
		}
	}

	encodedID, err := a.ID.Encode()
	if err != nil {
		return nil, &errors.Error{
			Code: errors.ENotFound,
			Err:  err,
		}
	}

	idx, err := authIndexBucket(tx)
	if err != nil {
		return nil, err
	}

	if err := idx.Put(authIndexKey(a.Token), encodedID); err != nil {
		return nil, &errors.Error{
			Code: errors.EInternal,
			Err:  err,
		}
	}

	b, err := tx.Bucket(authBucket)
	if err != nil {
		return nil, err
	}

	if err := b.Put(encodedID, v); err != nil {
		return nil, &errors.Error{
			Err: err,
		}
	}

	return a, nil

}

// DeleteAuthorization removes an authorization from storage
func (s *Store) DeleteAuthorization(ctx context.Context, tx kv.Tx, id platform.ID) error {
	a, err := s.GetAuthorizationByID(ctx, tx, id)
	if err != nil {
		return err
	}

	encodedID, err := id.Encode()
	if err != nil {
		return ErrInvalidAuthID
	}

	idx, err := authIndexBucket(tx)
	if err != nil {
		return err
	}

	b, err := tx.Bucket(authBucket)
	if err != nil {
		return err
	}

	if err := idx.Delete([]byte(a.Token)); err != nil {
		return errors.ErrInternalServiceError(err)
	}

	if err := b.Delete(encodedID); err != nil {
		return errors.ErrInternalServiceError(err)
	}

	return nil
}

func (s *Store) uniqueAuthToken(ctx context.Context, tx kv.Tx, a *influxdb.Authorization) error {
	err := unique(ctx, tx, authIndex, authIndexKey(a.Token))
	if err == kv.NotUniqueError {
		// by returning a generic error we are trying to hide when
		// a token is non-unique.
		return influxdb.ErrUnableToCreateToken
	}
	// otherwise, this is some sort of internal server error and we
	// should provide some debugging information.
	return err
}

func unique(ctx context.Context, tx kv.Tx, indexBucket, indexKey []byte) error {
	bucket, err := tx.Bucket(indexBucket)
	if err != nil {
		return kv.UnexpectedIndexError(err)
	}

	_, err = bucket.Get(indexKey)
	// if not found then this token is unique.
	if kv.IsNotFound(err) {
		return nil
	}

	// no error means this is not unique
	if err == nil {
		return kv.NotUniqueError
	}

	// any other error is some sort of internal server error
	return kv.UnexpectedIndexError(err)
}

// uniqueID returns nil if the ID provided is unique, returns an error otherwise
func uniqueID(ctx context.Context, tx kv.Tx, id platform.ID) error {
	encodedID, err := id.Encode()
	if err != nil {
		return ErrInvalidAuthID
	}

	b, err := tx.Bucket(authBucket)
	if err != nil {
		return errors.ErrInternalServiceError(err)
	}

	_, err = b.Get(encodedID)
	// if not found then the ID is unique
	if kv.IsNotFound(err) {
		return nil
	}
	// no error means this is not unique
	if err == nil {
		return kv.NotUniqueError
	}

	// any other error is some sort of internal server error
	return kv.UnexpectedIndexError(err)
}

func authorizationsPredicateFn(f influxdb.AuthorizationFilter) kv.CursorPredicateFunc {
	// if any errors occur reading the JSON data, the predicate will always return true
	// to ensure the value is included and handled higher up.

	if f.ID != nil {
		exp := *f.ID
		return func(_, value []byte) bool {
			got, err := jsonp.GetID(value, "id")
			if err != nil {
				return true
			}
			return got == exp
		}
	}

	if f.Token != nil {
		exp := *f.Token
		return func(_, value []byte) bool {
			// it is assumed that token never has escaped string data
			got, _, _, err := jsonparser.Get(value, "token")
			if err != nil {
				return true
			}
			return string(got) == exp
		}
	}

	var pred kv.CursorPredicateFunc
	if f.OrgID != nil {
		exp := *f.OrgID
		pred = func(_, value []byte) bool {
			got, err := jsonp.GetID(value, "orgID")
			if err != nil {
				return true
			}

			return got == exp
		}
	}

	if f.UserID != nil {
		exp := *f.UserID
		prevFn := pred
		pred = func(key, value []byte) bool {
			prev := prevFn == nil || prevFn(key, value)
			got, exists, err := jsonp.GetOptionalID(value, "userID")
			return prev && ((exp == got && exists) || err != nil)
		}
	}

	return pred
}

type predicateFunc func(a *influxdb.Authorization) bool

func filterAuthorizationsFn(filter influxdb.AuthorizationFilter) predicateFunc {

	if filter.ID != nil {
		return func(a *influxdb.Authorization) bool {
			return a.ID == *filter.ID
		}
	}

	if filter.Token != nil {
		return func(a *influxdb.Authorization) bool {
			return a.Token == *filter.Token
		}
	}

	var pred predicateFunc
	if filter.OrgID != nil {
		exp := *filter.OrgID
		prevFn := pred
		pred = func(a *influxdb.Authorization) bool {
			prev := prevFn == nil || prevFn(a)
			return prev && a.OrgID == exp
		}
	}

	if filter.UserID != nil {
		exp := *filter.UserID
		prevFn := pred
		pred = func(a *influxdb.Authorization) bool {
			prev := prevFn == nil || prevFn(a)
			return prev && a.UserID == exp
		}
	}

	if pred == nil {
		pred = func(a *influxdb.Authorization) bool { return true }
	}

	return pred
}
