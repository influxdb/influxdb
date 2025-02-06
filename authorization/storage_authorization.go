package authorization

import (
	"context"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"slices"

	"github.com/buger/jsonparser"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kv"
	jsonp "github.com/influxdata/influxdb/v2/pkg/jsonparser"
)

var (
	ErrHashedTokenMismatch = goerrors.New("HashedToken does not match Token")
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

func (s *Store) encodeAuthorization(a *influxdb.Authorization) ([]byte, error) {
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

	// Redact Token, if needed. This is done at the lowest level so it is impossible to serialize
	// raw tokens if hashing is enabled.
	if s.useHashedTokens {
		// Redact a copy, not the original. The raw Token value is still needed by the caller in some cases.
		redactedAuth := *a
		redactedAuth.Token = ""
		a = &redactedAuth
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

// verifyTokensMatch returns an error if a.Token and a.HashedToken are set
// but do not match.
func (s *Store) verifyTokensMatch(a *influxdb.Authorization) error {
	if a.Token == "" || a.HashedToken == "" {
		return nil
	}

	// If both Token and HashedToken are set, make sure they are equivalent before continuing.
	match, err := s.hasher.Match(a.HashedToken, a.Token)
	if err != nil {
		return fmt.Errorf("error matching tokens: %w", err)
	}
	if !match {
		return ErrHashedTokenMismatch
	}
	return nil
}

// hashToken hashes a.Token to a.HashedToken, if needed.
func (s *Store) hashToken(a *influxdb.Authorization) error {
	if !s.useHashedTokens || a.HashedToken != "" || a.Token == "" {
		// Either we're not using token hashing, the token has already been hashed,
		// or there's no token to be hashed.
		return nil
	}

	// Hash the token. Redaction of the hashed token takes place when the record is written.
	hashedToken, err := s.hasher.Hash(a.Token)
	if err != nil {
		return fmt.Errorf("error hashing token: %w", err)
	}
	a.HashedToken = hashedToken

	return nil
}

// CreateAuthorization takes an Authorization object and saves it in storage using its token
// using its token property as an index
func (s *Store) CreateAuthorization(ctx context.Context, tx kv.Tx, a *influxdb.Authorization) (retErr error) {
	defer func() {
		retErr = errors.ErrInternalServiceError(retErr, errors.WithErrorOp(influxdb.OpCreateAuthorization))
	}()
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

	return s.commitAuthorization(ctx, tx, a)
}

// GetAuthorization gets an authorization by its ID from the auth bucket in kv
func (s *Store) GetAuthorizationByID(ctx context.Context, tx kv.Tx, id platform.ID) (auth *influxdb.Authorization, retErr error) {
	defer func() {
		retErr = errors.ErrInternalServiceError(retErr, errors.WithErrorOp(influxdb.OpFindAuthorizationByID))
	}()
	encodedID, err := id.Encode()
	if err != nil {
		return nil, ErrInvalidAuthID
	}

	b, err := tx.Bucket(authBucket)
	if err != nil {
		return nil, err
	}

	v, err := b.Get(encodedID)
	if kv.IsNotFound(err) {
		return nil, ErrAuthNotFound
	}

	if err != nil {
		return nil, err
	}

	a := &influxdb.Authorization{}
	if err := decodeAuthorization(v, a); err != nil {
		return nil, err
	}

	return a, nil
}

// GetAuthorizationsByToken searches for an authorization by its raw (unhashed) token value. It will also search
// for entires with equivalent hashed tokens if the raw token is not directly found.
func (s *Store) GetAuthorizationByToken(ctx context.Context, tx kv.Tx, token string) (auth *influxdb.Authorization, retErr error) {
	defer func() {
		retErr = errors.ErrInternalServiceError(retErr, errors.WithErrorOp(influxdb.OpFindAuthorizationByToken))
	}()
	idx, err := authIndexBucket(tx)
	if err != nil {
		return nil, err
	}

	// use the token to look up the authorization's ID
	idKey, err := idx.Get(authIndexKey(token))
	if kv.IsNotFound(err) {
		authNotFoundErr := &errors.Error{
			Code: errors.ENotFound,
			Msg:  "authorization not found",
		}

		// Look for hashed token in hashed index. We have to do this even if hashed token storage is
		// currently turned off, because it may have been enabled previously, which means the token
		// could still be indexed by the hash.
		hashIdx, err := hashedAuthIndexBucket(tx)
		if err != nil {
			if s.ignoreMissingHashIndex && goerrors.Is(err, kv.ErrBucketNotFound) {
				return nil, authNotFoundErr
			} else {
				return nil, err
			}
		}

		// Try to look up token in hashed index. We have to do the lookup for all potential hash variants.
		// We also have to do this even if hashed token storage is off, because we might have indexed by
		// the hash when it previously enabled.
		allHashes, err := s.hasher.AllHashes(token)
		if err != nil {
			return nil, err
		} else if len(allHashes) == 0 {
			// No hashed tokens to lookup (shouldn't happen, but just in case it does).
			return nil, authNotFoundErr
		}
		found := false // found shouldn't really be needed since we know allHashes is not empty, but it's nice for extra safety.
		for _, hashedToken := range allHashes {
			// Very important we update the existing idKey and err variables and don't create new ones here.
			idKey, err = hashIdx.Get(hashedAuthIndexKey(hashedToken))
			if err == nil {
				// We found it! Stop looking. err will be nil after loop.
				found = true
				break
			} else {
				// Keep looking if we got a not found error.
				if !kv.IsNotFound(err) {
					return nil, err
				}
			}
		}
		if !found || kv.IsNotFound(err) {
			return nil, authNotFoundErr
		} else if err != nil {
			return nil, err
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
func (s *Store) ListAuthorizations(ctx context.Context, tx kv.Tx, f influxdb.AuthorizationFilter) (auths []*influxdb.Authorization, retErr error) {
	defer func() {
		retErr = errors.ErrInternalServiceError(retErr, errors.WithErrorOp(influxdb.OpFindAuthorizations))
	}()
	var as []*influxdb.Authorization
	pred := s.authorizationsPredicateFn(f)
	filterFn := s.filterAuthorizationsFn(f)
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

// commitAuthorization performs pre-commit checks and updates to an authorization record, commits it,
// and makes sure indices point to it. It does not delete any indices. The updated authorization is
// returned on success.
func (s *Store) commitAuthorization(ctx context.Context, tx kv.Tx, a *influxdb.Authorization) error {
	if err := s.verifyTokensMatch(a); err != nil {
		return err
	}

	if err := s.hashToken(a); err != nil {
		return err
	}

	v, err := s.encodeAuthorization(a)
	if err != nil {
		return errors.ErrInternalServiceError(err, errors.WithErrorCode(errors.EInvalid))
	}

	encodedID, err := a.ID.Encode()
	if err != nil {
		return errors.ErrInternalServiceError(err, errors.WithErrorCode(errors.ENotFound))
	}

	if !s.useHashedTokens && a.Token != "" {
		idx, err := authIndexBucket(tx)
		if err != nil {
			return err
		}

		if err := idx.Put(authIndexKey(a.Token), encodedID); err != nil {
			return err
		}
	}

	if a.HashedToken != "" {
		idx, err := hashedAuthIndexBucket(tx)
		// Don't ignore a missing index here, we want an error.
		if err != nil {
			return err
		}

		if err := idx.Put(hashedAuthIndexKey(a.HashedToken), encodedID); err != nil {
			return err
		}
	}

	b, err := tx.Bucket(authBucket)
	if err != nil {
		return err
	}

	if err := b.Put(encodedID, v); err != nil {
		return err
	}

	return nil
}

// deleteIndices removes indices for the given token and hashedToken.
func (s *Store) deleteIndices(ctx context.Context, tx kv.Tx, token, hashedToken string) error {
	authIdx, err := authIndexBucket(tx)
	if err != nil {
		return err
	}

	hashedAuthIdx, err := hashedAuthIndexBucket(tx)
	// Don't ignore missing index during an update.
	if err != nil {
		return err
	}

	if token != "" {
		if err := authIdx.Delete([]byte(token)); err != nil {
			return err
		}
	}

	if hashedToken != "" {
		if err := hashedAuthIdx.Delete([]byte(hashedToken)); err != nil {
			return err
		}
	}

	return nil
}

// UpdateAuthorization updates the status and description only of an authorization
func (s *Store) UpdateAuthorization(ctx context.Context, tx kv.Tx, id platform.ID, a *influxdb.Authorization) (auth *influxdb.Authorization, retErr error) {
	defer func() {
		retErr = errors.ErrInternalServiceError(retErr, errors.WithErrorOp(influxdb.OpUpdateAuthorization))
	}()

	initialToken := a.Token
	initialHashedToken := a.HashedToken

	if err := s.commitAuthorization(ctx, tx, a); err != nil {
		return nil, err
	}

	// Delete dangling indices from old raw tokens or hashed tokens.
	var removedToken string
	if initialToken != "" && (a.Token != initialToken || s.useHashedTokens) {
		removedToken = initialToken
	}

	var removedHashedToken string
	if initialHashedToken != "" && a.HashedToken != initialHashedToken {
		removedHashedToken = initialHashedToken
	}

	if err := s.deleteIndices(ctx, tx, removedToken, removedHashedToken); err != nil {
		return nil, err
	}

	return a, nil
}

// DeleteAuthorization removes an authorization from storage
func (s *Store) DeleteAuthorization(ctx context.Context, tx kv.Tx, id platform.ID) (retErr error) {
	defer func() {
		retErr = errors.ErrInternalServiceError(retErr, errors.WithErrorOp(influxdb.OpDeleteAuthorization))
	}()
	a, err := s.GetAuthorizationByID(ctx, tx, id)
	if err != nil {
		return err
	}

	encodedID, err := id.Encode()
	if err != nil {
		return ErrInvalidAuthID
	}

	b, err := tx.Bucket(authBucket)
	if err != nil {
		return err
	}

	if err := s.deleteIndices(ctx, tx, a.Token, a.HashedToken); err != nil {
		return err
	}

	if err := b.Delete(encodedID); err != nil {
		return err
	}

	return nil
}

func (s *Store) uniqueAuthTokenByIndex(ctx context.Context, tx kv.Tx, index, key []byte) error {
	err := unique(ctx, tx, index, key)
	if err == kv.NotUniqueError {
		// by returning a generic error we are trying to hide when
		// a token is non-unique.
		return influxdb.ErrUnableToCreateToken
	}

	// otherwise, this is some sort of internal server error and we
	// should provide some debugging information.
	return err
}

func (s *Store) uniqueAuthToken(ctx context.Context, tx kv.Tx, a *influxdb.Authorization) error {
	// Check if the raw token is unique.
	if a.Token != "" {
		if err := s.uniqueAuthTokenByIndex(ctx, tx, authIndex, authIndexKey(a.Token)); err != nil {
			return err
		}
	}

	// If Token is available, check for the uniqueness of the hashed version of Token using all
	// potential hashing schemes. If HashedToken was directly given, we must also check for it.
	allHashedTokens := make([]string, 0, s.hasher.AllHashesCount()+1)
	if a.HashedToken != "" {
		allHashedTokens = append(allHashedTokens, a.HashedToken)
	}
	if a.Token != "" {
		allRawHashes, err := s.hasher.AllHashes(a.Token)
		if err != nil {
			return err
		}
		allHashedTokens = append(allHashedTokens, allRawHashes...)
	}

	for _, hashedToken := range allHashedTokens {
		if err := s.uniqueAuthTokenByIndex(ctx, tx, hashedAuthIndex, hashedAuthIndexKey(hashedToken)); err != nil {
			if !s.ignoreMissingHashIndex || !goerrors.Is(err, kv.ErrBucketNotFound) {
				return err
			}
		}
	}

	return nil
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

func (s *Store) authorizationsPredicateFn(f influxdb.AuthorizationFilter) kv.CursorPredicateFunc {
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
		token := *f.Token
		allHashes, _ := s.hasher.AllHashes(token) // on error, allHashes is empty and we'll ignore hashedToken
		return func(_, value []byte) bool {
			// it is assumed that token never has escaped string data
			if got, _, _, err := jsonparser.Get(value, "token"); err == nil {
				return string(got) == token
			}
			if len(allHashes) > 0 {
				if got, _, _, err := jsonparser.Get(value, "hashedToken"); err == nil {
					return slices.Contains(allHashes, string(got))
				}
			}
			return true
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

func (s *Store) filterAuthorizationsFn(filter influxdb.AuthorizationFilter) func(a *influxdb.Authorization) bool {
	if filter.ID != nil {
		return func(a *influxdb.Authorization) bool {
			return a.ID == *filter.ID
		}
	}

	if filter.Token != nil {
		token := *filter.Token
		// if AllHashes returns an error, allHashes will be empty and we will ignore a.HashedToken.
		allHashes, _ := s.hasher.AllHashes(token)
		return func(a *influxdb.Authorization) bool {
			if a.Token == token {
				return true
			}
			return slices.Contains(allHashes, a.HashedToken)
		}
	}

	// Filter by org and user
	if filter.OrgID != nil && filter.UserID != nil {
		return func(a *influxdb.Authorization) bool {
			return a.OrgID == *filter.OrgID && a.UserID == *filter.UserID
		}
	}

	if filter.OrgID != nil {
		return func(a *influxdb.Authorization) bool {
			return a.OrgID == *filter.OrgID
		}
	}

	if filter.UserID != nil {
		return func(a *influxdb.Authorization) bool {
			return a.UserID == *filter.UserID
		}
	}

	return func(a *influxdb.Authorization) bool { return true }
}
