package authorization

import (
	"context"
	goerrors "errors"
	"fmt"
	"slices"

	"github.com/go-crypt/crypt"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/kv"
	influxdb2_algo "github.com/influxdata/influxdb/v2/pkg/crypt/algorithm/influxdb2"
	"github.com/influxdata/influxdb/v2/snowflake"
)

/*---
Token storage and verification

Storage of hashed tokens has been added as an optional feature. This stores only the hash of a token
in BoltDB. Token hashing is enabled with the `--use-hashed-tokens` option.

Upgrading the BoltDB schema is automatic on startup when using a new version of InfluxDB with token hashing support.
Additionally, raw tokens are automatically migrated to hashed tokens if `--use-hashed-tokens` is configured.
Due to the schema changes, to use a version of InfluxDB without hashed token support, a manual downgrade using
`influxd downgrade` must be run. Any tokens stored as hashed tokens will be unusable by the old version of InfluxDB
and must be reset or recreated.

The implementation has the following behaviors under different scenarios:
* Token hashing is enabled.
  * On startup and upgrade, any raw tokens in BoltDB are automatically hashed. The hashed token is
    stored and the raw token is removed. No raw tokens remain in BoltDB.
  * On downgrade, hashed tokens are deleted from BoltDB along with their indices. User confirmation
    is required to proceed if hashed tokens are present.
  * Token verification is performed by hashing the token and finding the hashed token in BoltDB.
  * New tokens generated are stored only as hashes.
  * When creating a backup, only hashed tokens are exported.
  * When restoring a backup, any raw tokens are converted to hashed tokens and stored only as hashes.
    No raw tokens are imported into BoltDB.
  * When listing tokens, the hash algorithm used (e.g. `SHA-512`) is returned instead of the hashed token value.
* Token hashing is disabled.
  * On upgrade and startup, no user visible action is taken. Any hashed tokens in BoltDB remain unchanged.
    The BoltDB store is updated to support hashed tokens, but no existing tokens are migrated.
  * On downgrade, hashed tokens are deleted from BoltDB along with their indices. User confirmation
    is required to proceed if hashed tokens are present.
  * Token verification is performed by looking up the raw token value, if the provided token is not in PHC format.
    If the raw token value is not found, then the hashed token value is calculated and token lookup attempted again.
  * New tokens generated are stored as raw tokens.
  * When creating a backup, tokens are exported in the format found in BoltDB (raw or hashed).
  * When restoring a backup, both raw tokens and hashed tokens are restored unchanged.
  * When listing tokens, raw tokens in BoltDB are returned. Hashed tokens in BoltDB are returned as
    the hash algorithm (e.g. `SHA-512``) instead of the hashed token value.
* Downgrading to an older InfluxDB without hashed token support with a BoltDB containing hashed tokens.
  * Downgrading requires a manual `influxd downgrade` command. If hashed tokens are found in the
    BoltDB, user confirmation is required. The user can also list impacted tokens. When downgrade is
	complete, all hashed tokens have been deleted from BoltDB along with their indices. The tokens with
	deleted hashes are no longer useable.
  * Operations are as usual in InfluxDB 2.7.

The hashed tokens in `Authorization.HashedToken` are stored in PHC format. PHC allows specifying the
algorithm used and any parameters. This allows gradual token algorithm transitions if new token hashing
algorithms are added in the future. PHC is used over MCF (Modular Crypt Format) because PHC is more
flexible and MCF does not support our chosen hash scheme.

When token hashing is enabled, on every startup (not just upgrade) InfluxDB scans the BoltDB for raw tokens in
the `Authorization.Token` field. When found, a hash is immediately stored in the `Authorization.HashedToken`
field and the `Authorization.Token` field is cleared. The token index is also updated to use the hashed
token value instead of the raw token value. This migration must occur on every startup and not just upgrades
because hashed tokens can be turned on and off with configuration.

When a backup is made, the format stored in BoltDB is exported as-is. If hashed tokens are enabled, only hashed
tokens are exported since only hashed tokens are stored. Without enabling hashed tokens, a mix of raw and
hashed tokens may be present in the backup.

When token hashing is enabled and a backup is restored, raw tokens are hashed before importing
into BoltDB. Raw tokens are not stored.

To verify tokens when hashed tokens are enabled, the presented token's hash is calculated and used
for token index lookup. The rest of the authorization flow is unchanged.

To verify tokens when hashed tokens are disabled, the an attempt is made to parse the presented token as
PHC. If the parse succeeds, the access is denied. This prevents an attack described below. After this check,
the presented raw token is used to lookup the token in the raw token index. If found, authorization proceeds
as normal. Otherwise, the token hash is calculated and used to lookup the token in the hashed token index.
A second check is then done on the authorization record token or token hash matches the presented token.
If found, authorization proceeds as normal.

The hashed token index is separate from the raw token index. Newer versions also verify that the token
is not a valid PHC string before starting authorization. This prevents the following attack:
1. Hashed token is extracted from BoltDB.
2. Token hashing is disabled.
3. The hashed token is presented to the API, which will misinterpret it as a raw token and allow access.

The token hashing algorithm is SHA-512. This provides a good level of security and is allowed by FIPS 140-2.
Because the token hashes must be useable as index lookups, salted password hashes (e.g. bcrypt, PBKDF2, Argon)
can not be used.

A potential future security would be optionally storing "peppered" hashes. This would require retrieving
the pepper key from outside of BoltDB, for example from Vault.

When listing tokens, hashed tokens are listed as "REDACTED" instead of the hashed
token value. Raw token values are returned as in previous versions.

---*/

const MaxIDGenerationN = 100
const ReservedIDs = 1000

var (
	ErrReadOnly = goerrors.New("authorization store is read-only")
)

var (
	authBucket      = []byte("authorizationsv1")
	authIndex       = []byte("authorizationindexv1")
	hashedAuthIndex = []byte("authorizationhashedindexv1")
)

type Store struct {
	kvStore kv.Store
	IDGen   platform.IDGenerator
	hasher  *AuthorizationHasher

	// Indicates if tokens should be stored in hashed PHC format.
	useHashedTokens bool

	// Indicates if Store is read-only.
	readOnly bool

	// ignoreMissingHashIndex indicates if missing hash indices in store should be ignored.
	// This is almost exclusively for testing.
	ignoreMissingHashIndex bool
}

type storePlusOptions struct {
	*Store
	hasherVariantName string
}

type StoreOption func(*storePlusOptions)

func WithAuthorizationHasher(hasher *AuthorizationHasher) StoreOption {
	return func(s *storePlusOptions) {
		s.hasher = hasher
	}
}

func WithAuthorizationHashVariantName(name string) StoreOption {
	return func(s *storePlusOptions) {
		s.hasherVariantName = name
	}
}

func WithReadOnly(readOnly bool) StoreOption {
	return func(s *storePlusOptions) {
		s.readOnly = readOnly
	}
}

func WithIgnoreMissingHashIndex(allowMissing bool) StoreOption {
	return func(s *storePlusOptions) {
		s.ignoreMissingHashIndex = allowMissing
	}
}

// NewStore creates a new authorization.Store object. kvStore is the underlying key-value store.
func NewStore(ctx context.Context, kvStore kv.Store, useHashedTokens bool, opts ...StoreOption) (*Store, error) {
	s := &storePlusOptions{
		Store: &Store{
			kvStore:         kvStore,
			IDGen:           snowflake.NewDefaultIDGenerator(),
			useHashedTokens: useHashedTokens,
		},
		hasherVariantName: DefaultHashVariantName,
	}

	for _, o := range opts {
		o(s)
	}

	if err := s.setup(ctx); err != nil {
		return nil, err
	}

	if s.hasher == nil {
		hasher, err := s.autogenerateHasher(ctx, s.hasherVariantName)
		if err != nil {
			return nil, err
		}
		s.hasher = hasher
	}

	// Perform hashed token migration if needed. This can not be performed by the migration service
	// because it requires configuration, and the migration service is more concerned with schema
	// and does not have configuration.
	if err := s.hashedTokenMigration(ctx); err != nil {
		return nil, fmt.Errorf("error during hashed token migration: %w", err)
	}

	return s.Store, nil
}

// autogenerateHasher generates an AuthorizationHasher that hashes using variantName.
// The decoders include variantName plus any other variants that are detected in the
// store.
func (s *Store) autogenerateHasher(ctx context.Context, variantName string) (*AuthorizationHasher, error) {
	// Determine which variants are present in the store.
	tempDecoder := crypt.NewDecoder()
	if err := influxdb2_algo.RegisterDecoder(tempDecoder); err != nil {
		return nil, fmt.Errorf("error registering test decoders for authorization store: %w", err)
	}

	var auths []*influxdb.Authorization
	err := s.View(ctx, func(tx kv.Tx) error {
		as, err := s.ListAuthorizations(ctx, tx, influxdb.AuthorizationFilter{})
		if err != nil {
			return err
		}
		auths = as
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error fetching authorization records for hash variant inventory: %w", err)
	}

	foundVariants := make(map[influxdb2_algo.Variant]struct{})
	for _, a := range auths {
		if a.HashedToken != "" {
			digest, err := tempDecoder.Decode(a.HashedToken)
			if err == nil {
				if influxdbDigest, ok := digest.(*influxdb2_algo.Digest); ok {
					foundVariants[influxdbDigest.Variant] = struct{}{}
				}
			}
		}
	}

	var decoderVariants []influxdb2_algo.Variant
	// Make sure we have the hasher variant we will make in there and that it is first in the list.
	hasherVariant := influxdb2_algo.NewVariant(variantName)
	decoderVariants = append(decoderVariants, hasherVariant)
	delete(foundVariants, hasherVariant)
	for variant := range foundVariants {
		decoderVariants = append(decoderVariants, variant)
	}

	hasher, err := NewAuthorizationHasher(WithHasherVariant(hasherVariant), WithDecoderVariants(decoderVariants))
	if err != nil {
		return nil, fmt.Errorf("error creating authorization hasher for authorization store: %w", err)
	}

	return hasher, nil
}

// hashedTokenMigration migrates any unhashed tokens in the store to hashed tokens.
func (s *Store) hashedTokenMigration(ctx context.Context) error {
	if !s.useHashedTokens || s.readOnly {
		return nil
	}

	// Figure out which authorization records need to be updated.
	var authsNeedingUpdate []*influxdb.Authorization
	err := s.View(ctx, func(tx kv.Tx) error {
		s.forEachAuthorization(ctx, tx, nil, func(a *influxdb.Authorization) bool {
			if a.HashedToken == "" && a.Token != "" {
				authsNeedingUpdate = append(authsNeedingUpdate, a)
			}
			return true
		})
		return nil
	})
	if err != nil {
		return err
	}

	for batch := range slices.Chunk(authsNeedingUpdate, 100) {
		err := s.Update(ctx, func(tx kv.Tx) error {
			// Now update them. This really seems too simple, but s.UpdateAuthorization() is magical.
			for _, a := range batch {
				if _, err := s.UpdateAuthorization(ctx, tx, a.ID, a); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("error migrating hashed tokens: %w", err)
		}
	}

	return nil
}

// View opens up a transaction that will not write to any data. Implementing interfaces
// should take care to ensure that all view transactions do not mutate any data.
func (s *Store) View(ctx context.Context, fn func(kv.Tx) error) error {
	return s.kvStore.View(ctx, fn)
}

// Update opens up a transaction that will mutate data.
func (s *Store) Update(ctx context.Context, fn func(kv.Tx) error) error {
	if s.readOnly {
		return ErrReadOnly
	}
	return s.kvStore.Update(ctx, fn)
}

func (s *Store) setup(ctx context.Context) error {
	return s.View(ctx, func(tx kv.Tx) error {
		if _, err := tx.Bucket(authBucket); err != nil {
			return err
		}
		if _, err := authIndexBucket(tx); err != nil {
			return err
		}
		if _, err := hashedAuthIndexBucket(tx); err != nil {
			if goerrors.Is(err, kv.ErrBucketNotFound) {
				if !s.ignoreMissingHashIndex || (s.useHashedTokens && !s.readOnly) {
					return fmt.Errorf("missing required index, upgrade required: %w", err)
				}
			} else {
				return err
			}
		}

		return nil
	})
}

// generateSafeID attempts to create ids for buckets
// and orgs that are without backslash, commas, and spaces, BUT ALSO do not already exist.
func (s *Store) generateSafeID(ctx context.Context, tx kv.Tx, bucket []byte) (platform.ID, error) {
	for i := 0; i < MaxIDGenerationN; i++ {
		id := s.IDGen.ID()

		// TODO: this is probably unnecessary but for testing we need to keep it in.
		// After KV is cleaned out we can update the tests and remove this.
		if id < ReservedIDs {
			continue
		}

		err := s.uniqueID(ctx, tx, bucket, id)
		if err == nil {
			return id, nil
		}

		if err == NotUniqueIDError {
			continue
		}

		return platform.InvalidID(), err
	}
	return platform.InvalidID(), ErrFailureGeneratingID
}

func (s *Store) uniqueID(ctx context.Context, tx kv.Tx, bucket []byte, id platform.ID) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	encodedID, err := id.Encode()
	if err != nil {
		return &errors.Error{
			Code: errors.EInvalid,
			Err:  err,
		}
	}

	b, err := tx.Bucket(bucket)
	if err != nil {
		return err
	}

	_, err = b.Get(encodedID)
	if kv.IsNotFound(err) {
		return nil
	}

	return NotUniqueIDError
}
