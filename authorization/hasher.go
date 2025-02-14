package authorization

import (
	"errors"
	"fmt"

	"github.com/go-crypt/crypt"
	"github.com/go-crypt/crypt/algorithm"
	influxdb2_algo "github.com/influxdata/influxdb/v2/pkg/crypt/algorithm/influxdb2"
)

var (
	ErrNoDecoders = errors.New("no authorization decoders specified")
)

type AuthorizationHasher struct {
	// hasher encodes tokens into hashed PHC-encoded tokens.
	hasher algorithm.Hash

	// decoder decodes hashed PHC-encoded tokens into crypt.Digest objects.
	decoder *crypt.Decoder

	// allHashers is the list of all hashers which could be used for hashed index lookup.
	allHashers []algorithm.Hash
}

const (
	DefaultHashVariant     = influxdb2_algo.VariantSHA256
	DefaultHashVariantName = influxdb2_algo.VariantIdentifierSHA256
)

type authorizationHasherOptions struct {
	hasherVariant   influxdb2_algo.Variant
	decoderVariants []influxdb2_algo.Variant
}

type AuthorizationHasherOption func(o *authorizationHasherOptions)

func WithHasherVariant(variant influxdb2_algo.Variant) AuthorizationHasherOption {
	return func(o *authorizationHasherOptions) {
		o.hasherVariant = variant
	}
}

func WithDecoderVariants(variants []influxdb2_algo.Variant) AuthorizationHasherOption {
	return func(o *authorizationHasherOptions) {
		o.decoderVariants = variants
	}
}

// NewAuthorizationHasher creates an AuthorizationHasher for influxdb2 algorithm hashed tokens.
// variantName specifies which token hashing variant to use, with blank indicating to use the default
// hashing variant. By defaults, all variants of the influxdb2 hashing scheme are supported for
// maximal compatibility.
func NewAuthorizationHasher(opts ...AuthorizationHasherOption) (*AuthorizationHasher, error) {
	options := authorizationHasherOptions{
		hasherVariant:   DefaultHashVariant,
		decoderVariants: influxdb2_algo.AllVariants,
	}

	for _, o := range opts {
		o(&options)
	}

	if len(options.decoderVariants) == 0 {
		return nil, ErrNoDecoders
	}

	// Create the hasher used for hashing new tokens before storage.
	hasher, err := influxdb2_algo.New(influxdb2_algo.WithVariant(options.hasherVariant))
	if err != nil {
		return nil, fmt.Errorf("creating hasher for AuthorizationHasher: %w", err)
	}

	// Create decoder and register all requested decoder variants.
	decoder := crypt.NewDecoder()
	for _, variant := range options.decoderVariants {
		if err := variant.RegisterDecoder(decoder); err != nil {
			return nil, fmt.Errorf("registering variant %s with decoder: %w", variant.Prefix(), err)
		}
	}

	// Create all variant hashers needed for requested decoder variants. This is for operations where all
	// potential variations of a raw token must be hashed.
	var allHashers []algorithm.Hash
	for _, variant := range options.decoderVariants {
		h, err := influxdb2_algo.New(influxdb2_algo.WithVariant(variant))
		if err != nil {
			return nil, fmt.Errorf("creating hasher %s for authorization service index lookups: %w", variant.Prefix(), err)
		}
		allHashers = append(allHashers, h)
	}

	return &AuthorizationHasher{
		hasher:     hasher,
		decoder:    decoder,
		allHashers: allHashers,
	}, nil
}

// Hash generates a PHC-encoded hash of token using the selected hash algorithm variant.
func (h *AuthorizationHasher) Hash(token string) (string, error) {
	digest, err := h.hasher.Hash(token)
	if err != nil {
		return "", fmt.Errorf("hashing raw token failed: %w", err)
	}
	return digest.Encode(), nil
}

// AllHashes generates a list of PHC-encoded hashes of token for all deterministic (i.e. non-salted) supported hashes.
func (h *AuthorizationHasher) AllHashes(token string) ([]string, error) {
	hashes := make([]string, len(h.allHashers))
	for idx, h := range h.allHashers {
		digest, err := h.Hash(token)
		if err != nil {
			variantName := "N/A"
			if influxdb_hasher, ok := h.(*influxdb2_algo.Hasher); ok {
				variantName = influxdb_hasher.Variant().Prefix()
			}
			return nil, fmt.Errorf("hashing raw token failed (variant=%s): %w", variantName, err)
		}
		hashes[idx] = digest.Encode()
	}
	return hashes, nil
}

// AllHashesCount returns the number of hash variants available through AllHashes.
func (h *AuthorizationHasher) AllHashesCount() int {
	return len(h.allHashers)
}

// Decode decodes a PHC-encoded hash into a Digest object that can be matched.
func (h *AuthorizationHasher) Decode(phc string) (algorithm.Digest, error) {
	return h.decoder.Decode(phc)
}

// Match determines if a raw token matches a PHC-encoded token.
func (h *AuthorizationHasher) Match(phc string, token string) (bool, error) {
	digest, err := h.Decode(phc)
	if err != nil {
		return false, err
	}

	return digest.MatchAdvanced(token)
}
