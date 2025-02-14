package influxdb2

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"fmt"

	"github.com/go-crypt/crypt/algorithm"
)

// NewVariant converts an identifier string to a influxdb2.Variant.
func NewVariant(identifier string) (variant Variant) {
	switch identifier {
	case VariantIdentifierSHA256:
		return VariantSHA256
	case VariantIdentifierSHA512:
		return VariantSHA512
	default:
		return VariantNone
	}
}

// Variant is a variant of the influxdb2.Digest.
type Variant int

const (
	// VariantNone is a variant of the influxdb2.Digest which is unknown.
	VariantNone Variant = iota

	// VariantSHA256 is a variant of the influxdb2.Digest which uses SHA256 as the hash.
	VariantSHA256

	// VariantSHA512 is a variant of the influxdb2.Digest which uses SHA512 as the hash.
	VariantSHA512
)

const DefaultVariant = VariantSHA256

var AllVariants []Variant = []Variant{
	VariantSHA256,
	VariantSHA512,
}

// Prefix returns the influxdb2.Variant prefix identifier.
func (v Variant) Prefix() (prefix string) {
	switch v {
	case VariantSHA256:
		return VariantIdentifierSHA256
	case VariantSHA512:
		return VariantIdentifierSHA512
	default:
		return
	}
}

// RegisterDecoder registers the variant with a decoder.
func (v Variant) RegisterDecoder(r algorithm.DecoderRegister) error {
	switch v {
	case VariantSHA256:
		return RegisterDecoderSHA256(r)
	case VariantSHA512:
		return RegisterDecoderSHA512(r)
	default:
		return fmt.Errorf("RegisterDecoder with invalid variant %v", v)
	}
}

// Decode performs the decode operation for this influxdb2.Variant.
func (v Variant) Decode(src string) (dst []byte, err error) {
	switch v {
	case VariantSHA256, VariantSHA512:
		return base64.URLEncoding.DecodeString(src)
	default:
		return []byte(src), nil
	}
}

// Encode performs the encode operation for this influxdb2.Variant.
func (v Variant) Encode(src []byte) (dst string) {
	return base64.URLEncoding.EncodeToString(v.Hash(src))
}

// Hash performs the hashing operation on the input, returning the raw binary.
func (v Variant) Hash(input []byte) []byte {
	switch v {
	case VariantSHA256:
		h := sha256.Sum256(input)
		return h[:]
	case VariantSHA512:
		h := sha512.Sum512(input)
		return h[:]
	}
	return nil
}
