package influxdb2

import (
	"crypto/subtle"
	"fmt"

	"github.com/go-crypt/crypt/algorithm"
)

// NewDigest creates a new plaintext.Digest using the plaintext.Variant.
func NewDigest(password string) (digest Digest) {
	return NewSHA256Digest(password)
}

// NewSHA256Digest creates a new influxdb2.Digest using the SHA256 for the hash.
func NewSHA256Digest(password string) (digest Digest) {
	digest = Digest{
		Variant: VariantSHA256,
		key:     []byte(password),
	}

	return digest
}

// Digest is an algorithm.Digest which handles influxdb2 matching.
type Digest struct {
	Variant Variant

	key []byte
}

// Match returns true if the string password matches the current influxdb2.Digest.
func (d *Digest) Match(password string) bool {
	return d.MatchBytes([]byte(password))
}

// MatchBytes returns true if the []byte passwordBytes matches the current influxdb2.Digest.
func (d *Digest) MatchBytes(passwordBytes []byte) bool {
	m, _ := d.MatchBytesAdvanced(passwordBytes)
	return m
}

// MatchAdvanced is the same as Match except if there is an error it returns that as well.
func (d *Digest) MatchAdvanced(password string) (match bool, err error) {
	return d.MatchBytesAdvanced([]byte(password))
}

// MatchBytesAdvanced is the same as MatchBytes except if there is an error it returns that as well.
func (d *Digest) MatchBytesAdvanced(passwordBytes []byte) (match bool, err error) {
	if len(d.key) == 0 {
		return false, fmt.Errorf(algorithm.ErrFmtDigestMatch, AlgName, fmt.Errorf("%w: key has 0 bytes", algorithm.ErrPasswordInvalid))
	}

	input := d.Variant.Hash(passwordBytes)
	return subtle.ConstantTimeCompare(d.key, input) == 1, nil
}

// Encode returns the encoded form of this plaintext.Digest.
func (d *Digest) Encode() string {
	return fmt.Sprintf(EncodingFmt, d.Variant.Prefix(), d.Variant.Encode(d.Key()))
}

// String returns the storable format of the plaintext.Digest encoded hash.
func (d *Digest) String() string {
	return d.Encode()
}

func (d *Digest) defaults() {
	switch d.Variant {
	case VariantSHA256, VariantSHA512:
		break
	default:
		d.Variant = DefaultVariant
	}
}

// Key returns the raw plaintext key which can be used in situations where the plaintext value is required such as
// validating JWT's signed by HMAC-SHA256.
func (d *Digest) Key() []byte {
	return d.key
}
