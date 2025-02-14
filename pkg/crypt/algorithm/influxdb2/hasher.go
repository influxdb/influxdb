package influxdb2

import (
	"github.com/go-crypt/crypt/algorithm"
)

// New returns a *hasher.Hasher without any settings configured.
func New(opts ...Opt) (hasher *Hasher, err error) {
	hasher = &Hasher{}

	if err = hasher.WithOptions(opts...); err != nil {
		return nil, err
	}

	if err = hasher.Validate(); err != nil {
		return nil, err
	}

	return hasher, nil
}

// Hasher is a crypt.Hash for plaintext which can be initialized via influxdb2.New using a functional options pattern.
type Hasher struct {
	variant Variant
}

// WithOptions applies the provided functional options provided as an influxdb2.Opt to the influxdb2.Hasher.
func (h *Hasher) WithOptions(opts ...Opt) (err error) {
	for _, opt := range opts {
		if err = opt(h); err != nil {
			return err
		}
	}

	return nil
}

// Variant returns which variant this hasher implements.
func (h *Hasher) Variant() Variant {
	return h.variant
}

// Validate checks the hasher configuration to ensure it's valid. This should be used when the influxdb2.Hasher is going
// to be reused and you should use it in conjunction with MustHash.
func (h *Hasher) Validate() (err error) {
	return nil
}

// Hash performs the hashing operation on a password and resets any relevant parameters such as a manually set salt.
// It then returns a plaintext.Digest and error.
func (h *Hasher) Hash(password string) (hashed algorithm.Digest, err error) {
	d := &Digest{
		Variant: h.variant,
		key:     []byte(password),
	}

	d.defaults()

	return d, nil
}

// HashWithSalt is an overload of plaintext.Digest that also accepts a salt.
func (h *Hasher) HashWithSalt(password string, _ []byte) (hashed algorithm.Digest, err error) {
	return h.Hash(password)
}

// MustHash overloads the Hash method and panics if the error is not nil. It's recommended if you use this method to
// utilize the Validate method first or handle the panic appropriately.
func (h *Hasher) MustHash(password string) (hashed algorithm.Digest) {
	if d, err := h.Hash(password); err != nil {
		panic(err)
	} else {
		return d
	}
}
