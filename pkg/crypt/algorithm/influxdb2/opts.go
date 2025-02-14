package influxdb2

import (
	"fmt"

	"github.com/go-crypt/crypt/algorithm"
)

// Opt describes the functional option pattern for the plaintext.Hasher.
type Opt func(h *Hasher) (err error)

// WithVariant configures the plaintext.Variant of the resulting plaintext.Digest.
// Default is plaintext.VariantPlainText.
func WithVariant(variant Variant) Opt {
	return func(h *Hasher) (err error) {
		switch variant {
		case VariantNone, VariantSHA256, VariantSHA512:
			h.variant = variant

			return nil
		default:
			return fmt.Errorf(algorithm.ErrFmtHasherValidation, AlgName, fmt.Errorf("%w: variant '%d' is invalid", algorithm.ErrParameterInvalid, variant))
		}
	}
}

// WithVariantName uses the variant name or identifier to configure the plaintext.Variant of the resulting plaintext.Digest.
// Default is plaintext.VariantPlainText.
func WithVariantName(identifier string) Opt {
	return func(h *Hasher) (err error) {
		if identifier == "" {
			return nil
		}

		variant := NewVariant(identifier)

		if variant == VariantNone {
			return fmt.Errorf(algorithm.ErrFmtHasherValidation, AlgName, fmt.Errorf("%w: variant identifier '%s' is invalid", algorithm.ErrParameterInvalid, identifier))
		}

		h.variant = variant

		return nil
	}
}
