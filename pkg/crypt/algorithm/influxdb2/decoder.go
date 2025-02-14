package influxdb2

import (
	"fmt"
	"strings"

	"github.com/go-crypt/crypt"
	"github.com/go-crypt/crypt/algorithm"
)

// RegisterDecoder registers all influxdb2 decoders.
func RegisterDecoder(r algorithm.DecoderRegister) error {
	for _, variant := range AllVariants {
		if err := RegisterDecoderVariant(r, variant); err != nil {
			return err
		}
	}
	return nil
}

// RegisterDecoderSHA256 registers specifically the SHA256 decoder variant with the algorithm.DecoderRegister.
func RegisterDecoderSHA256(r algorithm.DecoderRegister) (err error) {
	return RegisterDecoderVariant(r, VariantSHA256)
}

// RegisterDecoderSHA512 registers specifically the SHA512 decoder variant with the algorithm.DecoderRegister.
func RegisterDecoderSHA512(r algorithm.DecoderRegister) (err error) {
	return RegisterDecoderVariant(r, VariantSHA512)
}

// RegisterDecoderVariant registers the specified decoder variant.
func RegisterDecoderVariant(r algorithm.DecoderRegister, variant Variant) error {
	if err := r.RegisterDecodeFunc(variant.Prefix(), DecodeVariant(variant)); err != nil {
		return fmt.Errorf("error registered decoder variant %s: %w", variant.Prefix(), err)
	}
	return nil
}

// Decode the encoded digest into a algorithm.Digest.
func Decode(encodedDigest string) (digest algorithm.Digest, err error) {
	return DecodeVariant(VariantNone)(encodedDigest)
}

// DecodeVariant the encoded digest into a algorithm.Digest provided it matches the provided plaintext.Variant. If
// plaintext.VariantNone is used all variants can be decoded.
func DecodeVariant(v Variant) func(encodedDigest string) (digest algorithm.Digest, err error) {
	return func(encodedDigest string) (digest algorithm.Digest, err error) {
		var (
			parts   []string
			variant Variant
		)

		if variant, parts, err = decoderParts(encodedDigest); err != nil {
			return nil, fmt.Errorf(algorithm.ErrFmtDigestDecode, AlgName, err)
		}

		if v != VariantNone && v != variant {
			return nil, fmt.Errorf(algorithm.ErrFmtDigestDecode, AlgName, fmt.Errorf("the '%s' variant cannot be decoded only the '%s' variant can be", variant.Prefix(), v.Prefix()))
		}

		if digest, err = decode(variant, parts); err != nil {
			return nil, fmt.Errorf(algorithm.ErrFmtDigestDecode, AlgName, err)
		}

		return digest, nil
	}
}

func decoderParts(encodedDigest string) (Variant, []string, error) {
	// First section is empty, hence the +1.
	parts := strings.SplitN(encodedDigest, crypt.Delimiter, EncodingSections+1)

	if len(parts) != EncodingSections+1 {
		return VariantNone, nil, algorithm.ErrEncodedHashInvalidFormat
	}

	variant := NewVariant(parts[1])
	if variant == VariantNone {
		return variant, nil, fmt.Errorf("hash identifier is not valid for %s digest: %w", AlgName, algorithm.ErrEncodedHashInvalidIdentifier)
	}

	return variant, parts[2:], nil
}

func decode(variant Variant, parts []string) (digest algorithm.Digest, err error) {
	decoded := &Digest{
		Variant: variant,
	}

	if decoded.key, err = decoded.Variant.Decode(parts[0]); err != nil {
		return nil, fmt.Errorf("%w: %w", algorithm.ErrEncodedHashKeyEncoding, err)
	}

	if len(decoded.key) == 0 {
		return nil, fmt.Errorf("key has 0 bytes: %w", algorithm.ErrEncodedHashKeyEncoding)
	}

	return decoded, nil
}
