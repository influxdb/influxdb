package influxdb2

import (
	"fmt"
	"strings"

	"github.com/go-crypt/crypt"
	"github.com/go-crypt/crypt/algorithm"
)

// RegisterDecoder registers all influxdb2 decoders.
func RegisterDecoder(r algorithm.DecoderRegister) error {
	if err := RegisterDecoderSHA256(r); err != nil {
		return err
	}
	if err := RegisterDecoderSHA512(r); err != nil {
		return err
	}
	return nil
}

// RegisterDecoderSHA256 registers specifically the SHA256 decoder variant with the algorithm.DecoderRegister.
func RegisterDecoderSHA256(r algorithm.DecoderRegister) (err error) {
	if err = r.RegisterDecodeFunc(VariantSHA256.Prefix(), DecodeVariant(VariantSHA256)); err != nil {
		return err
	}

	return nil
}

// RegisterDecoderSHA512 registers specifically the SHA512 decoder variant with the algorithm.DecoderRegister.
func RegisterDecoderSHA512(r algorithm.DecoderRegister) (err error) {
	if err = r.RegisterDecodeFunc(VariantSHA512.Prefix(), DecodeVariant(VariantSHA512)); err != nil {
		return err
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
		return variant, nil, fmt.Errorf("%w: hash identifier is not valid for %s digest", algorithm.ErrEncodedHashInvalidIdentifier, AlgName)
	}

	return variant, parts[2:], nil
}

func decode(variant Variant, parts []string) (digest algorithm.Digest, err error) {
	decoded := &Digest{
		Variant: variant,
	}

	if decoded.key, err = decoded.Variant.Decode(parts[0]); err != nil {
		return nil, fmt.Errorf("%w: %v", algorithm.ErrEncodedHashKeyEncoding, err)
	}

	if len(decoded.key) == 0 {
		return nil, fmt.Errorf("%w: key has 0 bytes", algorithm.ErrEncodedHashKeyEncoding)
	}

	return decoded, nil
}
