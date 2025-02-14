package influxdb2

const (
	// EncodingFmt is the encoding format for this algorithm.
	EncodingFmt = "$%s$%s"

	// EncodingSections is the number sections in EncodingFmt delimited by crypt.Delimiter.
	EncodingSections = 2

	// AlgName is the name for this algorithm.
	AlgName = "influxdb2"

	// VariantIdentifierSHA256 is the identifier used in SHA256 variants of this algorithm.
	VariantIdentifierSHA256 = "influxdb2-sha256"

	// VariantIdentifierSHA512 is the identifier used in SHA512 variants of this algorithm.
	VariantIdentifierSHA512 = "influxdb2-sha512"
)
