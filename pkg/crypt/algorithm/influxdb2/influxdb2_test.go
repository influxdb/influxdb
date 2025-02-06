package influxdb2

import (
	"testing"

	"github.com/go-crypt/crypt"
	"github.com/go-crypt/crypt/algorithm"
	"github.com/stretchr/testify/require"
)

func TestInfluxDB2Outputs(t *testing.T) {
	hasherSha256, err := New(WithVariant(VariantSHA256))
	require.NoError(t, err)

	hasherSha512, err := New(WithVariant(VariantSHA512))
	require.NoError(t, err)

	cases := []struct {
		desc   string
		token  string
		phc    string
		hasher *Hasher
	}{
		{
			"ShouldValidateWithSHA256",
			"c27cb2033ab304629d32e1dbcd0ca7186322d3be98af5dd4c329ab800ef85d73",
			"$influxdb2-sha256$kMrC1MoFhWvvKSgyqpMaLuo2O3LINv4_XByCSkfV9K0=",
			hasherSha256,
		},
		{
			"ShouldValidateWithSHA512",
			"322be1195f22da43a88e5ef1e856b707d6b7d41a6068feec343decbc5d784e50",
			"$influxdb2-sha512$ffgzLTTAyWBDczT0kKzwLzLjlemQh6UiFvqIA0CPd-B7qgqAetWEuKRI9qOLeE4ak6mxcxwthyKUO40sHf5V5w==",
			hasherSha512,
		},
	}

	decoder := crypt.NewDecoder()
	require.NoError(t, RegisterDecoder(decoder))

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			{
				// Will the token match the PHC format?
				var digest algorithm.Digest
				digest, err := decoder.Decode(tc.phc)
				require.NoError(t, err)
				require.True(t, digest.Match(tc.token))

				// Will an incorrect token not match?
				require.False(t, digest.Match("WrongToken"))
			}

			{
				// Is hashing the token deterministic?
				digest, err := tc.hasher.Hash(tc.token)
				require.NoError(t, err)
				phc := digest.Encode()
				require.Equal(t, tc.phc, phc)

			}
		})
	}
}
