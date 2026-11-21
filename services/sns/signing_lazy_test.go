package sns_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

func TestSigningKeyGeneratedLazily(t *testing.T) {
	t.Parallel()

	tests := []struct {
		use  func(b *sns.InMemoryBackend)
		name string
		want bool
	}{
		{name: "construct_only", use: func(*sns.InMemoryBackend) {}, want: false},
		{name: "cert_pem", use: func(b *sns.InMemoryBackend) { assert.NotEmpty(t, b.SigningCertPEM()) }, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			tt.use(b)
			assert.Equal(t, tt.want, b.SigningKeyGenerated())
		})
	}
}
