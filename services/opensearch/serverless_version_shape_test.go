package opensearch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

const serverlessVersionPattern = `^[A-Za-z0-9+/]{20,36}={0,2}$`

func TestServerlessPolicyVersionShape(t *testing.T) {
	t.Parallel()

	doc := `{"Rules":[]}`

	tests := []struct {
		run  func(t *testing.T, b *opensearch.InMemoryBackend) string
		name string
	}{
		{run: func(t *testing.T, b *opensearch.InMemoryBackend) string {
			t.Helper()

			p, err := b.CreateServerlessAccessPolicy("data", "p", "", doc)
			require.NoError(t, err)
			p, err = b.UpdateServerlessAccessPolicy("data", "p", "d", doc, p.PolicyVersion)
			require.NoError(t, err)

			return p.PolicyVersion
		}, name: "access"},
		{run: func(t *testing.T, b *opensearch.InMemoryBackend) string {
			t.Helper()

			p, err := b.CreateServerlessEncryptionPolicy("encryption", "p", "", doc)
			require.NoError(t, err)
			p, err = b.UpdateServerlessEncryptionPolicy("encryption", "p", "d", doc, p.PolicyVersion)
			require.NoError(t, err)

			return p.PolicyVersion
		}, name: "encryption"},
		{run: func(t *testing.T, b *opensearch.InMemoryBackend) string {
			t.Helper()

			p, err := b.CreateServerlessLifecyclePolicy("retention", "p", "", doc)
			require.NoError(t, err)
			p, err = b.UpdateServerlessLifecyclePolicy("retention", "p", "d", doc, p.PolicyVersion)
			require.NoError(t, err)

			return p.PolicyVersion
		}, name: "lifecycle"},
		{run: func(t *testing.T, b *opensearch.InMemoryBackend) string {
			t.Helper()

			c, err := b.CreateServerlessSecurityConfig("saml", "", &opensearch.ServerlessSAMLOptions{})
			require.NoError(t, err)
			c, err = b.UpdateServerlessSecurityConfig(c.ID, "d", c.ConfigVersion, &opensearch.ServerlessSAMLOptions{})
			require.NoError(t, err)

			return c.ConfigVersion
		}, name: "security_config"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := opensearch.NewInMemoryBackend("123456789012", "us-east-1")
			assert.Regexp(t, serverlessVersionPattern, tt.run(t, b))
		})
	}
}
