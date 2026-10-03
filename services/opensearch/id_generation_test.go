package opensearch_test

import (
	"regexp"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// changeUUIDPattern locks in the fix for DryRunId/ChangeId, previously
// derived from time.Now().UnixNano() and colliding under synctest.
var changeUUIDPattern = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`,
)

func TestOpenSearchBackend_ChangeIDs_Unique(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, b *opensearch.InMemoryBackend) string
		name   string
	}{
		{
			name: "update_domain_config_change_id",
			create: func(t *testing.T, b *opensearch.InMemoryBackend) string {
				t.Helper()

				d, err := b.UpdateDomainConfig("dom-a", opensearch.UpdateDomainConfigInput{})
				require.NoError(t, err)

				return d.LastChangeID
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := opensearch.NewInMemoryBackend("000000000000", "us-east-1")
				_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: "dom-a"})
				require.NoError(t, err)

				id1 := tt.create(t, b)
				id2 := tt.create(t, b)

				assert.NotEqual(t, id1, id2, "two changes created back-to-back must get distinct IDs")
				assert.Regexp(t, changeUUIDPattern, id1)
				assert.Regexp(t, changeUUIDPattern, id2)
			})
		})
	}
}

func TestOpenSearchBackend_DryRunID_Format(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: "dom-b"})
	require.NoError(t, err)

	dr, err := b.GetDryRunProgress("dom-b")
	require.NoError(t, err)
	assert.Regexp(t, changeUUIDPattern, dr.DryRunID)
}

// TestOpenSearchBackend_ServerlessPolicyVersions_Unique checks same-instant updates
// get distinct PolicyVersion/ConfigVersion tokens.
func TestOpenSearchBackend_ServerlessPolicyVersions_Unique(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update func(t *testing.T, b *opensearch.InMemoryBackend) string
		name   string
	}{
		{
			name: "access_policy_version",
			update: func(t *testing.T, b *opensearch.InMemoryBackend) string {
				t.Helper()

				ap, err := b.UpdateServerlessAccessPolicy("data", "pol-a", "desc", `{"a":1}`, "")
				require.NoError(t, err)

				return ap.PolicyVersion
			},
		},
		{
			name: "security_config_version",
			update: func(t *testing.T, b *opensearch.InMemoryBackend) string {
				t.Helper()

				sc, err := b.UpdateServerlessSecurityConfig("saml/000000000000/1", "desc", "", nil)
				require.NoError(t, err)

				return sc.ConfigVersion
			},
		},
		{
			name: "encryption_policy_version",
			update: func(t *testing.T, b *opensearch.InMemoryBackend) string {
				t.Helper()

				ep, err := b.UpdateServerlessEncryptionPolicy("data", "pol-e", "desc", `{"e":1}`, "")
				require.NoError(t, err)

				return ep.PolicyVersion
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := opensearch.NewInMemoryBackend("000000000000", "us-east-1")

				_, err := b.CreateServerlessAccessPolicy("data", "pol-a", "desc", `{"a":0}`)
				require.NoError(t, err)
				_, err = b.CreateServerlessSecurityConfig("saml", "desc", nil)
				require.NoError(t, err)
				_, err = b.CreateServerlessEncryptionPolicy("data", "pol-e", "desc", `{"e":0}`)
				require.NoError(t, err)

				v1 := tt.update(t, b)
				v2 := tt.update(t, b)

				assert.NotEqual(t, v1, v2, "two updates in the same instant must get distinct versions")
			})
		})
	}
}

// TestOpenSearchBackend_LifecyclePolicyVersion_Unique chains updates, since each must
// pass the version returned by the previous call.
func TestOpenSearchBackend_LifecyclePolicyVersion_Unique(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := opensearch.NewInMemoryBackend("000000000000", "us-east-1")

		lp, err := b.CreateServerlessLifecyclePolicy("retention", "lp-a", "desc", `{"l":0}`)
		require.NoError(t, err)

		lp1, err := b.UpdateServerlessLifecyclePolicy("retention", "lp-a", "desc", `{"l":1}`, lp.PolicyVersion)
		require.NoError(t, err)

		lp2, err := b.UpdateServerlessLifecyclePolicy("retention", "lp-a", "desc", `{"l":2}`, lp1.PolicyVersion)
		require.NoError(t, err)

		assert.NotEqual(t, lp1.PolicyVersion, lp2.PolicyVersion,
			"two updates in the same instant must get distinct versions")
	})
}
