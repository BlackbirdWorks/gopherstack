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
