package redshift_test

import (
	"regexp"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// usageLimitIDPattern/reservedNodeIDPattern lock in the fix for IDs that were
// derived from time.Now().UnixNano() and collided under synctest.
var (
	usageLimitIDPattern   = regexp.MustCompile(`^ul-[0-9a-f]{16}$`)
	reservedNodeIDPattern = regexp.MustCompile(`^rn-[0-9a-f]{16}$`)
)

func TestRedshiftBackend_IDs_Unique(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create  func(t *testing.T, b *redshift.InMemoryBackend) string
		pattern *regexp.Regexp
		name    string
	}{
		{
			name:    "usage_limit",
			pattern: usageLimitIDPattern,
			create: func(t *testing.T, b *redshift.InMemoryBackend) string {
				t.Helper()

				ul, err := b.CreateUsageLimit("c1", "spectrum", "time", "log", 100, nil)
				require.NoError(t, err)

				return ul.UsageLimitID
			},
		},
		{
			name:    "reserved_node",
			pattern: reservedNodeIDPattern,
			create: func(t *testing.T, b *redshift.InMemoryBackend) string {
				t.Helper()

				node, err := b.PurchaseReservedNodeOffering("offering-dc2-large-1yr-allupfront", "", 1)
				require.NoError(t, err)

				return node.ReservedNodeID
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
				_, err := b.CreateCluster("c1", "dc2.large", "dev", "admin", nil, "", redshift.CreateClusterOptions{})
				require.NoError(t, err)

				id1 := tt.create(t, b)
				id2 := tt.create(t, b)

				assert.NotEqual(t, id1, id2, "two resources created back-to-back must get distinct IDs")
				assert.Regexp(t, tt.pattern, id1)
				assert.Regexp(t, tt.pattern, id2)
			})
		})
	}
}
