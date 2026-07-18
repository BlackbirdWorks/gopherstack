package mediapackage_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediapackage"
)

// TestBackend_CreateChannel_SetsCreatedAt verifies CreateChannel populates
// CreatedAt -- real MediaPackage always returns this field, but it was
// previously absent from the Channel type entirely.
func TestBackend_CreateChannel_SetsCreatedAt(t *testing.T) {
	t.Parallel()

	b := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")

	ch, err := b.CreateChannel("chan1", "desc", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, ch.CreatedAt)
}

// TestBackend_ConfigureLogs_PersistsLogGroups verifies ConfigureLogs actually
// stores the egress/ingress log group names instead of discarding them --
// previously the backend accepted both parameters, discarded them with `_ =`,
// and returned the channel unchanged (a disguised no-op).
func TestBackend_ConfigureLogs_PersistsLogGroups(t *testing.T) {
	t.Parallel()

	egress := "/aws/MediaPackage/EgressAccessLogs"
	ingress := "/aws/MediaPackage/IngressAccessLogs"

	tests := []struct {
		egress      *string
		ingress     *string
		wantEgress  *string
		wantIngress *string
		name        string
	}{
		{
			name:        "sets both log groups",
			egress:      &egress,
			ingress:     &ingress,
			wantEgress:  &egress,
			wantIngress: &ingress,
		},
		{
			name:        "nil leaves existing configuration untouched",
			egress:      nil,
			ingress:     nil,
			wantEgress:  nil,
			wantIngress: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateChannel("chan1", "", nil)
			require.NoError(t, err)

			ch, err := b.ConfigureLogs("chan1", tt.egress, tt.ingress)
			require.NoError(t, err)

			if tt.wantEgress == nil {
				assert.Nil(t, ch.EgressLogGroupName)
			} else {
				require.NotNil(t, ch.EgressLogGroupName)
				assert.Equal(t, *tt.wantEgress, *ch.EgressLogGroupName)
			}

			if tt.wantIngress == nil {
				assert.Nil(t, ch.IngressLogGroupName)
			} else {
				require.NotNil(t, ch.IngressLogGroupName)
				assert.Equal(t, *tt.wantIngress, *ch.IngressLogGroupName)
			}
		})
	}
}

// TestBackend_ConfigureLogs_PartialUpdatePreservesOther verifies that
// configuring only egress logs does not clobber a previously configured
// ingress log group (each is independently optional, per AWS's
// ConfigureLogsInput shape).
func TestBackend_ConfigureLogs_PartialUpdatePreservesOther(t *testing.T) {
	t.Parallel()

	egress := "/aws/MediaPackage/EgressAccessLogs"
	ingress := "/aws/MediaPackage/IngressAccessLogs"

	b := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateChannel("chan1", "", nil)
	require.NoError(t, err)

	_, err = b.ConfigureLogs("chan1", &egress, &ingress)
	require.NoError(t, err)

	updatedEgress := "/aws/MediaPackage/NewEgress"
	ch, err := b.ConfigureLogs("chan1", &updatedEgress, nil)
	require.NoError(t, err)

	require.NotNil(t, ch.EgressLogGroupName)
	assert.Equal(t, updatedEgress, *ch.EgressLogGroupName)
	require.NotNil(t, ch.IngressLogGroupName)
	assert.Equal(t, ingress, *ch.IngressLogGroupName)
}

// TestBackend_RotateChannelCredentials_OnlyFirstEndpoint verifies the
// deprecated RotateChannelCredentials op only rotates ingestEndpoints[0]'s
// username/password (per its AWS doc comment: "Changes the Channel's first
// IngestEndpoint's username and password"), leaving IDs/URLs and the second
// endpoint's credentials untouched. The backend previously regenerated both
// endpoints from scratch with brand new IDs and URLs, which does not match
// real MediaPackage behavior.
func TestBackend_RotateChannelCredentials_OnlyFirstEndpoint(t *testing.T) {
	t.Parallel()

	b := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
	before, err := b.CreateChannel("chan1", "", nil)
	require.NoError(t, err)
	require.Len(t, before.HlsIngest.IngestEndpoints, 2)

	beforeFirst := *before.HlsIngest.IngestEndpoints[0]
	beforeSecond := *before.HlsIngest.IngestEndpoints[1]

	after, err := b.RotateChannelCredentials("chan1")
	require.NoError(t, err)
	require.Len(t, after.HlsIngest.IngestEndpoints, 2)

	afterFirst := after.HlsIngest.IngestEndpoints[0]
	afterSecond := after.HlsIngest.IngestEndpoints[1]

	assert.Equal(t, beforeFirst.ID, afterFirst.ID, "first endpoint ID must not change")
	assert.Equal(t, beforeFirst.URL, afterFirst.URL, "first endpoint URL must not change")
	assert.NotEqual(t, beforeFirst.Username, afterFirst.Username, "first endpoint username must rotate")
	assert.NotEqual(t, beforeFirst.Password, afterFirst.Password, "first endpoint password must rotate")

	assert.Equal(t, beforeSecond, *afterSecond, "second endpoint must be untouched")
}
