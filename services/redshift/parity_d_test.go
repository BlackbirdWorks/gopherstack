package redshift_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// TestParity_RestoreFromClusterSnapshot_TagsInitialized guards against a
// nil-pointer panic: a cluster produced by RestoreFromClusterSnapshot must own
// a live Tags collection just like CreateCluster, because DescribeTags calls
// c.Tags.Clone() unconditionally for every cluster in the backend.
func TestParity_RestoreFromClusterSnapshot_TagsInitialized(t *testing.T) {
	t.Parallel()

	b := newRedshiftBackend()

	_, err := b.CreateCluster("src-cluster", "dc2.large", "dev", "admin")
	require.NoError(t, err)

	_, err = b.CreateClusterSnapshot("src-snap", "src-cluster")
	require.NoError(t, err)

	_, err = b.RestoreFromClusterSnapshot("restored-cluster", "src-snap")
	require.NoError(t, err)

	// Previously panicked with a nil pointer dereference inside tags.Tags.Clone.
	require.NotPanics(t, func() {
		_ = b.DescribeTags()
	})

	// CreateTags/DeleteTags must also work against the restored cluster.
	require.NoError(t, b.CreateTags("restored-cluster", map[string]string{"env": "prod"}))

	all := b.DescribeTags()
	assert.Equal(t, map[string]string{"env": "prod"}, all["restored-cluster"])

	require.NoError(t, b.DeleteTags("restored-cluster", []string{"env"}))
}

// TestParity_RestoreFromClusterSnapshot_TagsInitialized_HTTP is the HTTP-level
// counterpart: DescribeTags must not 500 after a RestoreFromClusterSnapshot,
// including via the tag-filtered DescribeClusters path.
func TestParity_RestoreFromClusterSnapshot_TagsInitialized_HTTP(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=src-cluster")
	postRedshiftForm(t, h,
		"Action=CreateClusterSnapshot&Version=2012-12-01&SnapshotIdentifier=src-snap&ClusterIdentifier=src-cluster")

	rec := postRedshiftForm(t, h,
		"Action=RestoreFromClusterSnapshot&Version=2012-12-01"+
			"&ClusterIdentifier=restored-cluster&SnapshotIdentifier=src-snap")
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postRedshiftForm(t, h, "Action=DescribeTags&Version=2012-12-01")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeTagsResponse")

	rec = postRedshiftForm(t, h,
		"Action=DescribeClusters&Version=2012-12-01&TagKey=env")
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestParity_RestoreFromClusterSnapshot_Lifecycle verifies the restored
// cluster's ClusterStatus actually reaches "available" instead of getting
// stuck in "restoring" forever.
func TestParity_RestoreFromClusterSnapshot_Lifecycle(t *testing.T) {
	t.Parallel()

	t.Run("no_activation_delay_is_immediately_available", func(t *testing.T) {
		t.Parallel()

		b := newRedshiftBackend()

		_, err := b.CreateCluster("src-cluster", "", "", "")
		require.NoError(t, err)
		_, err = b.CreateClusterSnapshot("src-snap", "src-cluster")
		require.NoError(t, err)

		restored, err := b.RestoreFromClusterSnapshot("restored-cluster", "src-snap")
		require.NoError(t, err)
		assert.Equal(t, "available", restored.Status)
	})

	t.Run("activation_delay_transitions_restoring_to_available", func(t *testing.T) {
		t.Parallel()

		b := newRedshiftBackend()
		redshift.SetClusterActivationDelay(b, 20*time.Millisecond)

		_, err := b.CreateCluster("src-cluster", "", "", "")
		require.NoError(t, err)
		_, err = b.CreateClusterSnapshot("src-snap", "src-cluster")
		require.NoError(t, err)

		restored, err := b.RestoreFromClusterSnapshot("restored-cluster", "src-snap")
		require.NoError(t, err)
		assert.Equal(t, "restoring", restored.Status,
			"restored cluster should start in restoring state when an activation delay is configured")

		require.Eventually(t, func() bool {
			clusters, _, descErr := b.DescribeClusters("restored-cluster", "", 0)

			return descErr == nil && len(clusters) == 1 && clusters[0].Status == "available"
		}, time.Second, 5*time.Millisecond,
			"restored cluster must transition out of restoring, previously it never did")
	})
}

// TestParity_ModifyCluster_EncryptedTriState verifies that Encrypted and
// EnhancedVpcRouting are tri-state on the wire: omitting the field leaves the
// setting unchanged, while explicitly sending "false" turns it off (e.g.
// decrypting a cluster). A plain bool previously could not distinguish
// "not specified" from "explicitly false", making it impossible to ever
// disable either setting via ModifyCluster.
func TestParity_ModifyCluster_EncryptedTriState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		modifyBody    string
		wantEncrypted string
	}{
		{
			name:          "unset_leaves_encrypted_true_unchanged",
			modifyBody:    "&NodeType=ra3.xlplus",
			wantEncrypted: "<Encrypted>true</Encrypted>",
		},
		{
			name:          "explicit_false_decrypts",
			modifyBody:    "&Encrypted=false",
			wantEncrypted: "<Encrypted>false</Encrypted>",
		},
		{
			name:          "explicit_true_stays_encrypted",
			modifyBody:    "&Encrypted=true",
			wantEncrypted: "<Encrypted>true</Encrypted>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=enc-cluster")
			postRedshiftForm(t, h,
				"Action=ModifyCluster&Version=2012-12-01&ClusterIdentifier=enc-cluster&Encrypted=true")

			rec := postRedshiftForm(t, h,
				"Action=ModifyCluster&Version=2012-12-01&ClusterIdentifier=enc-cluster"+tt.modifyBody)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantEncrypted)
		})
	}
}

// TestParity_GetClusterCredentials_Expiration verifies GetClusterCredentials
// serializes the Expiration field. It was previously computed by the backend
// but never wired into the response XML at all, unlike the sibling
// GetClusterCredentialsWithIAM operation which does include it -- a client
// (e.g. a JDBC/ODBC driver) has no way to know when the temporary password
// expires.
func TestParity_GetClusterCredentials_Expiration(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=cred-cluster")

	rec := postRedshiftForm(t, h,
		"Action=GetClusterCredentials&Version=2012-12-01&ClusterIdentifier=cred-cluster&DbUser=alice")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Expiration>")
	assert.Contains(t, rec.Body.String(), "GetClusterCredentialsResult")
}
