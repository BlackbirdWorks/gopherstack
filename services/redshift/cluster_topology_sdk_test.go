package redshift_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

func TestSDK_CreateClusterTopology(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		nodeType  string
		cluster   string
		wantCode  string
		nodes     int32
		wantNodes int32
	}{
		{name: "default", nodeType: "dc2.large", wantNodes: 1},
		{name: "multi_nodes", nodeType: "ra3.xlplus", nodes: 3, wantNodes: 3},
		{
			name:     "single_with_three",
			nodeType: "dc2.large",
			cluster:  "single-node",
			nodes:    3,
			wantCode: "InvalidParameterCombination",
		},
		{
			name:     "multi_with_one",
			nodeType: "dc2.large",
			cluster:  "multi-node",
			nodes:    1,
			wantCode: "InvalidParameterCombination",
		},
		{name: "too_many", nodeType: "dc2.large", nodes: 101, wantCode: "InvalidParameterValue"},
		{name: "bad_type", nodeType: "dc2.large", cluster: "weird", wantCode: "InvalidParameterValue"},
		{name: "bad_node_type", nodeType: "bogus.type", wantCode: "InvalidParameterValue"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newLifecycleFieldsBackendAndClient(t)
			in := &redshiftsdk.CreateClusterInput{
				ClusterIdentifier:  aws.String("topo"),
				NodeType:           aws.String(tc.nodeType),
				MasterUsername:     aws.String("admin"),
				MasterUserPassword: aws.String("Passw0rd1"),
			}
			if tc.nodes != 0 {
				in.NumberOfNodes = aws.Int32(tc.nodes)
			}
			if tc.cluster != "" {
				in.ClusterType = aws.String(tc.cluster)
			}

			out, err := client.CreateCluster(t.Context(), in)
			if tc.wantCode != "" {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tc.wantCode, apiErr.ErrorCode())
				assert.NotContains(t, apiErr.ErrorMessage(), tc.wantCode+":")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantNodes, aws.ToInt32(out.Cluster.NumberOfNodes))
		})
	}
}

func TestSDK_ErrorMessageHasNoCodePrefix(t *testing.T) {
	t.Parallel()

	_, client := newLifecycleFieldsBackendAndClient(t)

	_, err := client.DescribeClusters(t.Context(), &redshiftsdk.DescribeClustersInput{
		ClusterIdentifier: aws.String("missing"),
	})
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ClusterNotFound", apiErr.ErrorCode())
	assert.Equal(t, "cluster missing not found", apiErr.ErrorMessage())
}

func TestBackend_ClusterStateTransitions(t *testing.T) {
	t.Parallel()

	const delay = time.Minute

	cases := []struct {
		act        func(b *redshift.InMemoryBackend) (*redshift.Cluster, error)
		name       string
		wantNow    string
		wantLater  string
		wantErrNow bool
	}{
		{
			name: "resize",
			act: func(b *redshift.InMemoryBackend) (*redshift.Cluster, error) {
				return b.ResizeCluster("c", "", "", 4, false)
			},
			wantNow: "resizing", wantLater: "available",
		},
		{
			name:    "pause",
			act:     func(b *redshift.InMemoryBackend) (*redshift.Cluster, error) { return b.PauseCluster("c") },
			wantNow: "pausing", wantLater: "paused",
		},
		{
			name:       "resume_available",
			act:        func(b *redshift.InMemoryBackend) (*redshift.Cluster, error) { return b.ResumeCluster("c") },
			wantErrNow: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
				b.SetClusterActivationDelay(delay)
				_, err := b.CreateCluster("c", "dc2.large", "dev", "admin", nil, "", redshift.CreateClusterOptions{})
				require.NoError(t, err)
				time.Sleep(delay)

				got, err := tc.act(b)
				if tc.wantErrNow {
					require.ErrorIs(t, err, redshift.ErrClusterInvalidState)

					return
				}

				require.NoError(t, err)
				assert.Equal(t, tc.wantNow, got.Status)

				time.Sleep(delay)

				list, _, err := b.DescribeClusters("c", "", 0, nil, nil)
				require.NoError(t, err)
				assert.Equal(t, tc.wantLater, list[0].Status)
			})
		})
	}
}

func TestBackend_DeleteClusterReportsDeleting(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateCluster("c", "dc2.large", "dev", "admin", nil, "", redshift.CreateClusterOptions{})
	require.NoError(t, err)

	got, err := b.DeleteCluster("c")
	require.NoError(t, err)
	assert.Equal(t, "deleting", got.Status)
}
