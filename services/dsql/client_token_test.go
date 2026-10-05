package dsql_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dsqlsdk "github.com/aws/aws-sdk-go-v2/service/dsql"
	"github.com/aws/aws-sdk-go-v2/service/dsql/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientTokenReplay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *dsqlsdk.Client)
		name string
	}{
		{
			name: "create_cluster_replays_and_rejects_changed_params",
			run: func(t *testing.T, client *dsqlsdk.Client) {
				t.Helper()

				in := &dsqlsdk.CreateClusterInput{ClientToken: aws.String("tok-c")}
				first, err := client.CreateCluster(t.Context(), in)
				require.NoError(t, err)

				again, err := client.CreateCluster(t.Context(), in)
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(first.Identifier), aws.ToString(again.Identifier))

				_, err = client.CreateCluster(t.Context(), &dsqlsdk.CreateClusterInput{
					ClientToken: aws.String("tok-c"), DeletionProtectionEnabled: aws.Bool(true),
				})
				require.Error(t, err)
				assertAPIErrorCode(t, err, "ConflictException")

				other, err := client.CreateCluster(t.Context(), &dsqlsdk.CreateClusterInput{
					ClientToken: aws.String("tok-d"),
				})
				require.NoError(t, err)
				assert.NotEqual(t, aws.ToString(first.Identifier), aws.ToString(other.Identifier))
			},
		},
		{
			name: "create_stream_replays",
			run: func(t *testing.T, client *dsqlsdk.Client) {
				t.Helper()

				cluster, err := client.CreateCluster(t.Context(), &dsqlsdk.CreateClusterInput{})
				require.NoError(t, err)

				in := &dsqlsdk.CreateStreamInput{
					ClusterIdentifier: cluster.Identifier,
					TargetDefinition:  testKinesisTarget(),
					Format:            types.StreamFormatJson,
					Ordering:          types.StreamOrderingUnordered,
					ClientToken:       aws.String("tok-s"),
				}
				first, err := client.CreateStream(t.Context(), in)
				require.NoError(t, err)

				again, err := client.CreateStream(t.Context(), in)
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(first.StreamIdentifier), aws.ToString(again.StreamIdentifier))
			},
		},
		{
			name: "delete_cluster_replays_after_removal",
			run: func(t *testing.T, client *dsqlsdk.Client) {
				t.Helper()

				cluster, err := client.CreateCluster(t.Context(), &dsqlsdk.CreateClusterInput{})
				require.NoError(t, err)

				in := &dsqlsdk.DeleteClusterInput{Identifier: cluster.Identifier, ClientToken: aws.String("tok-del")}
				_, err = client.DeleteCluster(t.Context(), in)
				require.NoError(t, err)

				require.Eventually(t, func() bool {
					_, getErr := client.GetCluster(
						t.Context(), &dsqlsdk.GetClusterInput{Identifier: cluster.Identifier},
					)

					var apiErr smithy.APIError

					return errors.As(getErr, &apiErr) && apiErr.ErrorCode() == "ResourceNotFoundException"
				}, waitTimeout, pollInterval)

				replayed, err := client.DeleteCluster(t.Context(), in)
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(cluster.Identifier), aws.ToString(replayed.Identifier))

				_, err = client.DeleteCluster(t.Context(), &dsqlsdk.DeleteClusterInput{
					Identifier: cluster.Identifier, ClientToken: aws.String("tok-other"),
				})
				assertAPIErrorCode(t, err, "ResourceNotFoundException")
			},
		},
		{
			name: "put_cluster_policy_replays",
			run: func(t *testing.T, client *dsqlsdk.Client) {
				t.Helper()

				cluster, err := client.CreateCluster(t.Context(), &dsqlsdk.CreateClusterInput{})
				require.NoError(t, err)

				in := &dsqlsdk.PutClusterPolicyInput{
					Identifier: cluster.Identifier, Policy: aws.String(testPolicyDoc), ClientToken: aws.String("tok-p"),
				}
				first, err := client.PutClusterPolicy(t.Context(), in)
				require.NoError(t, err)

				again, err := client.PutClusterPolicy(t.Context(), in)
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(first.PolicyVersion), aws.ToString(again.PolicyVersion))

				fresh, err := client.PutClusterPolicy(t.Context(), &dsqlsdk.PutClusterPolicyInput{
					Identifier:  cluster.Identifier,
					Policy:      aws.String(testPolicyDoc),
					ClientToken: aws.String("tok-p2"),
				})
				require.NoError(t, err)
				assert.NotEqual(t, aws.ToString(first.PolicyVersion), aws.ToString(fresh.PolicyVersion))
			},
		},
		{
			name: "update_cluster_replays",
			run: func(t *testing.T, client *dsqlsdk.Client) {
				t.Helper()

				cluster, err := client.CreateCluster(t.Context(), &dsqlsdk.CreateClusterInput{})
				require.NoError(t, err)

				in := &dsqlsdk.UpdateClusterInput{
					Identifier:                cluster.Identifier,
					DeletionProtectionEnabled: aws.Bool(true),
					ClientToken:               aws.String("tok-u"),
				}
				_, err = client.UpdateCluster(t.Context(), in)
				require.NoError(t, err)

				_, err = client.UpdateCluster(t.Context(), in)
				require.NoError(t, err)

				_, err = client.UpdateCluster(t.Context(), &dsqlsdk.UpdateClusterInput{
					Identifier:                cluster.Identifier,
					DeletionProtectionEnabled: aws.Bool(false),
					ClientToken:               aws.String("tok-u"),
				})
				assertAPIErrorCode(t, err, "ConflictException")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newTestClient(t, newTestHandler()))
		})
	}
}
