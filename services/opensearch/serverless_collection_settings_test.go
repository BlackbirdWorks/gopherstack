package opensearch_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/opensearchserverless"
	aosstypes "github.com/aws/aws-sdk-go-v2/service/opensearchserverless/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

func TestServerlessCollection_Settings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, c *opensearchserverless.Client)
		name string
	}{
		{name: "create_round_trips_members", run: func(t *testing.T, c *opensearchserverless.Client) {
			t.Helper()

			out, err := c.CreateCollection(t.Context(), &opensearchserverless.CreateCollectionInput{
				Name:               aws.String("s1"),
				DeletionProtection: aosstypes.DeletionProtectionEnabled,
				StandbyReplicas:    aosstypes.StandbyReplicasDisabled,
				VectorOptions: &aosstypes.VectorOptions{
					ServerlessVectorAcceleration: aosstypes.ServerlessVectorAccelerationStatusAllowed,
				},
			})
			require.NoError(t, err)
			d := out.CreateCollectionDetail
			assert.Equal(t, aosstypes.DeletionProtectionEnabled, d.DeletionProtection)
			assert.Equal(t, aosstypes.StandbyReplicasDisabled, d.StandbyReplicas)
			assert.Equal(
				t,
				aosstypes.ServerlessVectorAccelerationStatusAllowed,
				d.VectorOptions.ServerlessVectorAcceleration,
			)

			got, err := c.BatchGetCollection(t.Context(), &opensearchserverless.BatchGetCollectionInput{
				Ids: []string{aws.ToString(d.Id)},
			})
			require.NoError(t, err)
			require.Len(t, got.CollectionDetails, 1)
			assert.Equal(t, aosstypes.DeletionProtectionEnabled, got.CollectionDetails[0].DeletionProtection)
		}},
		{
			name: "update_applies_members_and_protection_blocks_delete",
			run: func(t *testing.T, c *opensearchserverless.Client) {
				t.Helper()

				out, err := c.CreateCollection(
					t.Context(),
					&opensearchserverless.CreateCollectionInput{Name: aws.String("s2")},
				)
				require.NoError(t, err)
				id := out.CreateCollectionDetail.Id

				up, err := c.UpdateCollection(t.Context(), &opensearchserverless.UpdateCollectionInput{
					Id:                 id,
					DeletionProtection: aosstypes.DeletionProtectionEnabled,
					VectorOptions: &aosstypes.VectorOptions{
						ServerlessVectorAcceleration: aosstypes.ServerlessVectorAccelerationStatusEnabled,
					},
				})
				require.NoError(t, err)
				assert.Equal(t, aosstypes.DeletionProtectionEnabled, up.UpdateCollectionDetail.DeletionProtection)
				assert.Equal(t, aosstypes.ServerlessVectorAccelerationStatusEnabled,
					up.UpdateCollectionDetail.VectorOptions.ServerlessVectorAcceleration)

				_, err = c.DeleteCollection(t.Context(), &opensearchserverless.DeleteCollectionInput{Id: id})
				require.Error(t, err)
				assert.Equal(t, "ConflictException", apiCode(err))

				_, err = c.UpdateCollection(t.Context(), &opensearchserverless.UpdateCollectionInput{
					Id: id, DeletionProtection: aosstypes.DeletionProtectionDisabled,
				})
				require.NoError(t, err)
				_, err = c.DeleteCollection(t.Context(), &opensearchserverless.DeleteCollectionInput{Id: id})
				require.NoError(t, err)
			},
		},
		{name: "client_token_replays_and_mismatch_conflicts", run: func(t *testing.T, c *opensearchserverless.Client) {
			t.Helper()

			in := &opensearchserverless.CreateCollectionInput{Name: aws.String("s3"), ClientToken: aws.String("tok-1")}
			first, err := c.CreateCollection(t.Context(), in)
			require.NoError(t, err)
			again, err := c.CreateCollection(t.Context(), in)
			require.NoError(t, err)
			assert.Equal(t, first.CreateCollectionDetail.Id, again.CreateCollectionDetail.Id)

			_, err = c.CreateCollection(t.Context(), &opensearchserverless.CreateCollectionInput{
				Name: aws.String("s3-other"), ClientToken: aws.String("tok-1"),
			})
			require.Error(t, err)
			assert.Equal(t, "ConflictException", apiCode(err))
		}},
		{name: "duplicate_name_conflicts", run: func(t *testing.T, c *opensearchserverless.Client) {
			t.Helper()

			_, err := c.CreateCollection(
				t.Context(),
				&opensearchserverless.CreateCollectionInput{Name: aws.String("s4")},
			)
			require.NoError(t, err)
			_, err = c.CreateCollection(
				t.Context(),
				&opensearchserverless.CreateCollectionInput{Name: aws.String("s4")},
			)
			require.Error(t, err)
			assert.Equal(t, "ConflictException", apiCode(err))
		}},
		{name: "invalid_enum_rejected", run: func(t *testing.T, c *opensearchserverless.Client) {
			t.Helper()

			_, err := c.CreateCollection(t.Context(), &opensearchserverless.CreateCollectionInput{
				Name: aws.String("s5"), DeletionProtection: aosstypes.DeletionProtection("MAYBE"),
			})
			require.Error(t, err)
			assert.Equal(t, "ValidationException", apiCode(err))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newTestServerlessClient(t, testServerlessHandler(t)))
		})
	}
}
