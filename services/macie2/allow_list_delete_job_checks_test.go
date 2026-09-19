package macie2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"
	"github.com/aws/aws-sdk-go-v2/service/macie2/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// DeleteAllowListInput.IgnoreJobChecks (macie2@v1.54.4: "Amazon Macie checks
// for classification jobs that use the list and have a status other than
// COMPLETE or CANCELLED. By default, Macie rejects your request if any jobs
// meet these criteria.") was parsed nowhere -- a delete always succeeded
// even when a running job still referenced the allow list.
func TestDeleteAllowList_IgnoreJobChecks_RealClient(t *testing.T) {
	t.Parallel()

	newAllowListInUse := func(t *testing.T) (*macie2sdk.Client, string) {
		t.Helper()

		_, client := newMacie2Client(t)
		ctx := t.Context()

		created, err := client.CreateAllowList(ctx, &macie2sdk.CreateAllowListInput{
			Name:     aws.String("in-use-allow-list"),
			Criteria: &types.AllowListCriteria{Regex: aws.String("skip-.*")},
		})
		require.NoError(t, err)

		_, err = client.CreateClassificationJob(ctx, &macie2sdk.CreateClassificationJobInput{
			Name:    aws.String("job-using-allow-list"),
			JobType: types.JobTypeOneTime,
			S3JobDefinition: &types.S3JobDefinition{
				BucketDefinitions: []types.S3BucketDefinitionForJob{},
			},
			AllowListIds: []string{aws.ToString(created.Id)},
		})
		require.NoError(t, err)

		return client, aws.ToString(created.Id)
	}

	t.Run("rejected by default while a non-terminal job references it", func(t *testing.T) {
		t.Parallel()

		client, id := newAllowListInUse(t)

		_, err := client.DeleteAllowList(t.Context(), &macie2sdk.DeleteAllowListInput{Id: aws.String(id)})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, "ConflictException", apiErr.ErrorCode())

		_, getErr := client.GetAllowList(t.Context(), &macie2sdk.GetAllowListInput{Id: aws.String(id)})
		require.NoError(t, getErr, "the allow list must survive a rejected delete")
	})

	t.Run("IgnoreJobChecks true skips the check", func(t *testing.T) {
		t.Parallel()

		client, id := newAllowListInUse(t)

		_, err := client.DeleteAllowList(t.Context(), &macie2sdk.DeleteAllowListInput{
			Id:              aws.String(id),
			IgnoreJobChecks: aws.String("true"),
		})
		require.NoError(t, err)

		_, getErr := client.GetAllowList(t.Context(), &macie2sdk.GetAllowListInput{Id: aws.String(id)})
		require.Error(t, getErr, "the allow list must actually be gone")
	})
}
