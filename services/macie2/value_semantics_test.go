package macie2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"
	"github.com/aws/aws-sdk-go-v2/service/macie2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindingsFilter_PartialUpdateKeepsDescription(t *testing.T) {
	t.Parallel()

	cases := []struct {
		desc     *string
		name     string
		wantDesc string
	}{
		{name: "omitted", wantDesc: "orig"},
		{name: "explicit", desc: aws.String("new"), wantDesc: "new"},
		{name: "cleared", desc: aws.String(""), wantDesc: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, c := newMacie2Client(t)
			created, err := c.CreateFindingsFilter(t.Context(), &macie2sdk.CreateFindingsFilterInput{
				Name: aws.String("filter-one"), Description: aws.String("orig"), Action: types.FindingsFilterActionNoop,
				FindingCriteria: &types.FindingCriteria{},
			})
			require.NoError(t, err)

			_, err = c.UpdateFindingsFilter(t.Context(), &macie2sdk.UpdateFindingsFilterInput{
				Id: created.Id, Action: types.FindingsFilterActionArchive, Description: tc.desc,
			})
			require.NoError(t, err)

			got, err := c.GetFindingsFilter(t.Context(), &macie2sdk.GetFindingsFilterInput{Id: created.Id})
			require.NoError(t, err)
			assert.Equal(t, tc.wantDesc, aws.ToString(got.Description))
			assert.Equal(t, types.FindingsFilterActionArchive, got.Action)
			assert.Equal(t, "filter-one", aws.ToString(got.Name))
		})
	}
}

func TestClassificationJob_ManagedIdentifierSelectorDefaultsRecommended(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sel  types.ManagedDataIdentifierSelector
		want types.ManagedDataIdentifierSelector
	}{
		{name: "omitted", want: types.ManagedDataIdentifierSelectorRecommended},
		{name: "explicit", sel: types.ManagedDataIdentifierSelectorAll, want: types.ManagedDataIdentifierSelectorAll},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, c := newMacie2Client(t)
			created, err := c.CreateClassificationJob(t.Context(), &macie2sdk.CreateClassificationJobInput{
				Name:    aws.String("job"),
				JobType: types.JobTypeOneTime,
				S3JobDefinition: &types.S3JobDefinition{
					BucketDefinitions: []types.S3BucketDefinitionForJob{},
				},
				ManagedDataIdentifierSelector: tc.sel,
			})
			require.NoError(t, err)

			got, err := c.DescribeClassificationJob(
				t.Context(),
				&macie2sdk.DescribeClassificationJobInput{JobId: created.JobId},
			)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got.ManagedDataIdentifierSelector)
		})
	}
}
