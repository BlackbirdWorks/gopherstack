package cleanrooms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	crtypes "github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPopulateIdMappingTable_JobType_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		jobType crtypes.JobType
		wantErr bool
	}{
		{name: "unset"},
		{name: "batch", jobType: crtypes.JobTypeBatch},
		{name: "incremental", jobType: crtypes.JobTypeIncremental},
		{name: "delete_only", jobType: crtypes.JobTypeDeleteOnly},
		{name: "unknown", jobType: crtypes.JobType("SOMETIMES"), wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newRoundTripTestClient(t)
			ctx := t.Context()
			_, memID := createCollaborationAndMembership(t, client)

			created, err := client.CreateIdMappingTable(ctx, &cleanroomssdk.CreateIdMappingTableInput{
				MembershipIdentifier: aws.String(memID),
				Name:                 aws.String("mapping-table"),
				InputReferenceConfig: &crtypes.IdMappingTableInputReferenceConfig{
					InputReferenceArn: aws.String(
						"arn:aws:entityresolution:us-east-1:123456789012:idmappingworkflow/f",
					),
					ManageResourcePolicies: aws.Bool(true),
				},
			})
			require.NoError(t, err)

			out, err := client.PopulateIdMappingTable(ctx, &cleanroomssdk.PopulateIdMappingTableInput{
				MembershipIdentifier:     aws.String(memID),
				IdMappingTableIdentifier: created.IdMappingTable.Id,
				JobType:                  tc.jobType,
			})
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(out.IdMappingJobId))
		})
	}
}
