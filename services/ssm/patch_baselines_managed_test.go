package ssm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// aws_ssm_default_patch_baseline resets to the AWS-owned default for an OS
// via DescribePatchBaselines(OWNER=AWS, OPERATING_SYSTEM=...).
func TestDescribePatchBaselines_AWSManagedDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantName  string
		filters   []ssmtypes.PatchOrchestratorFilter
		wantCount int
	}{
		{
			name: "aws owner amazon linux 2",
			filters: []ssmtypes.PatchOrchestratorFilter{
				{Key: aws.String("OWNER"), Values: []string{"AWS"}},
				{Key: aws.String("OPERATING_SYSTEM"), Values: []string{"AMAZON_LINUX_2"}},
			},
			wantCount: 1,
			wantName:  "AWS-AmazonLinux2DefaultPatchBaseline",
		},
		{
			name: "self owner excludes managed",
			filters: []ssmtypes.PatchOrchestratorFilter{
				{Key: aws.String("OWNER"), Values: []string{"Self"}},
				{Key: aws.String("OPERATING_SYSTEM"), Values: []string{"AMAZON_LINUX_2"}},
			},
			wantCount: 0,
		},
		{
			name: "windows default",
			filters: []ssmtypes.PatchOrchestratorFilter{
				{Key: aws.String("OWNER"), Values: []string{"AWS"}},
				{Key: aws.String("OPERATING_SYSTEM"), Values: []string{"WINDOWS"}},
			},
			wantCount: 1,
			wantName:  "AWS-DefaultPatchBaseline",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSSMClient(t, ssm.NewHandler(ssm.NewInMemoryBackend()))

			out, err := client.DescribePatchBaselines(t.Context(), &ssmsdk.DescribePatchBaselinesInput{
				Filters: tt.filters,
			})
			require.NoError(t, err)
			require.Len(t, out.BaselineIdentities, tt.wantCount)

			if tt.wantCount == 0 {
				return
			}

			id := out.BaselineIdentities[0]
			assert.Equal(t, tt.wantName, aws.ToString(id.BaselineName))
			assert.True(t, id.DefaultBaseline)

			got, err := client.GetPatchBaseline(t.Context(), &ssmsdk.GetPatchBaselineInput{BaselineId: id.BaselineId})
			require.NoError(t, err)
			assert.Equal(t, tt.wantName, aws.ToString(got.Name))
		})
	}
}
