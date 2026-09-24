package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestRealClient_DescribeLaunchTemplatesFilters covers DescribeLaunchTemplates,
// which previously ignored Filters entirely.
func TestRealClient_DescribeLaunchTemplatesFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	web, err := client.CreateLaunchTemplate(t.Context(), &ec2sdk.CreateLaunchTemplateInput{
		LaunchTemplateName: aws.String("web-template"),
		LaunchTemplateData: &types.RequestLaunchTemplateData{ImageId: aws.String("ami-web")},
		TagSpecifications: []types.TagSpecification{{
			ResourceType: types.ResourceTypeLaunchTemplate,
			Tags:         []types.Tag{{Key: aws.String("Team"), Value: aws.String("infra")}},
		}},
	})
	require.NoError(t, err)
	_, err = client.CreateLaunchTemplate(t.Context(), &ec2sdk.CreateLaunchTemplateInput{
		LaunchTemplateName: aws.String("db-template"),
		LaunchTemplateData: &types.RequestLaunchTemplateData{ImageId: aws.String("ami-db")},
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "launch-template-name",
			filters: []types.Filter{{Name: aws.String("launch-template-name"), Values: []string{"web-template"}}},
			want:    []string{"web-template"},
		},
		{
			name:    "tag key/value",
			filters: []types.Filter{{Name: aws.String("tag:Team"), Values: []string{"infra"}}},
			want:    []string{"web-template"},
		},
		{
			name:    "tag-key",
			filters: []types.Filter{{Name: aws.String("tag-key"), Values: []string{"Team"}}},
			want:    []string{"web-template"},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("launch-template-name"), Values: []string{"missing"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeLaunchTemplates(
				t.Context(), &ec2sdk.DescribeLaunchTemplatesInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.LaunchTemplates))
			for _, lt := range out.LaunchTemplates {
				got = append(got, aws.ToString(lt.LaunchTemplateName))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}

	require.NotEmpty(t, aws.ToString(web.LaunchTemplate.LaunchTemplateId))
}
