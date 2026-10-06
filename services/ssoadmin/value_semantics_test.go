package ssoadmin_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssoadminsdk "github.com/aws/aws-sdk-go-v2/service/ssoadmin"
	ssoadmintypes "github.com/aws/aws-sdk-go-v2/service/ssoadmin/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplication_CreateStatusPersistsAndPartialUpdateKeepsIt(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		create     ssoadmintypes.ApplicationStatus
		update     ssoadmintypes.ApplicationStatus
		wantCreate ssoadmintypes.ApplicationStatus
		wantUpdate ssoadmintypes.ApplicationStatus
	}{
		{
			name:       "omitted defaults enabled",
			wantCreate: ssoadmintypes.ApplicationStatusEnabled,
			wantUpdate: ssoadmintypes.ApplicationStatusEnabled,
		},
		{
			name:       "disabled persists across description update",
			create:     ssoadmintypes.ApplicationStatusDisabled,
			wantCreate: ssoadmintypes.ApplicationStatusDisabled,
			wantUpdate: ssoadmintypes.ApplicationStatusDisabled,
		},
		{
			name:       "update flips status",
			create:     ssoadmintypes.ApplicationStatusDisabled,
			update:     ssoadmintypes.ApplicationStatusEnabled,
			wantCreate: ssoadmintypes.ApplicationStatusDisabled,
			wantUpdate: ssoadmintypes.ApplicationStatusEnabled,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newRealClient(t)

			inst, err := c.CreateInstance(t.Context(), &ssoadminsdk.CreateInstanceInput{Name: aws.String("i")})
			require.NoError(t, err)

			app, err := c.CreateApplication(t.Context(), &ssoadminsdk.CreateApplicationInput{
				InstanceArn:            inst.InstanceArn,
				ApplicationProviderArn: aws.String("arn:aws:sso::aws:applicationProvider/custom"),
				Name:                   aws.String("a"),
				Status:                 tc.create,
			})
			require.NoError(t, err)

			got, err := c.DescribeApplication(t.Context(), &ssoadminsdk.DescribeApplicationInput{
				ApplicationArn: app.ApplicationArn,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantCreate, got.Status)

			_, err = c.UpdateApplication(t.Context(), &ssoadminsdk.UpdateApplicationInput{
				ApplicationArn: app.ApplicationArn, Description: aws.String("d"), Status: tc.update,
			})
			require.NoError(t, err)

			got, err = c.DescribeApplication(t.Context(), &ssoadminsdk.DescribeApplicationInput{
				ApplicationArn: app.ApplicationArn,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantUpdate, got.Status)
			assert.Equal(t, "d", aws.ToString(got.Description))
		})
	}
}
