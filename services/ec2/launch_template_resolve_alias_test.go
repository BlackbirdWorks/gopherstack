package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/blackbirdworks/gopherstack/services/ssm"
)

type ssmSiblingConfig struct{ h *ssm.Handler }

func (c ssmSiblingConfig) GetSSMHandler() service.Registerable { return c.h }

func TestRealClient_LaunchTemplateResolveAlias(t *testing.T) {
	t.Parallel()

	const (
		aliasOK      = "resolve:ssm:/golden/ami/latest"
		aliasMissing = "resolve:ssm:/golden/ami/missing"
	)

	tests := []struct {
		name         string
		imageID      string
		wantImageID  string
		wantErrCodes string
		resolve      bool
		wireSSM      bool
	}{
		{"alias_kept_by_default", aliasOK, aliasOK, "", false, true},
		{"alias_resolved", aliasOK, "ami-0123456789abcdef0", "", true, true},
		{"plain_id_untouched", "ami-plain", "ami-plain", "", true, true},
		{"missing_parameter", aliasMissing, "", "InvalidParameterValue", true, true},
		{"ssm_unavailable", aliasOK, "", "InvalidParameterValue", true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend(tailAcct, "us-east-1")

			if tt.wireSSM {
				ssmBackend := ssm.NewInMemoryBackend()
				_, err := ssmBackend.PutParameter(t.Context(), &ssm.PutParameterInput{
					Name: "/golden/ami/latest", Type: "String", Value: "ami-0123456789abcdef0",
				})
				require.NoError(t, err)
				b.SetAppConfig(ssmSiblingConfig{h: ssm.NewHandler(ssmBackend)})
			}

			client := newTestEC2Client(t, ec2.NewHandler(b))

			lt, err := client.CreateLaunchTemplate(t.Context(), &ec2sdk.CreateLaunchTemplateInput{
				LaunchTemplateName: aws.String("lt"),
				LaunchTemplateData: &types.RequestLaunchTemplateData{ImageId: aws.String("ami-seed")},
			})
			require.NoError(t, err)

			created, err := client.CreateLaunchTemplateVersion(t.Context(), &ec2sdk.CreateLaunchTemplateVersionInput{
				LaunchTemplateId:   lt.LaunchTemplate.LaunchTemplateId,
				LaunchTemplateData: &types.RequestLaunchTemplateData{ImageId: aws.String(tt.imageID)},
				ResolveAlias:       aws.Bool(tt.resolve),
			})
			if tt.wantErrCodes != "" {
				require.ErrorContains(t, err, tt.wantErrCodes)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantImageID, aws.ToString(created.LaunchTemplateVersion.LaunchTemplateData.ImageId))

			desc, err := client.DescribeLaunchTemplateVersions(t.Context(), &ec2sdk.DescribeLaunchTemplateVersionsInput{
				LaunchTemplateId: lt.LaunchTemplate.LaunchTemplateId,
				ResolveAlias:     aws.Bool(tt.resolve),
			})
			require.NoError(t, err)
			require.NotEmpty(t, desc.LaunchTemplateVersions)
			assert.Equal(t, tt.wantImageID, aws.ToString(desc.LaunchTemplateVersions[0].LaunchTemplateData.ImageId))
		})
	}
}
