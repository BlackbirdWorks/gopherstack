package codeartifact_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	casdk "github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/aws/aws-sdk-go-v2/service/codeartifact/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeartifact"
)

func TestPublishPackageVersion_OriginRestrictions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mode     types.PackageGroupOriginRestrictionMode
		allowed  []string
		pkgBlock bool
		wantErr  bool
	}{
		{name: "group allow", mode: types.PackageGroupOriginRestrictionModeAllow},
		{name: "group block", mode: types.PackageGroupOriginRestrictionModeBlock, wantErr: true},
		{
			name: "group specific listed", mode: types.PackageGroupOriginRestrictionModeAllowSpecificRepositories,
			allowed: []string{"repo"},
		},
		{
			name: "group specific other", mode: types.PackageGroupOriginRestrictionModeAllowSpecificRepositories,
			allowed: []string{"elsewhere"}, wantErr: true,
		},
		{name: "package block", mode: types.PackageGroupOriginRestrictionModeAllow, pkgBlock: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := codeartifact.NewHandler(codeartifact.NewInMemoryBackend("123456789012", "us-east-1"))
			c := newTestCodeArtifactClient(t, h)
			ctx := t.Context()

			_, err := c.CreateDomain(ctx, &casdk.CreateDomainInput{Domain: aws.String("d")})
			require.NoError(t, err)

			for _, repo := range []string{"repo", "elsewhere"} {
				_, err = c.CreateRepository(ctx, &casdk.CreateRepositoryInput{
					Domain: aws.String("d"), Repository: aws.String(repo),
				})
				require.NoError(t, err)
			}

			publish := func(version string) error {
				_, perr := c.PublishPackageVersion(ctx, &casdk.PublishPackageVersionInput{
					Domain: aws.String("d"), Repository: aws.String("repo"), Format: types.PackageFormatNpm,
					Package: aws.String("lib"), PackageVersion: aws.String(version),
					AssetName: aws.String("lib.tgz"), AssetSHA256: aws.String(sha256Hex("x")),
					AssetContent: strings.NewReader("x"),
				})

				return perr
			}

			require.NoError(t, publish("1.0.0"))

			_, err = c.CreatePackageGroup(ctx, &casdk.CreatePackageGroupInput{
				Domain: aws.String("d"), PackageGroup: aws.String("/npm/*"),
			})
			require.NoError(t, err)

			_, err = c.UpdatePackageGroupOriginConfiguration(ctx, &casdk.UpdatePackageGroupOriginConfigurationInput{
				Domain: aws.String("d"), PackageGroup: aws.String("/npm/*"),
				Restrictions: map[string]types.PackageGroupOriginRestrictionMode{"PUBLISH": tt.mode},
			})
			require.NoError(t, err)

			for _, repo := range tt.allowed {
				_, err = c.UpdatePackageGroupOriginConfiguration(ctx, &casdk.UpdatePackageGroupOriginConfigurationInput{
					Domain: aws.String("d"), PackageGroup: aws.String("/npm/*"),
					AddAllowedRepositories: []types.PackageGroupAllowedRepository{{
						OriginRestrictionType: types.PackageGroupOriginRestrictionTypePublish,
						RepositoryName:        aws.String(repo),
					}},
				})
				require.NoError(t, err)
			}

			if tt.pkgBlock {
				_, err = c.PutPackageOriginConfiguration(ctx, &casdk.PutPackageOriginConfigurationInput{
					Domain: aws.String("d"), Repository: aws.String("repo"), Format: types.PackageFormatNpm,
					Package: aws.String("lib"), Restrictions: &types.PackageOriginRestrictions{
						Publish:  types.AllowPublishBlock,
						Upstream: types.AllowUpstreamAllow,
					},
				})
				require.NoError(t, err)
			}

			err = publish("2.0.0")
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ValidationException")
			} else {
				require.NoError(t, err)
			}

			require.NoError(t, publish("1.0.0"))
		})
	}
}
