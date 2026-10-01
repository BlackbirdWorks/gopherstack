package codeartifact_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	casdk "github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/aws/aws-sdk-go-v2/service/codeartifact/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribe_UnpublishedIsNotFound_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(c *casdk.Client, h string) error
		name string
	}{
		{
			name: "package",
			call: func(c *casdk.Client, _ string) error {
				_, err := c.DescribePackage(t.Context(), &casdk.DescribePackageInput{
					Domain: aws.String("nf-domain"), Repository: aws.String("nf-repo"),
					Format: types.PackageFormatNpm, Package: aws.String("ghost"),
				})

				return err
			},
		},
		{
			name: "version",
			call: func(c *casdk.Client, _ string) error {
				_, err := c.DescribePackageVersion(t.Context(), &casdk.DescribePackageVersionInput{
					Domain: aws.String("nf-domain"), Repository: aws.String("nf-repo"),
					Format: types.PackageFormatNpm, Package: aws.String("ghost"),
					PackageVersion: aws.String("1.0.0"),
				})

				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupDomain(t, h, "nf-domain")
			setupRepo(t, h, "nf-domain", "nf-repo")
			client := newTestCodeArtifactClient(t, h)

			var nf *types.ResourceNotFoundException
			require.ErrorAs(t, tt.call(client, ""), &nf)

			list, err := client.ListPackages(t.Context(), &casdk.ListPackagesInput{
				Domain: aws.String("nf-domain"), Repository: aws.String("nf-repo"),
			})
			require.NoError(t, err)
			assert.Empty(t, list.Packages, "a failed describe must not create state")
		})
	}
}
