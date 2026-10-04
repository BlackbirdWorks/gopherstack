package codeartifact_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	casdk "github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/aws/aws-sdk-go-v2/service/codeartifact/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopyPackageVersions_IncludeFromUpstream_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		include     *bool
		name        string
		wantSuccess bool
	}{
		{name: "unset_misses_upstream", include: nil, wantSuccess: false},
		{name: "false_misses_upstream", include: aws.Bool(false), wantSuccess: false},
		{name: "true_copies_from_upstream", include: aws.Bool(true), wantSuccess: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupDomain(t, h, "up-domain")
			setupRepo(t, h, "up-domain", "up-root")
			client := newTestCodeArtifactClient(t, h)
			ctx := t.Context()

			_, err := client.CreateRepository(ctx, &casdk.CreateRepositoryInput{
				Domain: aws.String("up-domain"), Repository: aws.String("up-mid"),
				Upstreams: []types.UpstreamRepository{{RepositoryName: aws.String("up-root")}},
			})
			require.NoError(t, err)
			_, err = client.CreateRepository(ctx, &casdk.CreateRepositoryInput{
				Domain: aws.String("up-domain"), Repository: aws.String("up-src"),
				Upstreams: []types.UpstreamRepository{{RepositoryName: aws.String("up-mid")}},
			})
			require.NoError(t, err)
			setupRepo(t, h, "up-domain", "up-dst")
			seedVersion(t, h, "up-domain", "up-root", "npm", "", "react", "18.0.0")

			out, err := client.CopyPackageVersions(ctx, &casdk.CopyPackageVersionsInput{
				Domain:                aws.String("up-domain"),
				SourceRepository:      aws.String("up-src"),
				DestinationRepository: aws.String("up-dst"),
				Format:                "npm",
				Package:               aws.String("react"),
				Versions:              []string{"18.0.0"},
				IncludeFromUpstream:   tc.include,
			})
			require.NoError(t, err)

			listed, err := client.ListPackageVersions(ctx, &casdk.ListPackageVersionsInput{
				Domain: aws.String("up-domain"), Repository: aws.String("up-dst"),
				Format: "npm", Package: aws.String("react"),
			})
			if tc.wantSuccess {
				require.Contains(t, out.SuccessfulVersions, "18.0.0")
				require.NoError(t, err)
				assert.Len(t, listed.Versions, 1)

				return
			}

			require.Contains(t, out.FailedVersions, "18.0.0")
			assert.Equal(t, types.PackageVersionErrorCodeNotFound, out.FailedVersions["18.0.0"].ErrorCode)
		})
	}
}
