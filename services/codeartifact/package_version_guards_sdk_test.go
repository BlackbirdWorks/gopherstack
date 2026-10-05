package codeartifact_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	casdk "github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/aws/aws-sdk-go-v2/service/codeartifact/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type (
	okMap   = map[string]types.SuccessfulPackageVersionInfo
	failMap = map[string]types.PackageVersionError
)

func TestCodeArtifactSDK_BulkVersionGuards(t *testing.T) {
	t.Parallel()

	const (
		domain = "guard-domain"
		pkg    = "react"
	)

	tests := []struct {
		call     func(t *testing.T, c *casdk.Client, rev string) (okMap, failMap, error)
		name     string
		wantCode string
		wantOK   bool
	}{
		{
			name: "delete matching expected status",
			call: func(t *testing.T, c *casdk.Client, _ string) (okMap, failMap, error) {
				t.Helper()
				o, err := c.DeletePackageVersions(t.Context(), &casdk.DeletePackageVersionsInput{
					Domain: aws.String(domain), Repository: aws.String("src"), Format: "npm", Package: aws.String(pkg),
					Versions: []string{"1.0.0"}, ExpectedStatus: types.PackageVersionStatusPublished,
				})
				if err != nil {
					return nil, nil, err
				}

				return o.SuccessfulVersions, o.FailedVersions, nil
			},
			wantOK: true,
		},
		{
			name: "delete mismatched status",
			call: func(t *testing.T, c *casdk.Client, _ string) (okMap, failMap, error) {
				t.Helper()
				o, err := c.DeletePackageVersions(t.Context(), &casdk.DeletePackageVersionsInput{
					Domain: aws.String(domain), Repository: aws.String("src"), Format: "npm", Package: aws.String(pkg),
					Versions: []string{"1.0.0"}, ExpectedStatus: types.PackageVersionStatusArchived,
				})
				if err != nil {
					return nil, nil, err
				}

				return o.SuccessfulVersions, o.FailedVersions, nil
			},
			wantCode: "MISMATCHED_STATUS",
		},
		{
			name: "dispose mismatched revision",
			call: func(t *testing.T, c *casdk.Client, _ string) (okMap, failMap, error) {
				t.Helper()
				o, err := c.DisposePackageVersions(t.Context(), &casdk.DisposePackageVersionsInput{
					Domain: aws.String(domain), Repository: aws.String("src"), Format: "npm", Package: aws.String(pkg),
					Versions: []string{"1.0.0"}, VersionRevisions: map[string]string{"1.0.0": "not-the-revision"},
				})
				if err != nil {
					return nil, nil, err
				}

				return o.SuccessfulVersions, o.FailedVersions, nil
			},
			wantCode: "MISMATCHED_REVISION",
		},
		{
			name: "dispose matching revision",
			call: func(t *testing.T, c *casdk.Client, rev string) (okMap, failMap, error) {
				t.Helper()
				o, err := c.DisposePackageVersions(t.Context(), &casdk.DisposePackageVersionsInput{
					Domain: aws.String(domain), Repository: aws.String("src"), Format: "npm", Package: aws.String(pkg),
					Versions: []string{"1.0.0"}, VersionRevisions: map[string]string{"1.0.0": rev},
				})
				if err != nil {
					return nil, nil, err
				}

				return o.SuccessfulVersions, o.FailedVersions, nil
			},
			wantOK: true,
		},
		{
			name: "update status mismatched expected status",
			call: func(t *testing.T, c *casdk.Client, _ string) (okMap, failMap, error) {
				t.Helper()
				o, err := c.UpdatePackageVersionsStatus(t.Context(), &casdk.UpdatePackageVersionsStatusInput{
					Domain: aws.String(domain), Repository: aws.String("src"), Format: "npm", Package: aws.String(pkg),
					Versions: []string{"1.0.0"}, TargetStatus: types.PackageVersionStatusArchived,
					ExpectedStatus: types.PackageVersionStatusUnlisted,
				})
				if err != nil {
					return nil, nil, err
				}

				return o.SuccessfulVersions, o.FailedVersions, nil
			},
			wantCode: "MISMATCHED_STATUS",
		},
		{
			name: "update status by revision",
			call: func(t *testing.T, c *casdk.Client, rev string) (okMap, failMap, error) {
				t.Helper()
				o, err := c.UpdatePackageVersionsStatus(t.Context(), &casdk.UpdatePackageVersionsStatusInput{
					Domain: aws.String(domain), Repository: aws.String("src"), Format: "npm", Package: aws.String(pkg),
					Versions: []string{"1.0.0"}, VersionRevisions: map[string]string{"1.0.0": rev},
					TargetStatus:   types.PackageVersionStatusArchived,
					ExpectedStatus: types.PackageVersionStatusPublished,
				})
				if err != nil {
					return nil, nil, err
				}

				return o.SuccessfulVersions, o.FailedVersions, nil
			},
			wantOK: true,
		},
		{
			name: "copy mismatched revision",
			call: func(t *testing.T, c *casdk.Client, _ string) (okMap, failMap, error) {
				t.Helper()
				o, err := c.CopyPackageVersions(t.Context(), &casdk.CopyPackageVersionsInput{
					Domain: aws.String(domain), SourceRepository: aws.String("src"),
					DestinationRepository: aws.String("dst"), Format: "npm", Package: aws.String(pkg),
					VersionRevisions: map[string]string{"1.0.0": "stale"},
				})
				if err != nil {
					return nil, nil, err
				}

				return o.SuccessfulVersions, o.FailedVersions, nil
			},
			wantCode: "MISMATCHED_REVISION",
		},
		{
			name: "copy by revision",
			call: func(t *testing.T, c *casdk.Client, rev string) (okMap, failMap, error) {
				t.Helper()
				o, err := c.CopyPackageVersions(t.Context(), &casdk.CopyPackageVersionsInput{
					Domain: aws.String(domain), SourceRepository: aws.String("src"),
					DestinationRepository: aws.String("dst"), Format: "npm", Package: aws.String(pkg),
					VersionRevisions: map[string]string{"1.0.0": rev},
				})
				if err != nil {
					return nil, nil, err
				}

				return o.SuccessfulVersions, o.FailedVersions, nil
			},
			wantOK: true,
		},
		{
			name: "copy both versions and revisions",
			call: func(t *testing.T, c *casdk.Client, rev string) (okMap, failMap, error) {
				t.Helper()
				_, err := c.CopyPackageVersions(t.Context(), &casdk.CopyPackageVersionsInput{
					Domain: aws.String(domain), SourceRepository: aws.String("src"),
					DestinationRepository: aws.String("dst"), Format: "npm", Package: aws.String(pkg),
					Versions: []string{"1.0.0"}, VersionRevisions: map[string]string{"1.0.0": rev},
				})

				return nil, nil, err
			},
			wantCode: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupDomain(t, h, domain)
			setupRepo(t, h, domain, "src")
			setupRepo(t, h, domain, "dst")
			seedVersion(t, h, domain, "src", "npm", "", pkg, "1.0.0")
			client := newTestCodeArtifactClient(t, h)

			desc, err := client.DescribePackageVersion(t.Context(), &casdk.DescribePackageVersionInput{
				Domain: aws.String(domain), Repository: aws.String("src"), Format: "npm",
				Package: aws.String(pkg), PackageVersion: aws.String("1.0.0"),
			})
			require.NoError(t, err)

			okVersions, failedVersions, err := tt.call(t, client, aws.ToString(desc.PackageVersion.Revision))
			if tt.wantCode == "ValidationException" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantCode)

				return
			}
			require.NoError(t, err)
			if tt.wantOK {
				assert.Contains(t, okVersions, "1.0.0")
				assert.Empty(t, failedVersions)

				return
			}
			assert.Empty(t, okVersions)
			require.Contains(t, failedVersions, "1.0.0")
			assert.Equal(t, tt.wantCode, string(failedVersions["1.0.0"].ErrorCode))
		})
	}
}

func TestCodeArtifactSDK_CopyAllowOverwrite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantCode  string
		overwrite bool
	}{
		{name: "rejected by default", wantCode: "ALREADY_EXISTS"},
		{name: "allowed", overwrite: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupDomain(t, h, "ow-domain")
			setupRepo(t, h, "ow-domain", "src")
			setupRepo(t, h, "ow-domain", "dst")
			seedVersion(t, h, "ow-domain", "src", "npm", "", "react", "1.0.0")
			seedVersion(t, h, "ow-domain", "dst", "npm", "", "react", "1.0.0")
			client := newTestCodeArtifactClient(t, h)

			out, err := client.CopyPackageVersions(t.Context(), &casdk.CopyPackageVersionsInput{
				Domain: aws.String("ow-domain"), SourceRepository: aws.String("src"),
				DestinationRepository: aws.String("dst"), Format: "npm", Package: aws.String("react"),
				Versions: []string{"1.0.0"}, AllowOverwrite: aws.Bool(tt.overwrite),
			})
			require.NoError(t, err)
			if tt.wantCode != "" {
				require.Contains(t, out.FailedVersions, "1.0.0")
				assert.Equal(t, tt.wantCode, string(out.FailedVersions["1.0.0"].ErrorCode))

				return
			}
			assert.Contains(t, out.SuccessfulVersions, "1.0.0")
			assert.Empty(t, out.FailedVersions)
		})
	}
}
