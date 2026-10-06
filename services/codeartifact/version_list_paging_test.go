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

const pagingPackageJSON = `{"name":"p","version":"1.0.0","dependencies":{"lodash":"^4"},` +
	`"devDependencies":{"jest":"^29"},"peerDependencies":{"react":">=16"},"optionalDependencies":{"fsevents":"^2"}}`

// TestListPackageVersion_Paging covers max-results/next-token on assets and next-token on dependencies.
func TestListPackageVersion_Paging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		list   func(t *testing.T, c *casdk.Client, size int32, token *string) (int, *string)
		name   string
		format types.PackageFormat
		resume string
		assets []string
		want   []int
		size   int32
	}{
		{
			name: "assets", format: types.PackageFormatGeneric, assets: []string{"c.bin", "a.bin", "b.bin"},
			size: 2, want: []int{2, 1},
			list: func(t *testing.T, c *casdk.Client, size int32, token *string) (int, *string) {
				t.Helper()

				out, err := c.ListPackageVersionAssets(t.Context(), &casdk.ListPackageVersionAssetsInput{
					Domain: aws.String(
						"pg-domain",
					),
					Repository:     aws.String("pg-repo"),
					Format:         types.PackageFormatGeneric,
					Package:        aws.String("p"),
					PackageVersion: aws.String("1.0.0"),
					Namespace:      aws.String("ns"),
					MaxResults:     aws.Int32(size),
					NextToken:      token,
				})
				require.NoError(t, err)

				return len(out.Assets), out.NextToken
			},
		},
		{
			name: "dependencies", format: types.PackageFormatNpm, assets: []string{"package.json"},
			resume: "peer//react", want: []int{4, 2},
			list: func(t *testing.T, c *casdk.Client, _ int32, token *string) (int, *string) {
				t.Helper()

				out, err := c.ListPackageVersionDependencies(t.Context(), &casdk.ListPackageVersionDependenciesInput{
					Domain: aws.String("pg-domain"), Repository: aws.String("pg-repo"), Format: types.PackageFormatNpm,
					Package: aws.String("p"), PackageVersion: aws.String("1.0.0"), NextToken: token,
				})
				require.NoError(t, err)

				return len(out.Dependencies), out.NextToken
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCodeArtifactClient(
				t,
				codeartifact.NewHandler(codeartifact.NewInMemoryBackend("123456789012", "us-east-1")),
			)

			_, err := client.CreateDomain(t.Context(), &casdk.CreateDomainInput{Domain: aws.String("pg-domain")})
			require.NoError(t, err)

			_, err = client.CreateRepository(t.Context(), &casdk.CreateRepositoryInput{
				Domain: aws.String("pg-domain"), Repository: aws.String("pg-repo"),
			})
			require.NoError(t, err)

			for _, a := range tt.assets {
				content := "x-" + a
				if a == "package.json" {
					content = pagingPackageJSON
				}

				in := &casdk.PublishPackageVersionInput{
					Domain: aws.String("pg-domain"), Repository: aws.String("pg-repo"), Format: tt.format,
					Package: aws.String("p"), PackageVersion: aws.String("1.0.0"), AssetName: aws.String(a),
					AssetSHA256: aws.String(sha256Hex(content)), AssetContent: strings.NewReader(content),
				}
				if tt.format == types.PackageFormatGeneric {
					in.Namespace = aws.String("ns")
				}

				_, err = client.PublishPackageVersion(t.Context(), in)
				require.NoError(t, err)
			}

			var (
				token *string
				got   []int
			)

			for range 4 {
				n, next := tt.list(t, client, tt.size, token)

				got = append(got, n)
				if token = next; token == nil {
					break
				}
			}

			if tt.resume != "" {
				n, _ := tt.list(t, client, 0, aws.String(tt.resume))
				got = append(got[:1], n)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
