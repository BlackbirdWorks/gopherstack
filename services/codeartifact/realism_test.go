package codeartifact_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	casdk "github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/aws/aws-sdk-go-v2/service/codeartifact/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeartifact"
)

func newRealismClient(t *testing.T) *casdk.Client {
	t.Helper()

	return newTestCodeArtifactClient(
		t,
		codeartifact.NewHandler(codeartifact.NewInMemoryBackend("000000000000", "us-east-1")),
	)
}

func TestRealism_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     func(t *testing.T, c *casdk.Client) error
		name     string
		wantCode string
	}{
		{
			name: "domain too long",
			call: func(t *testing.T, c *casdk.Client) error {
				t.Helper()
				_, err := c.CreateDomain(
					t.Context(),
					&casdk.CreateDomainInput{Domain: aws.String(strings.Repeat("a", 51))},
				)

				return err
			},
			wantCode: "ValidationException",
		},
		{
			name: "domain uppercase",
			call: func(t *testing.T, c *casdk.Client) error {
				t.Helper()
				_, err := c.CreateDomain(t.Context(), &casdk.CreateDomainInput{Domain: aws.String("Dom")})

				return err
			},
			wantCode: "ValidationException",
		},
		{
			name: "bad encryption key",
			call: func(t *testing.T, c *casdk.Client) error {
				t.Helper()
				_, err := c.CreateDomain(t.Context(), &casdk.CreateDomainInput{
					Domain: aws.String("dom2"), EncryptionKey: aws.String("arn:bad"),
				})

				return err
			},
			wantCode: "ValidationException",
		},
		{
			name: "repo bad name",
			call: func(t *testing.T, c *casdk.Client) error {
				t.Helper()
				_, err := c.CreateRepository(t.Context(), &casdk.CreateRepositoryInput{
					Domain: aws.String("dom"), Repository: aws.String("bad name"),
				})

				return err
			},
			wantCode: "ValidationException",
		},
		{
			name: "repo long description",
			call: func(t *testing.T, c *casdk.Client) error {
				t.Helper()
				_, err := c.CreateRepository(t.Context(), &casdk.CreateRepositoryInput{
					Domain: aws.String(
						"dom",
					), Repository: aws.String("r2"), Description: aws.String(strings.Repeat("d", 1001)),
				})

				return err
			},
			wantCode: "ValidationException",
		},
		{
			name: "missing upstream",
			call: func(t *testing.T, c *casdk.Client) error {
				t.Helper()
				_, err := c.CreateRepository(t.Context(), &casdk.CreateRepositoryInput{
					Domain: aws.String("dom"), Repository: aws.String("r3"),
					Upstreams: []types.UpstreamRepository{{RepositoryName: aws.String("ghost")}},
				})

				return err
			},
			wantCode: "ResourceNotFoundException",
		},
		{
			name: "unknown external connection",
			call: func(t *testing.T, c *casdk.Client) error {
				t.Helper()
				_, err := c.AssociateExternalConnection(t.Context(), &casdk.AssociateExternalConnectionInput{
					Domain: aws.String(
						"dom",
					), Repository: aws.String("r1"), ExternalConnection: aws.String("public:bogus"),
				})

				return err
			},
			wantCode: "ValidationException",
		},
		{
			name: "unknown format",
			call: func(t *testing.T, c *casdk.Client) error {
				t.Helper()
				_, err := c.GetRepositoryEndpoint(t.Context(), &casdk.GetRepositoryEndpointInput{
					Domain: aws.String("dom"), Repository: aws.String("r1"), Format: "bogus",
				})

				return err
			},
			wantCode: "ValidationException",
		},
		{
			name: "domain not found wording",
			call: func(t *testing.T, c *casdk.Client) error {
				t.Helper()
				_, err := c.DescribeDomain(t.Context(), &casdk.DescribeDomainInput{Domain: aws.String("ghost")})

				return err
			},
			wantCode: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newRealismClient(t)
			_, err := c.CreateDomain(t.Context(), &casdk.CreateDomainInput{Domain: aws.String("dom")})
			require.NoError(t, err)
			_, err = c.CreateRepository(t.Context(), &casdk.CreateRepositoryInput{
				Domain: aws.String("dom"), Repository: aws.String("r1"),
			})
			require.NoError(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, tt.call(t, c), &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.NotContains(t, apiErr.ErrorMessage(), "Exception")
		})
	}
}
