package codeartifact_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	casdk "github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/aws/aws-sdk-go-v2/service/codeartifact/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeartifact"
)

func TestAssociateExternalConnection_StatusIsSDKEnum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		connection string
	}{
		{name: "npm", connection: "public:npmjs"},
		{name: "pypi", connection: "public:pypi"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := codeartifact.NewHandler(codeartifact.NewInMemoryBackend("123456789012", "us-east-1"))
			client := newTestCodeArtifactClient(t, h)
			ctx := t.Context()

			_, err := client.CreateDomain(ctx, &casdk.CreateDomainInput{Domain: aws.String("dom")})
			require.NoError(t, err)
			_, err = client.CreateRepository(ctx, &casdk.CreateRepositoryInput{
				Domain: aws.String("dom"), Repository: aws.String("repo"),
			})
			require.NoError(t, err)

			out, err := client.AssociateExternalConnection(ctx, &casdk.AssociateExternalConnectionInput{
				Domain:             aws.String("dom"),
				Repository:         aws.String("repo"),
				ExternalConnection: aws.String(tc.connection),
			})
			require.NoError(t, err)
			require.Len(t, out.Repository.ExternalConnections, 1)
			assert.Equal(t, types.ExternalConnectionStatusAvailable, out.Repository.ExternalConnections[0].Status)
		})
	}
}
