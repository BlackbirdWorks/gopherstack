package cloudfront_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

func TestManagedCertificateRequest_TokenHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createHost types.ValidationTokenHost
		updateHost types.ValidationTokenHost
		wantCreate types.ValidationTokenHost
		wantUpdate types.ValidationTokenHost
	}{
		{
			name: "self_hosted_then_cloudfront", createHost: types.ValidationTokenHostSelfHosted,
			updateHost: types.ValidationTokenHostCloudFront,
			wantCreate: types.ValidationTokenHostSelfHosted, wantUpdate: types.ValidationTokenHostCloudFront,
		},
		{
			name: "default_then_self_hosted", updateHost: types.ValidationTokenHostSelfHosted,
			wantCreate: types.ValidationTokenHostCloudFront, wantUpdate: types.ValidationTokenHostSelfHosted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
			t.Cleanup(backend.Close)
			client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

			in := &cfsdk.CreateDistributionTenantInput{
				DistributionId: aws.String("dist-1"),
				Name:           aws.String("t1"),
				Domains:        []types.DomainItem{{Domain: aws.String("mcr.example.com")}},
			}
			if tt.createHost != "" {
				in.ManagedCertificateRequest = &types.ManagedCertificateRequest{
					ValidationTokenHost: tt.createHost,
					PrimaryDomainName:   aws.String("mcr.example.com"),
				}
			}

			created, err := client.CreateDistributionTenant(t.Context(), in)
			require.NoError(t, err)
			id := created.DistributionTenant.Id

			got, err := client.GetManagedCertificateDetails(
				t.Context(), &cfsdk.GetManagedCertificateDetailsInput{Identifier: id},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCreate, got.ManagedCertificateDetails.ValidationTokenHost)

			_, err = client.UpdateDistributionTenant(t.Context(), &cfsdk.UpdateDistributionTenantInput{
				Id:      id,
				IfMatch: created.ETag,
				ManagedCertificateRequest: &types.ManagedCertificateRequest{
					ValidationTokenHost: tt.updateHost,
				},
			})
			require.NoError(t, err)

			got, err = client.GetManagedCertificateDetails(
				t.Context(), &cfsdk.GetManagedCertificateDetailsInput{Identifier: id},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantUpdate, got.ManagedCertificateDetails.ValidationTokenHost)
		})
	}
}

func TestManagedCertificateRequest_InvalidHostRejected(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	t.Cleanup(backend.Close)
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	_, err := client.CreateDistributionTenant(t.Context(), &cfsdk.CreateDistributionTenantInput{
		DistributionId:            aws.String("dist-1"),
		Name:                      aws.String("t1"),
		Domains:                   []types.DomainItem{{Domain: aws.String("bad.example.com")}},
		ManagedCertificateRequest: &types.ManagedCertificateRequest{ValidationTokenHost: "bogus"},
	})
	require.Error(t, err)
}
