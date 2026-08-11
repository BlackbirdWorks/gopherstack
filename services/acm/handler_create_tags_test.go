package acm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	acmsdk "github.com/aws/aws-sdk-go-v2/service/acm"
	acmtypes "github.com/aws/aws-sdk-go-v2/service/acm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acm"
)

// TestCreateOpsWithTags_RoundTrip drives every acm Create op whose real Input
// struct accepts Tags (acm@v1.43.4: api_op_RequestCertificate.go:163,
// api_op_ImportCertificate.go:103, api_op_CreateAcmeDomainValidation.go:53,
// api_op_CreateAcmeEndpoint.go:56, api_op_CreateAcmeExternalAccountBinding.go:53)
// through the real SDK client and asserts ListTagsForResource sees what was
// supplied at creation (gopherstack-2mwl). ACM dispatches tags at the handler
// level (h.tags, handler_tags.go/handler_resource_tags.go), not the backend
// -- certificates, ACME endpoints, ACME domain validations, and ACME external
// account bindings all share one ARN-keyed store, verified by
// resolveTaggableResourceArn (handler_resource_tags.go).
//
// Before the fix, ImportCertificate's wire handler (importCertificateInput,
// handler_certificates.go) decoded no Tags field at all -- a pure
// decode-drop, the same shape as elasticache's six creates -- even though
// RequestCertificate's identical Tags shape worked correctly and fed the same
// h.setTags call ListTagsForResource reads.
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *acmsdk.Client) string
		name  string
	}{
		{
			name: "certificate (request)",
			setup: func(t *testing.T, client *acmsdk.Client) string {
				t.Helper()

				out, err := client.RequestCertificate(t.Context(), &acmsdk.RequestCertificateInput{
					DomainName: aws.String("tagged-request.example.com"),
					Tags:       []acmtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.CertificateArn)
			},
		},
		{
			name: "certificate (import)",
			setup: func(t *testing.T, client *acmsdk.Client) string {
				t.Helper()

				certPEM, keyPEM := generateTestCert(t)

				out, err := client.ImportCertificate(t.Context(), &acmsdk.ImportCertificateInput{
					Certificate: []byte(certPEM),
					PrivateKey:  []byte(keyPEM),
					Tags:        []acmtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.CertificateArn)
			},
		},
		{
			name: "acme domain validation",
			setup: func(t *testing.T, client *acmsdk.Client) string {
				t.Helper()

				ep, err := client.CreateAcmeEndpoint(t.Context(), &acmsdk.CreateAcmeEndpointInput{
					AuthorizationBehavior: acmtypes.AcmeAuthorizationBehaviorPreApproved,
					CertificateAuthority: &acmtypes.CertificateAuthorityMemberPublicCertificateAuthority{
						Value: acmtypes.PublicCertificateAuthority{},
					},
				})
				require.NoError(t, err)

				out, err := client.CreateAcmeDomainValidation(
					t.Context(),
					&acmsdk.CreateAcmeDomainValidationInput{
						AcmeEndpointArn: ep.AcmeEndpointArn,
						DomainName:      aws.String("tagged-dv.example.com"),
						PrevalidationOptions: &acmtypes.PrevalidationOptionsMemberDnsPrevalidation{
							Value: acmtypes.DnsPrevalidationOptions{},
						},
						Tags: []acmtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
					},
				)
				require.NoError(t, err)

				return aws.ToString(out.AcmeDomainValidationArn)
			},
		},
		{
			name: "acme endpoint",
			setup: func(t *testing.T, client *acmsdk.Client) string {
				t.Helper()

				out, err := client.CreateAcmeEndpoint(t.Context(), &acmsdk.CreateAcmeEndpointInput{
					AuthorizationBehavior: acmtypes.AcmeAuthorizationBehaviorPreApproved,
					CertificateAuthority: &acmtypes.CertificateAuthorityMemberPublicCertificateAuthority{
						Value: acmtypes.PublicCertificateAuthority{},
					},
					Tags: []acmtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
				})
				require.NoError(t, err)

				return aws.ToString(out.AcmeEndpointArn)
			},
		},
		{
			name: "acme external account binding",
			setup: func(t *testing.T, client *acmsdk.Client) string {
				t.Helper()

				ep, err := client.CreateAcmeEndpoint(t.Context(), &acmsdk.CreateAcmeEndpointInput{
					AuthorizationBehavior: acmtypes.AcmeAuthorizationBehaviorPreApproved,
					CertificateAuthority: &acmtypes.CertificateAuthorityMemberPublicCertificateAuthority{
						Value: acmtypes.PublicCertificateAuthority{},
					},
				})
				require.NoError(t, err)

				out, err := client.CreateAcmeExternalAccountBinding(
					t.Context(),
					&acmsdk.CreateAcmeExternalAccountBindingInput{
						AcmeEndpointArn: ep.AcmeEndpointArn,
						RoleArn:         aws.String("arn:aws:iam::000000000000:role/acme"),
						Tags:            []acmtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
					},
				)
				require.NoError(t, err)

				return aws.ToString(out.ExternalAccountBinding.AcmeExternalAccountBindingArn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := acm.NewInMemoryBackend("000000000000", wireTestRegion)
			client := newTestACMClient(t, acm.NewHandler(backend))

			resourceArn := tt.setup(t, client)
			require.NotEmpty(t, resourceArn)

			got, err := client.ListTagsForResource(t.Context(), &acmsdk.ListTagsForResourceInput{
				ResourceArn: aws.String(resourceArn),
			})
			require.NoError(t, err)
			require.Len(t, got.Tags, 1)
			assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
			assert.Equal(t, "test", aws.ToString(got.Tags[0].Value))
		})
	}
}
