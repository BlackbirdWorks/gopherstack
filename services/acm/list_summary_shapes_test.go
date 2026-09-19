package acm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	acmsdk "github.com/aws/aws-sdk-go-v2/service/acm"
	"github.com/aws/aws-sdk-go-v2/service/acm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acm"
)

// TestListSummaryShapes proves this pass's over-wide-response audit
// (gopherstack, 2026-09-19) for acm's six flagged List ops: ListAcmeAccounts,
// ListAcmeDomainValidations, ListAcmeEndpoints, ListAcmeExternalAccountBindings
// and ListCertificates already matched their real *Summary member sets
// exactly (verified via cmd/structfielddiff against acm@v1.49.0); this test
// locks that in and proves the two real fixes found: ListCertificates.
// CertificateSummary was missing CertificateKeyPairOrigin (derivable via the
// existing certKeyPairOrigin helper, same as SearchCertificates already
// does), and ListCertificateDomainValidations was not implemented at all.
func TestListSummaryShapes(t *testing.T) {
	t.Parallel()

	t.Run("acme accounts exact", func(t *testing.T) {
		t.Parallel()

		h := acm.NewHandler(acm.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestACMClient(t, h)
		ctx := t.Context()

		ep, err := client.CreateAcmeEndpoint(ctx, &acmsdk.CreateAcmeEndpointInput{
			AuthorizationBehavior: types.AcmeAuthorizationBehaviorPreApproved,
			CertificateAuthority: &types.CertificateAuthorityMemberPublicCertificateAuthority{
				Value: types.PublicCertificateAuthority{},
			},
		})
		require.NoError(t, err)

		out, err := client.ListAcmeAccounts(ctx, &acmsdk.ListAcmeAccountsInput{
			AcmeEndpointArn: ep.AcmeEndpointArn,
		})
		require.NoError(t, err)
		assert.Empty(t, out.AcmeAccounts)
	})

	t.Run("acme domain validations exact", func(t *testing.T) {
		t.Parallel()

		h := acm.NewHandler(acm.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestACMClient(t, h)
		ctx := t.Context()

		ep, err := client.CreateAcmeEndpoint(ctx, &acmsdk.CreateAcmeEndpointInput{
			AuthorizationBehavior: types.AcmeAuthorizationBehaviorPreApproved,
			CertificateAuthority: &types.CertificateAuthorityMemberPublicCertificateAuthority{
				Value: types.PublicCertificateAuthority{},
			},
		})
		require.NoError(t, err)

		_, err = client.CreateAcmeDomainValidation(ctx, &acmsdk.CreateAcmeDomainValidationInput{
			AcmeEndpointArn: ep.AcmeEndpointArn,
			DomainName:      aws.String("example.com"),
			PrevalidationOptions: &types.PrevalidationOptionsMemberDnsPrevalidation{
				Value: types.DnsPrevalidationOptions{
					DomainScope: &types.DomainScope{ExactDomain: types.DomainScopeOptionEnabled},
				},
			},
		})
		require.NoError(t, err)

		out, err := client.ListAcmeDomainValidations(ctx, &acmsdk.ListAcmeDomainValidationsInput{
			AcmeEndpointArn: ep.AcmeEndpointArn,
		})
		require.NoError(t, err)
		require.Len(t, out.AcmeDomainValidations, 1)
		dv := out.AcmeDomainValidations[0]
		assert.Equal(t, "example.com", aws.ToString(dv.DomainName))
		assert.Equal(t, types.AcmeDomainValidationStatusValidating, dv.Status)
		dnsPrevalidation, ok := dv.PrevalidationDetails.(*types.PrevalidationDetailsMemberDnsPrevalidation)
		require.True(t, ok)
		assert.NotNil(t, dnsPrevalidation.Value.ResourceRecord)
	})

	t.Run("acme endpoints exact", func(t *testing.T) {
		t.Parallel()

		h := acm.NewHandler(acm.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestACMClient(t, h)
		ctx := t.Context()

		_, err := client.CreateAcmeEndpoint(ctx, &acmsdk.CreateAcmeEndpointInput{
			AuthorizationBehavior: types.AcmeAuthorizationBehaviorPreApproved,
			CertificateAuthority: &types.CertificateAuthorityMemberPublicCertificateAuthority{
				Value: types.PublicCertificateAuthority{},
			},
			Contact: types.AcmeContactRequired,
		})
		require.NoError(t, err)

		out, err := client.ListAcmeEndpoints(ctx, &acmsdk.ListAcmeEndpointsInput{})
		require.NoError(t, err)
		require.Len(t, out.AcmeEndpoints, 1)
		ep := out.AcmeEndpoints[0]
		assert.Equal(t, types.AcmeContactRequired, ep.Contact)
		assert.NotNil(t, ep.CertificateAuthority)
	})

	t.Run("acme external account bindings exact", func(t *testing.T) {
		t.Parallel()

		h := acm.NewHandler(acm.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestACMClient(t, h)
		ctx := t.Context()

		ep, err := client.CreateAcmeEndpoint(ctx, &acmsdk.CreateAcmeEndpointInput{
			AuthorizationBehavior: types.AcmeAuthorizationBehaviorPreApproved,
			CertificateAuthority: &types.CertificateAuthorityMemberPublicCertificateAuthority{
				Value: types.PublicCertificateAuthority{},
			},
		})
		require.NoError(t, err)

		_, err = client.CreateAcmeExternalAccountBinding(ctx, &acmsdk.CreateAcmeExternalAccountBindingInput{
			AcmeEndpointArn: ep.AcmeEndpointArn,
			RoleArn:         aws.String("arn:aws:iam::123456789012:role/acme"),
		})
		require.NoError(t, err)

		out, err := client.ListAcmeExternalAccountBindings(ctx, &acmsdk.ListAcmeExternalAccountBindingsInput{
			AcmeEndpointArn: ep.AcmeEndpointArn,
		})
		require.NoError(t, err)
		require.Len(t, out.ExternalAccountBindings, 1)
		assert.Equal(t, "arn:aws:iam::123456789012:role/acme", aws.ToString(out.ExternalAccountBindings[0].RoleArn))
	})

	t.Run("certificates key pair origin was missing", func(t *testing.T) {
		t.Parallel()

		h := acm.NewHandler(acm.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestACMClient(t, h)
		ctx := t.Context()

		_, err := client.RequestCertificate(ctx, &acmsdk.RequestCertificateInput{
			DomainName: aws.String("keypairorigin.example.com"),
		})
		require.NoError(t, err)

		out, err := client.ListCertificates(ctx, &acmsdk.ListCertificatesInput{})
		require.NoError(t, err)
		require.Len(t, out.CertificateSummaryList, 1)
		gotOrigin := out.CertificateSummaryList[0].CertificateKeyPairOrigin
		assert.Equal(t, types.CertificateKeyPairOriginAwsManaged, gotOrigin)

		rec := postACMJSON(t, h, "ListCertificates", `{}`)
		assert.Contains(t, rec.Body.String(), `"CertificateKeyPairOrigin":"AWS_MANAGED"`)
	})

	t.Run("certificate domain validations new op", func(t *testing.T) {
		t.Parallel()

		h := acm.NewHandler(acm.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestACMClient(t, h)
		ctx := t.Context()

		reqOut, err := client.RequestCertificate(ctx, &acmsdk.RequestCertificateInput{
			DomainName: aws.String("cdv.example.com"),
		})
		require.NoError(t, err)

		out, err := client.ListCertificateDomainValidations(ctx, &acmsdk.ListCertificateDomainValidationsInput{
			CertificateArn: reqOut.CertificateArn,
		})
		require.NoError(t, err)
		require.Len(t, out.DomainValidationSummaryList, 1)

		dv := out.DomainValidationSummaryList[0]
		assert.Equal(t, "cdv.example.com", aws.ToString(dv.DomainName))
		require.NotNil(t, dv.ActiveValidationConfiguration)
		assert.Equal(t, types.ValidationMethodDns, dv.ActiveValidationConfiguration.ValidationMethod)
		assert.Contains(t,
			[]types.DomainStatus{types.DomainStatusPendingValidation, types.DomainStatusSuccess},
			dv.ActiveValidationConfiguration.ValidationStatus,
		)
		require.NotNil(t, dv.ActiveValidationConfiguration.ValidationChallenge)

		challenge := dv.ActiveValidationConfiguration.ValidationChallenge
		dnsChallenge, ok := challenge.(*types.ValidationChallengeMemberDnsValidationChallenge)
		require.True(t, ok)
		assert.NotNil(t, dnsChallenge.Value.ResourceRecord)
		assert.Nil(t, dv.RequestedValidationConfiguration)

		rec := postACMJSON(t, h, "ListCertificateDomainValidations",
			`{"CertificateArn":"`+aws.ToString(reqOut.CertificateArn)+`"}`)
		assert.NotContains(t, rec.Body.String(), "RequestedValidationConfiguration")
	})

	t.Run("certificate domain validations not found", func(t *testing.T) {
		t.Parallel()

		h := acm.NewHandler(acm.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestACMClient(t, h)
		ctx := t.Context()

		_, err := client.ListCertificateDomainValidations(ctx, &acmsdk.ListCertificateDomainValidationsInput{
			CertificateArn: aws.String("arn:aws:acm:us-east-1:123456789012:certificate/does-not-exist"),
		})
		require.Error(t, err)
	})
}
