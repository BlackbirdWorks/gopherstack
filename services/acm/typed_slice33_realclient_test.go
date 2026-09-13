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

// TestSlice33_ACM_RealClient drives every gopherstack-n3zi typed-slice-33
// uncovered acm op through the real aws-sdk-go-v2 client
// (newTestACMClient, shared with wire_field_additions_test.go).
func TestSlice33_ACM_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testCertificateTagsRealClient, "certificate_tags"},
		{testGetAndExportCertificateRealClient, "get_and_export_certificate"},
		{testRenewCertificateRealClient, "renew_certificate"},
		{testResendValidationEmailRealClient, "resend_validation_email"},
		{testRevokeCertificateRealClient, "revoke_certificate"},
		{testUpdateCertificateOptionsRealClient, "update_certificate_options"},
		{testUntagResourceRealClient, "untag_resource"},
		{testAccountConfigurationRealClient, "account_configuration"},
		{testAcmeEndpointsRealClient, "acme_endpoints"},
		{testAcmeAccountsStructuralGapRealClient, "acme_accounts_structural_gap"},
		{testAcmeDomainValidationsRealClient, "acme_domain_validations"},
		{testAcmeExternalAccountBindingsRealClient, "acme_external_account_bindings"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

// testCertificateTagsRealClient covers AddTagsToCertificate,
// ListTagsForCertificate, RemoveTagsFromCertificate.
func testCertificateTagsRealClient(t *testing.T) {
	t.Helper()

	h := acm.NewHandler(acm.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestACMClient(t, h)
	ctx := t.Context()

	reqOut, err := client.RequestCertificate(ctx, &acmsdk.RequestCertificateInput{
		DomainName: aws.String("tags.example.com"),
	})
	require.NoError(t, err)
	certARN := aws.ToString(reqOut.CertificateArn)

	_, err = client.AddTagsToCertificate(ctx, &acmsdk.AddTagsToCertificateInput{
		CertificateArn: aws.String(certARN),
		Tags: []types.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
			{Key: aws.String("team"), Value: aws.String("net")},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForCertificate(ctx, &acmsdk.ListTagsForCertificateInput{
		CertificateArn: aws.String(certARN),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Tags, 2)

	_, err = client.RemoveTagsFromCertificate(ctx, &acmsdk.RemoveTagsFromCertificateInput{
		CertificateArn: aws.String(certARN),
		Tags:           []types.Tag{{Key: aws.String("team")}},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsForCertificate(ctx, &acmsdk.ListTagsForCertificateInput{
		CertificateArn: aws.String(certARN),
	})
	require.NoError(t, err)
	require.Len(t, listOut2.Tags, 1)
	assert.Equal(t, "env", aws.ToString(listOut2.Tags[0].Key))
}

// testGetAndExportCertificateRealClient covers GetCertificate and
// ExportCertificate, seeded via ImportCertificate (already typed-covered
// elsewhere) so the certificate is instantly ISSUED with a known private key.
func testGetAndExportCertificateRealClient(t *testing.T) {
	t.Helper()

	h := acm.NewHandler(acm.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestACMClient(t, h)
	ctx := t.Context()

	certPEM, keyPEM := generateTestCert(t)

	impOut, err := client.ImportCertificate(ctx, &acmsdk.ImportCertificateInput{
		Certificate: []byte(certPEM),
		PrivateKey:  []byte(keyPEM),
	})
	require.NoError(t, err)
	certARN := aws.ToString(impOut.CertificateArn)

	getOut, err := client.GetCertificate(ctx, &acmsdk.GetCertificateInput{
		CertificateArn: aws.String(certARN),
	})
	require.NoError(t, err)
	assert.Equal(t, certPEM, aws.ToString(getOut.Certificate))

	expOut, err := client.ExportCertificate(ctx, &acmsdk.ExportCertificateInput{
		CertificateArn: aws.String(certARN),
		Passphrase:     []byte("s3cr3t-passphrase"),
	})
	require.NoError(t, err)
	assert.Equal(t, certPEM, aws.ToString(expOut.Certificate))
	assert.NotEmpty(t, aws.ToString(expOut.PrivateKey))
}

// testRenewCertificateRealClient covers RenewCertificate.
func testRenewCertificateRealClient(t *testing.T) {
	t.Helper()

	h := acm.NewHandler(acm.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestACMClient(t, h)
	ctx := t.Context()

	reqOut, err := client.RequestCertificate(ctx, &acmsdk.RequestCertificateInput{
		DomainName: aws.String("renew.example.com"),
	})
	require.NoError(t, err)

	_, err = client.RenewCertificate(ctx, &acmsdk.RenewCertificateInput{
		CertificateArn: reqOut.CertificateArn,
	})
	require.NoError(t, err)

	descOut, err := client.DescribeCertificate(ctx, &acmsdk.DescribeCertificateInput{
		CertificateArn: reqOut.CertificateArn,
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Certificate.RenewalSummary)
	assert.Equal(t, types.RenewalEligibilityEligible, descOut.Certificate.RenewalEligibility)
}

// testResendValidationEmailRealClient covers ResendValidationEmail.
func testResendValidationEmailRealClient(t *testing.T) {
	t.Helper()

	h := acm.NewHandler(acm.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestACMClient(t, h)
	ctx := t.Context()

	reqOut, err := client.RequestCertificate(ctx, &acmsdk.RequestCertificateInput{
		DomainName:       aws.String("email.example.com"),
		ValidationMethod: types.ValidationMethodEmail,
	})
	require.NoError(t, err)

	_, err = client.ResendValidationEmail(ctx, &acmsdk.ResendValidationEmailInput{
		CertificateArn:   reqOut.CertificateArn,
		Domain:           aws.String("email.example.com"),
		ValidationDomain: aws.String("email.example.com"),
	})
	require.NoError(t, err)
}

// testRevokeCertificateRealClient covers RevokeCertificate.
func testRevokeCertificateRealClient(t *testing.T) {
	t.Helper()

	h := acm.NewHandler(acm.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestACMClient(t, h)
	ctx := t.Context()

	reqOut, err := client.RequestCertificate(ctx, &acmsdk.RequestCertificateInput{
		DomainName: aws.String("revoke.example.com"),
	})
	require.NoError(t, err)

	_, err = client.RevokeCertificate(ctx, &acmsdk.RevokeCertificateInput{
		CertificateArn:   reqOut.CertificateArn,
		RevocationReason: types.RevocationReasonKeyCompromise,
	})
	require.NoError(t, err)

	descOut, err := client.DescribeCertificate(ctx, &acmsdk.DescribeCertificateInput{
		CertificateArn: reqOut.CertificateArn,
	})
	require.NoError(t, err)
	assert.Equal(t, types.CertificateStatusRevoked, descOut.Certificate.Status)
	require.NotNil(t, descOut.Certificate.RevokedAt)
}

// testUpdateCertificateOptionsRealClient covers UpdateCertificateOptions.
func testUpdateCertificateOptionsRealClient(t *testing.T) {
	t.Helper()

	h := acm.NewHandler(acm.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestACMClient(t, h)
	ctx := t.Context()

	reqOut, err := client.RequestCertificate(ctx, &acmsdk.RequestCertificateInput{
		DomainName: aws.String("options.example.com"),
	})
	require.NoError(t, err)

	_, err = client.UpdateCertificateOptions(ctx, &acmsdk.UpdateCertificateOptionsInput{
		CertificateArn: reqOut.CertificateArn,
		Options: &types.CertificateOptions{
			CertificateTransparencyLoggingPreference: types.CertificateTransparencyLoggingPreferenceDisabled,
		},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeCertificate(ctx, &acmsdk.DescribeCertificateInput{
		CertificateArn: reqOut.CertificateArn,
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Certificate.Options)
	assert.Equal(
		t,
		types.CertificateTransparencyLoggingPreferenceDisabled,
		descOut.Certificate.Options.CertificateTransparencyLoggingPreference,
	)
}

// testUntagResourceRealClient covers UntagResource (the generic
// cross-resource tagging op; TagResource is already typed-covered
// elsewhere).
func testUntagResourceRealClient(t *testing.T) {
	t.Helper()

	h := acm.NewHandler(acm.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestACMClient(t, h)
	ctx := t.Context()

	reqOut, err := client.RequestCertificate(ctx, &acmsdk.RequestCertificateInput{
		DomainName: aws.String("untag.example.com"),
	})
	require.NoError(t, err)
	certARN := reqOut.CertificateArn

	_, err = client.TagResource(ctx, &acmsdk.TagResourceInput{
		ResourceArn: certARN,
		Tags:        []types.Tag{{Key: aws.String("k1"), Value: aws.String("v1")}},
	})
	require.NoError(t, err)

	_, err = client.UntagResource(ctx, &acmsdk.UntagResourceInput{
		ResourceArn: certARN,
		TagKeys:     []string{"k1"},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(ctx, &acmsdk.ListTagsForResourceInput{
		ResourceArn: certARN,
	})
	require.NoError(t, err)
	assert.Empty(t, listOut.Tags)
}

// testAccountConfigurationRealClient covers GetAccountConfiguration and
// PutAccountConfiguration.
func testAccountConfigurationRealClient(t *testing.T) {
	t.Helper()

	h := acm.NewHandler(acm.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestACMClient(t, h)
	ctx := t.Context()

	_, err := client.PutAccountConfiguration(ctx, &acmsdk.PutAccountConfigurationInput{
		IdempotencyToken: aws.String("idem-token-1"),
		ExpiryEvents:     &types.ExpiryEventsConfiguration{DaysBeforeExpiry: aws.Int32(30)},
	})
	require.NoError(t, err)

	getOut, err := client.GetAccountConfiguration(ctx, &acmsdk.GetAccountConfigurationInput{})
	require.NoError(t, err)
	require.NotNil(t, getOut.ExpiryEvents)
	require.NotNil(t, getOut.ExpiryEvents.DaysBeforeExpiry)
	assert.Equal(t, int32(30), aws.ToInt32(getOut.ExpiryEvents.DaysBeforeExpiry))
}

// testAcmeEndpointsRealClient covers ListAcmeEndpoints and
// UpdateAcmeEndpoint (CreateAcmeEndpoint/DescribeAcmeEndpoint are already
// typed-covered elsewhere, used here only to seed state).
func testAcmeEndpointsRealClient(t *testing.T) {
	t.Helper()

	h := acm.NewHandler(acm.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestACMClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateAcmeEndpoint(ctx, &acmsdk.CreateAcmeEndpointInput{
		AuthorizationBehavior: types.AcmeAuthorizationBehaviorPreApproved,
		CertificateAuthority: &types.CertificateAuthorityMemberPublicCertificateAuthority{
			Value: types.PublicCertificateAuthority{
				AllowedKeyAlgorithms: []types.PublicKeyAlgorithm{types.PublicKeyAlgorithmEcPrime256V1},
			},
		},
		Contact: types.AcmeContactNotRequired,
	})
	require.NoError(t, err)
	epARN := aws.ToString(createOut.AcmeEndpointArn)

	listOut, err := client.ListAcmeEndpoints(ctx, &acmsdk.ListAcmeEndpointsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.AcmeEndpoints, 1)
	assert.Equal(t, epARN, aws.ToString(listOut.AcmeEndpoints[0].AcmeEndpointArn))

	_, err = client.UpdateAcmeEndpoint(ctx, &acmsdk.UpdateAcmeEndpointInput{
		AcmeEndpointArn: aws.String(epARN),
		Contact:         types.AcmeContactRequired,
	})
	require.NoError(t, err)

	descOut, err := client.DescribeAcmeEndpoint(ctx, &acmsdk.DescribeAcmeEndpointInput{
		AcmeEndpointArn: aws.String(epARN),
	})
	require.NoError(t, err)
	assert.Equal(t, types.AcmeContactRequired, descOut.AcmeEndpoint.Contact)
}

// testAcmeAccountsStructuralGapRealClient covers DescribeAcmeAccount,
// ListAcmeAccounts, RevokeAcmeAccount. Per acme_accounts.go's own doc
// comment, gopherstack never populates the AcmeAccount table (a real ACME
// protocol front-end -- distinct from the ACM control-plane API this
// service emulates -- is out of scope), so this proves the real,
// non-fabricated behavior: endpoint-FK validation for real, and honest
// empty/not-found results, through the real typed client.
func testAcmeAccountsStructuralGapRealClient(t *testing.T) {
	t.Helper()

	h := acm.NewHandler(acm.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestACMClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateAcmeEndpoint(ctx, &acmsdk.CreateAcmeEndpointInput{
		AuthorizationBehavior: types.AcmeAuthorizationBehaviorPreApproved,
		CertificateAuthority: &types.CertificateAuthorityMemberPublicCertificateAuthority{
			Value: types.PublicCertificateAuthority{
				AllowedKeyAlgorithms: []types.PublicKeyAlgorithm{types.PublicKeyAlgorithmEcPrime256V1},
			},
		},
	})
	require.NoError(t, err)
	epARN := aws.ToString(createOut.AcmeEndpointArn)

	listOut, err := client.ListAcmeAccounts(ctx, &acmsdk.ListAcmeAccountsInput{
		AcmeEndpointArn: aws.String(epARN),
	})
	require.NoError(t, err)
	assert.Empty(t, listOut.AcmeAccounts)

	_, err = client.DescribeAcmeAccount(ctx, &acmsdk.DescribeAcmeAccountInput{
		AcmeEndpointArn: aws.String(epARN),
		AccountUrl:      aws.String("https://acme.example.com/account/1"),
	})
	require.Error(t, err)

	_, err = client.RevokeAcmeAccount(ctx, &acmsdk.RevokeAcmeAccountInput{
		AcmeEndpointArn: aws.String(epARN),
		AccountUrl:      aws.String("https://acme.example.com/account/1"),
	})
	require.Error(t, err)

	// Endpoint FK is validated for real: an unknown endpoint ARN also 404s.
	_, err = client.ListAcmeAccounts(ctx, &acmsdk.ListAcmeAccountsInput{
		AcmeEndpointArn: aws.String(epARN + "-does-not-exist"),
	})
	require.Error(t, err)
}

// testAcmeDomainValidationsRealClient covers DeleteAcmeDomainValidation,
// DescribeAcmeDomainValidation, ListAcmeDomainValidations,
// UpdateAcmeDomainValidation (CreateAcmeDomainValidation is already
// typed-covered elsewhere, used here only to seed state).
func testAcmeDomainValidationsRealClient(t *testing.T) {
	t.Helper()

	h := acm.NewHandler(acm.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestACMClient(t, h)
	ctx := t.Context()

	epOut, err := client.CreateAcmeEndpoint(ctx, &acmsdk.CreateAcmeEndpointInput{
		AuthorizationBehavior: types.AcmeAuthorizationBehaviorPreApproved,
		CertificateAuthority: &types.CertificateAuthorityMemberPublicCertificateAuthority{
			Value: types.PublicCertificateAuthority{
				AllowedKeyAlgorithms: []types.PublicKeyAlgorithm{types.PublicKeyAlgorithmEcPrime256V1},
			},
		},
	})
	require.NoError(t, err)
	epARN := aws.ToString(epOut.AcmeEndpointArn)

	createOut, err := client.CreateAcmeDomainValidation(ctx, &acmsdk.CreateAcmeDomainValidationInput{
		AcmeEndpointArn: aws.String(epARN),
		DomainName:      aws.String("dv.example.com"),
		PrevalidationOptions: &types.PrevalidationOptionsMemberDnsPrevalidation{
			Value: types.DnsPrevalidationOptions{
				DomainScope: &types.DomainScope{ExactDomain: types.DomainScopeOptionEnabled},
			},
		},
	})
	require.NoError(t, err)
	dvARN := aws.ToString(createOut.AcmeDomainValidationArn)

	descOut, err := client.DescribeAcmeDomainValidation(ctx, &acmsdk.DescribeAcmeDomainValidationInput{
		AcmeDomainValidationArn: aws.String(dvARN),
	})
	require.NoError(t, err)
	assert.Equal(t, "dv.example.com", aws.ToString(descOut.AcmeDomainValidation.DomainName))
	require.NotNil(t, descOut.AcmeDomainValidation.PrevalidationDetails)
	dnsDetails, ok := descOut.AcmeDomainValidation.PrevalidationDetails.(*types.PrevalidationDetailsMemberDnsPrevalidation)
	require.True(t, ok)
	require.NotNil(t, dnsDetails.Value.ResourceRecord)
	assert.NotEmpty(t, aws.ToString(dnsDetails.Value.ResourceRecord.Name))

	listOut, err := client.ListAcmeDomainValidations(ctx, &acmsdk.ListAcmeDomainValidationsInput{
		AcmeEndpointArn: aws.String(epARN),
	})
	require.NoError(t, err)
	require.Len(t, listOut.AcmeDomainValidations, 1)

	_, err = client.UpdateAcmeDomainValidation(ctx, &acmsdk.UpdateAcmeDomainValidationInput{
		AcmeDomainValidationArn: aws.String(dvARN),
		PrevalidationOptions: &types.PrevalidationOptionsMemberDnsPrevalidation{
			Value: types.DnsPrevalidationOptions{
				DomainScope: &types.DomainScope{Subdomains: types.DomainScopeOptionEnabled},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteAcmeDomainValidation(ctx, &acmsdk.DeleteAcmeDomainValidationInput{
		AcmeDomainValidationArn: aws.String(dvARN),
	})
	require.NoError(t, err)

	listOut2, err := client.ListAcmeDomainValidations(ctx, &acmsdk.ListAcmeDomainValidationsInput{
		AcmeEndpointArn: aws.String(epARN),
	})
	require.NoError(t, err)
	assert.Empty(t, listOut2.AcmeDomainValidations)
}

// testAcmeExternalAccountBindingsRealClient covers
// DeleteAcmeExternalAccountBinding, DescribeAcmeExternalAccountBinding,
// GetAcmeExternalAccountBindingCredentials,
// ListAcmeExternalAccountBindings, RevokeAcmeExternalAccountBinding
// (CreateAcmeExternalAccountBinding is already typed-covered elsewhere,
// used here only to seed state).
func testAcmeExternalAccountBindingsRealClient(t *testing.T) {
	t.Helper()

	h := acm.NewHandler(acm.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestACMClient(t, h)
	ctx := t.Context()

	epOut, err := client.CreateAcmeEndpoint(ctx, &acmsdk.CreateAcmeEndpointInput{
		AuthorizationBehavior: types.AcmeAuthorizationBehaviorPreApproved,
		CertificateAuthority: &types.CertificateAuthorityMemberPublicCertificateAuthority{
			Value: types.PublicCertificateAuthority{
				AllowedKeyAlgorithms: []types.PublicKeyAlgorithm{types.PublicKeyAlgorithmEcPrime256V1},
			},
		},
	})
	require.NoError(t, err)
	epARN := aws.ToString(epOut.AcmeEndpointArn)

	createOut, err := client.CreateAcmeExternalAccountBinding(ctx, &acmsdk.CreateAcmeExternalAccountBindingInput{
		AcmeEndpointArn: aws.String(epARN),
		RoleArn:         aws.String("arn:aws:iam::123456789012:role/acme-eab-role"),
		Expiration:      &types.Expiration{Type: types.TimeTypeDays, Value: aws.Int64(7)},
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.ExternalAccountBinding)
	eabARN := aws.ToString(createOut.ExternalAccountBinding.AcmeExternalAccountBindingArn)

	descOut, err := client.DescribeAcmeExternalAccountBinding(ctx, &acmsdk.DescribeAcmeExternalAccountBindingInput{
		AcmeExternalAccountBindingArn: aws.String(eabARN),
	})
	require.NoError(t, err)
	assert.Equal(t, epARN, aws.ToString(descOut.ExternalAccountBinding.AcmeEndpointArn))
	require.NotNil(t, descOut.ExternalAccountBinding.ExpiresAt)

	listOut, err := client.ListAcmeExternalAccountBindings(ctx, &acmsdk.ListAcmeExternalAccountBindingsInput{
		AcmeEndpointArn: aws.String(epARN),
	})
	require.NoError(t, err)
	require.Len(t, listOut.ExternalAccountBindings, 1)

	credOut, err := client.GetAcmeExternalAccountBindingCredentials(
		ctx, &acmsdk.GetAcmeExternalAccountBindingCredentialsInput{
			AcmeExternalAccountBindingArn: aws.String(eabARN),
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(credOut.KeyId))
	assert.NotEmpty(t, aws.ToString(credOut.MacKey))

	_, err = client.RevokeAcmeExternalAccountBinding(ctx, &acmsdk.RevokeAcmeExternalAccountBindingInput{
		AcmeExternalAccountBindingArn: aws.String(eabARN),
	})
	require.NoError(t, err)

	descOut2, err := client.DescribeAcmeExternalAccountBinding(ctx, &acmsdk.DescribeAcmeExternalAccountBindingInput{
		AcmeExternalAccountBindingArn: aws.String(eabARN),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut2.ExternalAccountBinding.RevokedAt)

	_, err = client.DeleteAcmeExternalAccountBinding(ctx, &acmsdk.DeleteAcmeExternalAccountBindingInput{
		AcmeExternalAccountBindingArn: aws.String(eabARN),
	})
	require.NoError(t, err)

	listOut2, err := client.ListAcmeExternalAccountBindings(ctx, &acmsdk.ListAcmeExternalAccountBindingsInput{
		AcmeEndpointArn: aws.String(epARN),
	})
	require.NoError(t, err)
	assert.Empty(t, listOut2.ExternalAccountBindings)
}
