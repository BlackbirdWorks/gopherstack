package acmpca_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	acmpcasdk "github.com/aws/aws-sdk-go-v2/service/acmpca"
	acmpcatypes "github.com/aws/aws-sdk-go-v2/service/acmpca/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

// createTestRootCA creates a ROOT CA through the real client and returns its
// ARN. This backend auto-self-signs and activates ROOT CAs on creation (see
// certificate_authorities.go's newCertificateAuthorityLocked doc comment),
// so no separate GetCertificateAuthorityCsr/ImportCertificateAuthorityCertificate
// round trip is needed to reach ACTIVE.
func createTestRootCA(t *testing.T, client *acmpcasdk.Client, commonName string) string {
	t.Helper()

	out, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
		CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeRoot,
		CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
			KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
			SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
			Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String(commonName)},
		},
	})
	require.NoError(t, err)

	return aws.ToString(out.CertificateAuthorityArn)
}

// TestTypedSlice23RealClient drives acmpca's remaining typed-coverage-blind
// ops (gopherstack-n3zi slice 23) through the real aws-sdk-go-v2 client:
// CreateCertificateAuthorityAuditReport, CreatePermission,
// DeleteCertificateAuthority, DeletePermission, DeletePolicy,
// DescribeCertificateAuthorityAuditReport, GetCertificateAuthorityCertificate,
// GetCertificateAuthorityCsr, GetPolicy, ListPermissions, ListTags, PutPolicy,
// RestoreCertificateAuthority, TagCertificateAuthority,
// UntagCertificateAuthority.
func TestTypedSlice23RealClient(t *testing.T) {
	t.Parallel()

	t.Run("audit report lifecycle", func(t *testing.T) {
		t.Parallel()

		backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
		client := newTestACMPCASDKClient(t, acmpca.NewHandler(backend))
		caArn := createTestRootCA(t, client, "Audit Report CA")

		createOut, err := client.CreateCertificateAuthorityAuditReport(t.Context(),
			&acmpcasdk.CreateCertificateAuthorityAuditReportInput{
				CertificateAuthorityArn:   aws.String(caArn),
				S3BucketName:              aws.String("s23-audit-bucket"),
				AuditReportResponseFormat: acmpcatypes.AuditReportResponseFormatJson,
			},
		)
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(createOut.AuditReportId))
		require.NotEmpty(t, aws.ToString(createOut.S3Key))

		descOut, err := client.DescribeCertificateAuthorityAuditReport(t.Context(),
			&acmpcasdk.DescribeCertificateAuthorityAuditReportInput{
				CertificateAuthorityArn: aws.String(caArn),
				AuditReportId:           createOut.AuditReportId,
			},
		)
		require.NoError(t, err)
		assert.Equal(t, "s23-audit-bucket", aws.ToString(descOut.S3BucketName))
		assert.Equal(t, aws.ToString(createOut.S3Key), aws.ToString(descOut.S3Key))
		assert.Equal(t, acmpcatypes.AuditReportStatusSuccess, descOut.AuditReportStatus)
	})

	t.Run("CA certificate and CSR retrieval", func(t *testing.T) {
		t.Parallel()

		backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
		client := newTestACMPCASDKClient(t, acmpca.NewHandler(backend))
		caArn := createTestRootCA(t, client, "Cert Retrieval CA")

		csrOut, err := client.GetCertificateAuthorityCsr(t.Context(), &acmpcasdk.GetCertificateAuthorityCsrInput{
			CertificateAuthorityArn: aws.String(caArn),
		})
		require.NoError(t, err)
		assert.Contains(t, aws.ToString(csrOut.Csr), "CERTIFICATE REQUEST")

		certOut, err := client.GetCertificateAuthorityCertificate(t.Context(),
			&acmpcasdk.GetCertificateAuthorityCertificateInput{CertificateAuthorityArn: aws.String(caArn)},
		)
		require.NoError(t, err)
		assert.Contains(t, aws.ToString(certOut.Certificate), "CERTIFICATE")
	})

	t.Run("permissions CRUD", func(t *testing.T) {
		t.Parallel()

		backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
		client := newTestACMPCASDKClient(t, acmpca.NewHandler(backend))
		caArn := createTestRootCA(t, client, "Permissions CA")

		_, err := client.CreatePermission(t.Context(), &acmpcasdk.CreatePermissionInput{
			CertificateAuthorityArn: aws.String(caArn),
			Principal:               aws.String("acm.amazonaws.com"),
			SourceAccount:           aws.String(testAccountID),
			Actions: []acmpcatypes.ActionType{
				acmpcatypes.ActionTypeIssueCertificate,
				acmpcatypes.ActionTypeGetCertificate,
			},
		})
		require.NoError(t, err)

		listOut, err := client.ListPermissions(t.Context(), &acmpcasdk.ListPermissionsInput{
			CertificateAuthorityArn: aws.String(caArn),
		})
		require.NoError(t, err)
		require.Len(t, listOut.Permissions, 1)
		assert.Equal(t, "acm.amazonaws.com", aws.ToString(listOut.Permissions[0].Principal))
		assert.ElementsMatch(
			t,
			[]acmpcatypes.ActionType{acmpcatypes.ActionTypeIssueCertificate, acmpcatypes.ActionTypeGetCertificate},
			listOut.Permissions[0].Actions,
		)

		_, err = client.DeletePermission(t.Context(), &acmpcasdk.DeletePermissionInput{
			CertificateAuthorityArn: aws.String(caArn),
			Principal:               aws.String("acm.amazonaws.com"),
			SourceAccount:           aws.String(testAccountID),
		})
		require.NoError(t, err)

		listAfterDelete, err := client.ListPermissions(t.Context(), &acmpcasdk.ListPermissionsInput{
			CertificateAuthorityArn: aws.String(caArn),
		})
		require.NoError(t, err)
		assert.Empty(t, listAfterDelete.Permissions)
	})

	t.Run("resource policy CRUD", func(t *testing.T) {
		t.Parallel()

		backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
		client := newTestACMPCASDKClient(t, acmpca.NewHandler(backend))
		caArn := createTestRootCA(t, client, "Policy CA")

		policyDoc := `{"Version":"2012-10-17","Statement":[]}`

		_, err := client.PutPolicy(t.Context(), &acmpcasdk.PutPolicyInput{
			ResourceArn: aws.String(caArn),
			Policy:      aws.String(policyDoc),
		})
		require.NoError(t, err)

		getOut, err := client.GetPolicy(t.Context(), &acmpcasdk.GetPolicyInput{ResourceArn: aws.String(caArn)})
		require.NoError(t, err)
		assert.Equal(t, policyDoc, aws.ToString(getOut.Policy))

		_, err = client.DeletePolicy(t.Context(), &acmpcasdk.DeletePolicyInput{ResourceArn: aws.String(caArn)})
		require.NoError(t, err)

		_, err = client.GetPolicy(t.Context(), &acmpcasdk.GetPolicyInput{ResourceArn: aws.String(caArn)})
		require.Error(t, err, "policy no longer exists after delete")
	})

	t.Run("CA tags", func(t *testing.T) {
		t.Parallel()

		backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
		client := newTestACMPCASDKClient(t, acmpca.NewHandler(backend))
		caArn := createTestRootCA(t, client, "Tags CA")

		_, err := client.TagCertificateAuthority(t.Context(), &acmpcasdk.TagCertificateAuthorityInput{
			CertificateAuthorityArn: aws.String(caArn),
			Tags:                    []acmpcatypes.Tag{{Key: aws.String("team"), Value: aws.String("pki")}},
		})
		require.NoError(t, err)

		listOut, err := client.ListTags(
			t.Context(),
			&acmpcasdk.ListTagsInput{CertificateAuthorityArn: aws.String(caArn)},
		)
		require.NoError(t, err)
		require.Len(t, listOut.Tags, 1)
		assert.Equal(t, "team", aws.ToString(listOut.Tags[0].Key))
		assert.Equal(t, "pki", aws.ToString(listOut.Tags[0].Value))

		_, err = client.UntagCertificateAuthority(t.Context(), &acmpcasdk.UntagCertificateAuthorityInput{
			CertificateAuthorityArn: aws.String(caArn),
			Tags:                    []acmpcatypes.Tag{{Key: aws.String("team"), Value: aws.String("pki")}},
		})
		require.NoError(t, err)

		listAfterUntag, err := client.ListTags(
			t.Context(),
			&acmpcasdk.ListTagsInput{CertificateAuthorityArn: aws.String(caArn)},
		)
		require.NoError(t, err)
		assert.Empty(t, listAfterUntag.Tags)
	})

	t.Run("delete and restore lifecycle", func(t *testing.T) {
		t.Parallel()

		backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
		client := newTestACMPCASDKClient(t, acmpca.NewHandler(backend))
		caArn := createTestRootCA(t, client, "Delete Restore CA")

		_, err := client.UpdateCertificateAuthority(t.Context(), &acmpcasdk.UpdateCertificateAuthorityInput{
			CertificateAuthorityArn: aws.String(caArn),
			Status:                  acmpcatypes.CertificateAuthorityStatusDisabled,
		})
		require.NoError(t, err)

		_, err = client.DeleteCertificateAuthority(t.Context(), &acmpcasdk.DeleteCertificateAuthorityInput{
			CertificateAuthorityArn: aws.String(caArn),
		})
		require.NoError(t, err)

		descAfterDelete, err := client.DescribeCertificateAuthority(t.Context(),
			&acmpcasdk.DescribeCertificateAuthorityInput{CertificateAuthorityArn: aws.String(caArn)},
		)
		require.NoError(t, err)
		assert.Equal(t, acmpcatypes.CertificateAuthorityStatusDeleted, descAfterDelete.CertificateAuthority.Status)

		_, err = client.RestoreCertificateAuthority(t.Context(), &acmpcasdk.RestoreCertificateAuthorityInput{
			CertificateAuthorityArn: aws.String(caArn),
		})
		require.NoError(t, err)

		descAfterRestore, err := client.DescribeCertificateAuthority(t.Context(),
			&acmpcasdk.DescribeCertificateAuthorityInput{CertificateAuthorityArn: aws.String(caArn)},
		)
		require.NoError(t, err)
		assert.Equal(t, acmpcatypes.CertificateAuthorityStatusDisabled, descAfterRestore.CertificateAuthority.Status)
	})
}
