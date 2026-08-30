package acmpca_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	acmpcasdk "github.com/aws/aws-sdk-go-v2/service/acmpca"
	acmpcatypes "github.com/aws/aws-sdk-go-v2/service/acmpca/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

// TestCreateCertificateAuthority_InvalidKeyStorageStandard_RealClient drives
// CreateCertificateAuthority through the real client with an out-of-enum
// KeyStorageSecurityStandard. gopherstack previously emitted
// "InvalidParameterException" here (gopherstack-r3pr) -- no acm-pca
// operation's deserializeOpError models that literal (confirmed by grepping
// every awsAwsjson11_deserializeOpError* switch in
// aws-sdk-go-v2/service/acmpca@v1.50.0/deserializers.go). CreateCertificateAuthority's
// own switch (awsAwsjson11_deserializeOpErrorCreateCertificateAuthority) models
// InvalidArgsException, InvalidPolicyException, InvalidTagException,
// LimitExceededException -- InvalidArgsException is the correct code for an
// invalid argument value.
func TestCreateCertificateAuthority_InvalidKeyStorageStandard_RealClient(t *testing.T) {
	t.Parallel()

	backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	h := acmpca.NewHandler(backend)
	client := newTestACMPCASDKClient(t, h)

	_, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
		CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeRoot,
		CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
			KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
			SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
			Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("Bad Standard CA")},
		},
		KeyStorageSecurityStandard: "NOT_A_REAL_STANDARD",
	})
	require.Error(t, err)

	var ia *acmpcatypes.InvalidArgsException
	require.ErrorAs(t, err, &ia, "expected a real InvalidArgsException from the SDK deserializer")
}

// TestUpdateCertificateAuthority_InvalidStatus_RealClient drives
// UpdateCertificateAuthority through the real client with a Status value
// outside {ACTIVE, DISABLED}. Same fabricated-code bug as above;
// UpdateCertificateAuthority's own deserializer
// (awsAwsjson11_deserializeOpErrorUpdateCertificateAuthority) models
// InvalidArgsException among its errors.
func TestUpdateCertificateAuthority_InvalidStatus_RealClient(t *testing.T) {
	t.Parallel()

	backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	h := acmpca.NewHandler(backend)
	client := newTestACMPCASDKClient(t, h)

	created, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
		CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeRoot,
		CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
			KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
			SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
			Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("Update Me CA")},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateCertificateAuthority(t.Context(), &acmpcasdk.UpdateCertificateAuthorityInput{
		CertificateAuthorityArn: created.CertificateAuthorityArn,
		Status:                  "NOT_A_REAL_STATUS",
	})
	require.Error(t, err)

	var ia *acmpcatypes.InvalidArgsException
	require.ErrorAs(t, err, &ia, "expected a real InvalidArgsException from the SDK deserializer")
}

// TestRevokeCertificate_InvalidRevocationReason_RealClient drives
// RevokeCertificate through the real client with a RevocationReason outside
// the documented enum. gopherstack previously emitted "InvalidParameterException"
// here too; RevokeCertificate's own deserializer
// (awsAwsjson11_deserializeOpErrorRevokeCertificate) models InvalidRequestException
// ("the request action cannot be performed or is prohibited"), which is the
// correct code for an unrecognized RevocationReason value.
func TestRevokeCertificate_InvalidRevocationReason_RealClient(t *testing.T) {
	t.Parallel()

	backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	h := acmpca.NewHandler(backend)
	client := newTestACMPCASDKClient(t, h)

	created, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
		CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeRoot,
		CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
			KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
			SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
			Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("Revoke CA")},
		},
	})
	require.NoError(t, err)

	_, err = client.RevokeCertificate(t.Context(), &acmpcasdk.RevokeCertificateInput{
		CertificateAuthorityArn: created.CertificateAuthorityArn,
		CertificateSerial:       aws.String("01"),
		RevocationReason:        "NOT_A_REAL_REASON",
	})
	require.Error(t, err)

	var ir *acmpcatypes.InvalidRequestException
	require.ErrorAs(t, err, &ir, "expected a real InvalidRequestException from the SDK deserializer")
}

// TestImportCertificateAuthorityCertificate_MalformedCertificate_RealClient
// drives ImportCertificateAuthorityCertificate through the real client with
// Certificate bytes that are not a valid PEM certificate (the SDK
// base64-encodes the []byte field regardless of its content, so this reaches
// gopherstack's server-side PEM decode). gopherstack previously emitted
// "InvalidParameterException" here; ImportCertificateAuthorityCertificate's
// own deserializer (awsAwsjson11_deserializeOpErrorImportCertificateAuthorityCertificate)
// models MalformedCertificateException, which is the correct code for a
// certificate that fails to decode/parse.
func TestImportCertificateAuthorityCertificate_MalformedCertificate_RealClient(t *testing.T) {
	t.Parallel()

	backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	h := acmpca.NewHandler(backend)
	client := newTestACMPCASDKClient(t, h)

	created, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
		CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeSubordinate,
		CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
			KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
			SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
			Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("Import Me CA")},
		},
	})
	require.NoError(t, err)

	_, err = client.ImportCertificateAuthorityCertificate(t.Context(),
		&acmpcasdk.ImportCertificateAuthorityCertificateInput{
			CertificateAuthorityArn: created.CertificateAuthorityArn,
			Certificate:             []byte("this is not a PEM certificate"),
		},
	)
	require.Error(t, err)

	var mc *acmpcatypes.MalformedCertificateException
	require.ErrorAs(t, err, &mc, "expected a real MalformedCertificateException from the SDK deserializer")
}
