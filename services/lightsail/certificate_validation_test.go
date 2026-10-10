package lightsail_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	lightsailtypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCertificate_ValidationRecordsAndInUseCount(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateCertificate(ctx, &lightsailsdk.CreateCertificateInput{
		CertificateName:         aws.String("val-cert"),
		DomainName:              aws.String("example.com"),
		SubjectAlternativeNames: []string{"www.example.com", "example.com"},
	})
	require.NoError(t, err)

	get := func() lightsailtypes.Certificate {
		out, getErr := client.GetCertificates(ctx, &lightsailsdk.GetCertificatesInput{
			CertificateName: aws.String("val-cert"),
		})
		require.NoError(t, getErr)
		require.Len(t, out.Certificates, 1)

		return *out.Certificates[0].CertificateDetail
	}

	require.Eventually(t, func() bool {
		return get().Status == lightsailtypes.CertificateStatusIssued
	}, defaultAsyncWait, defaultAsyncPoll, "certificate never issued")

	detail := get()
	require.Len(t, detail.DomainValidationRecords, 2, "one record per distinct domain")
	assert.NotEmpty(t, aws.ToString(detail.SupportCode))
	assert.EqualValues(t, 0, detail.InUseResourceCount)

	for _, r := range detail.DomainValidationRecords {
		assert.Equal(t, lightsailtypes.CertificateDomainValidationStatusSuccess, r.ValidationStatus)
		require.NotNil(t, r.ResourceRecord)
		assert.Equal(t, "CNAME", aws.ToString(r.ResourceRecord.Type))
		assert.Contains(t, aws.ToString(r.ResourceRecord.Name), aws.ToString(r.DomainName))
		assert.Contains(t, aws.ToString(r.ResourceRecord.Value), "acm-validations.aws.")
	}

	_, err = client.CreateBucket(ctx, &lightsailsdk.CreateBucketInput{
		BucketName: aws.String("val-origin"), BundleId: aws.String("small_1_0"),
	})
	require.NoError(t, err)

	_, err = client.CreateDistribution(ctx, &lightsailsdk.CreateDistributionInput{
		DistributionName: aws.String("val-dist"), BundleId: aws.String("small_1_0"),
		Origin:               &lightsailtypes.InputOrigin{Name: aws.String("val-origin")},
		DefaultCacheBehavior: &lightsailtypes.CacheBehavior{Behavior: lightsailtypes.BehaviorEnum("cache")},
	})
	require.NoError(t, err)

	_, err = client.AttachCertificateToDistribution(ctx, &lightsailsdk.AttachCertificateToDistributionInput{
		DistributionName: aws.String("val-dist"), CertificateName: aws.String("val-cert"),
	})
	require.NoError(t, err)

	assert.EqualValues(t, 1, get().InUseResourceCount)
}
