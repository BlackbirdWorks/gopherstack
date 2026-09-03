package elbv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// TestDescribeTrustStores_Pagination proves Marker/PageSize are honored --
// previously handleDescribeTrustStores read ARN/Name filters but never
// paginated at all, always returning every trust store in one page.
func TestDescribeTrustStores_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	client := newTestELBv2Client(t, h)
	ctx := t.Context()

	names := []string{"ts-a", "ts-b", "ts-c"}
	for _, n := range names {
		_, err := client.CreateTrustStore(ctx, &elbv2sdk.CreateTrustStoreInput{
			Name:                         aws.String(n),
			CaCertificatesBundleS3Bucket: aws.String("bucket"),
			CaCertificatesBundleS3Key:    aws.String("key/" + n),
		})
		require.NoError(t, err)
	}

	page1, err := client.DescribeTrustStores(ctx, &elbv2sdk.DescribeTrustStoresInput{
		PageSize: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, page1.TrustStores, 2, "PageSize must cap the page size")
	require.NotNil(t, page1.NextMarker, "a truncated result must carry a NextMarker")

	page2, err := client.DescribeTrustStores(ctx, &elbv2sdk.DescribeTrustStoresInput{
		PageSize: aws.Int32(2),
		Marker:   page1.NextMarker,
	})
	require.NoError(t, err)
	require.Len(t, page2.TrustStores, 1, "the second page must return the remainder")
}

// TestDescribeTrustStoreRevocations_RevocationIDsFilter proves the
// RevocationIds request member is applied -- previously it was accepted on
// the wire but never read, so every revocation on the trust store was
// always returned regardless of the requested IDs.
func TestDescribeTrustStoreRevocations_RevocationIDsFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	client := newTestELBv2Client(t, h)
	ctx := t.Context()

	tsOut, err := client.CreateTrustStore(ctx, &elbv2sdk.CreateTrustStoreInput{
		Name:                         aws.String("revocation-filter-ts"),
		CaCertificatesBundleS3Bucket: aws.String("bucket"),
		CaCertificatesBundleS3Key:    aws.String("key"),
	})
	require.NoError(t, err)
	tsArn := aws.ToString(tsOut.TrustStores[0].TrustStoreArn)

	added, err := client.AddTrustStoreRevocations(ctx, &elbv2sdk.AddTrustStoreRevocationsInput{
		TrustStoreArn: aws.String(tsArn),
		RevocationContents: []types.RevocationContent{
			{RevocationType: types.RevocationTypeCrl, S3Bucket: aws.String("bucket"), S3Key: aws.String("rev1.crl")},
			{RevocationType: types.RevocationTypeCrl, S3Bucket: aws.String("bucket"), S3Key: aws.String("rev2.crl")},
		},
	})
	require.NoError(t, err)
	require.Len(t, added.TrustStoreRevocations, 2)

	wantID := aws.ToInt64(added.TrustStoreRevocations[0].RevocationId)

	filtered, err := client.DescribeTrustStoreRevocations(ctx, &elbv2sdk.DescribeTrustStoreRevocationsInput{
		TrustStoreArn: aws.String(tsArn),
		RevocationIds: []int64{wantID},
	})
	require.NoError(t, err)
	require.Len(t, filtered.TrustStoreRevocations, 1, "RevocationIds filter must exclude non-matching revocations")
	assert.Equal(t, wantID, aws.ToInt64(filtered.TrustStoreRevocations[0].RevocationId))

	all, err := client.DescribeTrustStoreRevocations(ctx, &elbv2sdk.DescribeTrustStoreRevocationsInput{
		TrustStoreArn: aws.String(tsArn),
	})
	require.NoError(t, err)
	assert.Len(t, all.TrustStoreRevocations, 2)
}

// TestDescribeListenerCertificates_Pagination proves Marker/PageSize are
// honored -- previously handleDescribeListenerCertificates never read
// either and always returned every certificate on the listener in one page.
func TestDescribeListenerCertificates_Pagination(t *testing.T) {
	t.Parallel()

	backend := elbv2.NewInMemoryBackend("123456789012", "us-east-1")
	h := elbv2.NewHandler(backend)
	client := newTestELBv2Client(t, h)
	ctx := t.Context()

	lbOut, err := client.CreateLoadBalancer(ctx, &elbv2sdk.CreateLoadBalancerInput{
		Name:    aws.String("cert-page-lb"),
		Subnets: []string{"subnet-11111111", "subnet-22222222"},
	})
	require.NoError(t, err)
	lbArn := aws.ToString(lbOut.LoadBalancers[0].LoadBalancerArn)

	lsOut, err := client.CreateListener(ctx, &elbv2sdk.CreateListenerInput{
		LoadBalancerArn: aws.String(lbArn),
		Protocol:        types.ProtocolEnumHttps,
		Port:            aws.Int32(443),
		Certificates: []types.Certificate{
			{CertificateArn: aws.String("arn:aws:acm:us-east-1:123456789012:certificate/default")},
		},
		DefaultActions: []types.Action{
			{
				Type: types.ActionTypeEnumFixedResponse,
				FixedResponseConfig: &types.FixedResponseActionConfig{
					StatusCode: aws.String("200"),
				},
			},
		},
	})
	require.NoError(t, err)
	listenerArn := aws.ToString(lsOut.Listeners[0].ListenerArn)

	extraCertSuffixes := []string{"a", "b"}
	for _, suffix := range extraCertSuffixes {
		_, addErr := client.AddListenerCertificates(ctx, &elbv2sdk.AddListenerCertificatesInput{
			ListenerArn: aws.String(listenerArn),
			Certificates: []types.Certificate{
				{CertificateArn: aws.String("arn:aws:acm:us-east-1:123456789012:certificate/extra-" + suffix)},
			},
		})
		require.NoError(t, addErr)
	}

	// Default cert + 2 SNI certs = 3 total.
	page1, err := client.DescribeListenerCertificates(ctx, &elbv2sdk.DescribeListenerCertificatesInput{
		ListenerArn: aws.String(listenerArn),
		PageSize:    aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, page1.Certificates, 2, "PageSize must cap the page size")
	require.NotNil(t, page1.NextMarker, "a truncated result must carry a NextMarker")

	page2, err := client.DescribeListenerCertificates(ctx, &elbv2sdk.DescribeListenerCertificatesInput{
		ListenerArn: aws.String(listenerArn),
		PageSize:    aws.Int32(2),
		Marker:      page1.NextMarker,
	})
	require.NoError(t, err)
	require.Len(t, page2.Certificates, 1, "the second page must return the remainder")
}
