package lightsail_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	lightsailtypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/stretchr/testify/require"
)

// TestLoadBalancerRoundTrip exercises family L+M end to end.
func TestLoadBalancerRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateInstances(ctx, &lightsailsdk.CreateInstancesInput{
		InstanceNames: []string{"lb-target-1"}, AvailabilityZone: aws.String("us-east-1a"),
		BlueprintId: aws.String("amazon_linux_2023"), BundleId: aws.String("nano_3_0"),
	})
	require.NoError(t, err)

	_, err = client.CreateLoadBalancer(ctx, &lightsailsdk.CreateLoadBalancerInput{
		LoadBalancerName: aws.String("lb-1"), InstancePort: 80,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, getErr := client.GetLoadBalancer(
			ctx,
			&lightsailsdk.GetLoadBalancerInput{LoadBalancerName: aws.String("lb-1")},
		)

		return getErr == nil && out.LoadBalancer.State == lightsailtypes.LoadBalancerStateActive
	}, defaultAsyncWait, defaultAsyncPoll, "load balancer never reached active")

	_, err = client.AttachInstancesToLoadBalancer(ctx, &lightsailsdk.AttachInstancesToLoadBalancerInput{
		LoadBalancerName: aws.String("lb-1"), InstanceNames: []string{"lb-target-1"},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, getErr := client.GetLoadBalancer(
			ctx,
			&lightsailsdk.GetLoadBalancerInput{LoadBalancerName: aws.String("lb-1")},
		)

		return getErr == nil && len(out.LoadBalancer.InstanceHealthSummary) == 1 &&
			out.LoadBalancer.InstanceHealthSummary[0].InstanceHealth == lightsailtypes.InstanceHealthStateHealthy
	}, defaultAsyncWait, defaultAsyncPoll, "attached instance never reached healthy")

	_, err = client.CreateLoadBalancerTlsCertificate(ctx, &lightsailsdk.CreateLoadBalancerTlsCertificateInput{
		LoadBalancerName: aws.String(
			"lb-1",
		), CertificateName: aws.String("lb-1-cert"), CertificateDomainName: aws.String("example.com"),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, getErr := client.GetLoadBalancerTlsCertificates(
			ctx,
			&lightsailsdk.GetLoadBalancerTlsCertificatesInput{LoadBalancerName: aws.String("lb-1")},
		)

		return getErr == nil && len(out.TlsCertificates) == 1 &&
			out.TlsCertificates[0].Status == lightsailtypes.LoadBalancerTlsCertificateStatusIssued
	}, defaultAsyncWait, defaultAsyncPoll, "LB TLS cert never issued")

	_, err = client.AttachLoadBalancerTlsCertificate(ctx, &lightsailsdk.AttachLoadBalancerTlsCertificateInput{
		LoadBalancerName: aws.String("lb-1"), CertificateName: aws.String("lb-1-cert"),
	})
	require.NoError(t, err)

	policiesOut, err := client.GetLoadBalancerTlsPolicies(ctx, &lightsailsdk.GetLoadBalancerTlsPoliciesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, policiesOut.TlsPolicies)

	_, err = client.DetachInstancesFromLoadBalancer(ctx, &lightsailsdk.DetachInstancesFromLoadBalancerInput{
		LoadBalancerName: aws.String("lb-1"), InstanceNames: []string{"lb-target-1"},
	})
	require.NoError(t, err)

	_, err = client.DeleteLoadBalancerTlsCertificate(ctx, &lightsailsdk.DeleteLoadBalancerTlsCertificateInput{
		LoadBalancerName: aws.String("lb-1"), CertificateName: aws.String("lb-1-cert"),
	})
	require.NoError(t, err)

	_, err = client.DeleteLoadBalancer(ctx, &lightsailsdk.DeleteLoadBalancerInput{LoadBalancerName: aws.String("lb-1")})
	require.NoError(t, err)
}

// TestDomainAndCertificateAndDistributionRoundTrip exercises family U, V,
// and T end to end, including the confirmed-global Domain ARN and the
// distribution-origin FK onto a real Bucket.
func TestDomainAndCertificateAndDistributionRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	ctx := t.Context()

	_, err := client.CreateDomain(ctx, &lightsailsdk.CreateDomainInput{DomainName: aws.String("example.com")})
	require.NoError(t, err)

	getDomain, err := client.GetDomain(ctx, &lightsailsdk.GetDomainInput{DomainName: aws.String("example.com")})
	require.NoError(t, err)
	require.Contains(t, aws.ToString(getDomain.Domain.Arn), ":lightsail:global:")

	_, err = client.CreateDomainEntry(ctx, &lightsailsdk.CreateDomainEntryInput{
		DomainName: aws.String("example.com"),
		DomainEntry: &lightsailtypes.DomainEntry{
			Name: aws.String("www.example.com"), Type: aws.String("A"), Target: aws.String("203.0.113.5"),
		},
	})
	require.NoError(t, err)

	afterCreate, err := client.GetDomain(ctx, &lightsailsdk.GetDomainInput{DomainName: aws.String("example.com")})
	require.NoError(t, err)
	require.Len(t, afterCreate.Domain.DomainEntries, 1)

	entryID := afterCreate.Domain.DomainEntries[0].Id

	_, err = client.UpdateDomainEntry(ctx, &lightsailsdk.UpdateDomainEntryInput{
		DomainName: aws.String("example.com"),
		DomainEntry: &lightsailtypes.DomainEntry{
			Id: entryID, Name: aws.String("www.example.com"), Type: aws.String("A"), Target: aws.String("203.0.113.6"),
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteDomainEntry(ctx, &lightsailsdk.DeleteDomainEntryInput{
		DomainName:  aws.String("example.com"),
		DomainEntry: &lightsailtypes.DomainEntry{Id: entryID},
	})
	require.NoError(t, err)

	domainsOut, err := client.GetDomains(ctx, &lightsailsdk.GetDomainsInput{})
	require.NoError(t, err)
	require.Len(t, domainsOut.Domains, 1)

	// Certificate (family V).
	_, err = client.CreateCertificate(ctx, &lightsailsdk.CreateCertificateInput{
		CertificateName: aws.String("cert-1"), DomainName: aws.String("example.com"),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		out, getErr := client.GetCertificates(
			ctx,
			&lightsailsdk.GetCertificatesInput{CertificateName: aws.String("cert-1")},
		)

		return getErr == nil && len(out.Certificates) == 1 &&
			out.Certificates[0].CertificateDetail.Status == lightsailtypes.CertificateStatusIssued
	}, defaultAsyncWait, defaultAsyncPoll, "certificate never issued")

	// Distribution (family T), origin FK onto a real Bucket.
	_, err = client.CreateBucket(
		ctx,
		&lightsailsdk.CreateBucketInput{
			BucketName: aws.String("dist-origin-bucket"),
			BundleId:   aws.String("small_1_0"),
		},
	)
	require.NoError(t, err)

	_, err = client.CreateDistribution(ctx, &lightsailsdk.CreateDistributionInput{
		DistributionName: aws.String("dist-1"), BundleId: aws.String("small_1_0"),
		Origin:               &lightsailtypes.InputOrigin{Name: aws.String("dist-origin-bucket")},
		DefaultCacheBehavior: &lightsailtypes.CacheBehavior{Behavior: lightsailtypes.BehaviorEnum("cache")},
	})
	require.NoError(t, err)

	distOut, err := client.GetDistributions(
		ctx,
		&lightsailsdk.GetDistributionsInput{DistributionName: aws.String("dist-1")},
	)
	require.NoError(t, err)
	require.Len(t, distOut.Distributions, 1)
	require.Equal(t, "us-east-1", string(distOut.Distributions[0].Location.RegionName))

	_, err = client.AttachCertificateToDistribution(ctx, &lightsailsdk.AttachCertificateToDistributionInput{
		DistributionName: aws.String("dist-1"), CertificateName: aws.String("cert-1"),
	})
	require.NoError(t, err)

	resetOut, resetErr := client.ResetDistributionCache(
		ctx,
		&lightsailsdk.ResetDistributionCacheInput{DistributionName: aws.String("dist-1")},
	)
	require.NoError(t, resetErr)
	require.NotNil(t, resetOut.CreateTime)

	latestOut, latestErr := client.GetDistributionLatestCacheReset(
		ctx,
		&lightsailsdk.GetDistributionLatestCacheResetInput{DistributionName: aws.String("dist-1")},
	)
	require.NoError(t, latestErr)
	require.NotNil(t, latestOut.CreateTime)

	_, err = client.DetachCertificateFromDistribution(
		ctx,
		&lightsailsdk.DetachCertificateFromDistributionInput{DistributionName: aws.String("dist-1")},
	)
	require.NoError(t, err)

	_, err = client.DeleteDistribution(
		ctx,
		&lightsailsdk.DeleteDistributionInput{DistributionName: aws.String("dist-1")},
	)
	require.NoError(t, err)

	_, err = client.DeleteCertificate(ctx, &lightsailsdk.DeleteCertificateInput{CertificateName: aws.String("cert-1")})
	require.NoError(t, err)

	_, err = client.DeleteDomain(ctx, &lightsailsdk.DeleteDomainInput{DomainName: aws.String("example.com")})
	require.NoError(t, err)
}
