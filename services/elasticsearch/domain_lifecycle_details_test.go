package elasticsearch_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticsearchsdk "github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	"github.com/aws/aws-sdk-go-v2/service/elasticsearchservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

func TestPackageAssociationDetails(t *testing.T) {
	t.Parallel()

	backend := elasticsearch.NewInMemoryBackend("123456789012", rtTestRegion)
	client := newTestElasticsearchClient(t, elasticsearch.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateElasticsearchDomain(ctx, &elasticsearchsdk.CreateElasticsearchDomainInput{
		DomainName: aws.String("assoc-domain"),
	})
	require.NoError(t, err)

	src := &types.PackageSource{S3BucketName: aws.String("b"), S3Key: aws.String("k")}
	pkg, err := client.CreatePackage(ctx, &elasticsearchsdk.CreatePackageInput{
		PackageName: aws.String("assoc-pkg"), PackageType: types.PackageTypeTxtDictionary, PackageSource: src,
	})
	require.NoError(t, err)

	_, err = client.UpdatePackage(ctx, &elasticsearchsdk.UpdatePackageInput{
		PackageID: pkg.PackageDetails.PackageID, PackageSource: src,
	})
	require.NoError(t, err)

	assoc, err := client.AssociatePackage(ctx, &elasticsearchsdk.AssociatePackageInput{
		DomainName: aws.String("assoc-domain"), PackageID: pkg.PackageDetails.PackageID,
	})
	require.NoError(t, err)

	check := func(t *testing.T, d types.DomainPackageDetails) {
		t.Helper()

		assert.Equal(t, "assoc-pkg", aws.ToString(d.PackageName))
		assert.Equal(t, types.PackageTypeTxtDictionary, d.PackageType)
		assert.Equal(t, "v2", aws.ToString(d.PackageVersion))
		assert.Equal(t, "analyzers/"+aws.ToString(pkg.PackageDetails.PackageID), aws.ToString(d.ReferencePath))
		require.NotNil(t, d.LastUpdated)
		assert.WithinDuration(t, time.Now(), *d.LastUpdated, time.Minute)
	}

	check(t, *assoc.DomainPackageDetails)

	byPkg, err := client.ListDomainsForPackage(ctx, &elasticsearchsdk.ListDomainsForPackageInput{
		PackageID: pkg.PackageDetails.PackageID,
	})
	require.NoError(t, err)
	require.Len(t, byPkg.DomainPackageDetailsList, 1)
	check(t, byPkg.DomainPackageDetailsList[0])

	byDomain, err := client.ListPackagesForDomain(ctx, &elasticsearchsdk.ListPackagesForDomainInput{
		DomainName: aws.String("assoc-domain"),
	})
	require.NoError(t, err)
	require.Len(t, byDomain.DomainPackageDetailsList, 1)
	check(t, byDomain.DomainPackageDetailsList[0])

	snap := backend.Snapshot(ctx)
	restored := elasticsearch.NewInMemoryBackend("123456789012", rtTestRegion)
	require.NoError(t, restored.Restore(ctx, snap))

	got, err := restored.ListDomainsForPackage(ctx, aws.ToString(pkg.PackageDetails.PackageID))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "v2", got[0].PackageVersion)
}

type fakeClock struct{ ns atomic.Int64 }

func (c *fakeClock) now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *fakeClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

func TestDomainProcessingWindows(t *testing.T) {
	t.Parallel()

	const delay = time.Minute

	clock := &fakeClock{}
	clock.ns.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())

	backend := elasticsearch.NewInMemoryBackend("123456789012", rtTestRegion)
	backend.SetClock(clock.now)
	backend.SetProcessingDelay(delay)

	client := newTestElasticsearchClient(t, elasticsearch.NewHandler(backend))
	ctx := t.Context()
	name := aws.String("proc-domain")

	describe := func(t *testing.T) *types.ElasticsearchDomainStatus {
		t.Helper()

		out, err := client.DescribeElasticsearchDomain(ctx, &elasticsearchsdk.DescribeElasticsearchDomainInput{
			DomainName: name,
		})
		require.NoError(t, err)

		return out.DomainStatus
	}

	optionState := func(t *testing.T) types.OptionState {
		t.Helper()

		out, err := client.DescribeElasticsearchDomainConfig(
			ctx,
			&elasticsearchsdk.DescribeElasticsearchDomainConfigInput{
				DomainName: name,
			},
		)
		require.NoError(t, err)

		return out.DomainConfig.ElasticsearchVersion.Status.State
	}

	changeStatus := func(t *testing.T) types.OverallChangeStatus {
		t.Helper()

		out, err := client.DescribeDomainChangeProgress(ctx, &elasticsearchsdk.DescribeDomainChangeProgressInput{
			DomainName: name,
		})
		require.NoError(t, err)

		return out.ChangeProgressStatus.Status
	}

	_, err := client.CreateElasticsearchDomain(ctx, &elasticsearchsdk.CreateElasticsearchDomainInput{DomainName: name})
	require.NoError(t, err)

	st := describe(t)
	assert.True(t, aws.ToBool(st.Processing))
	assert.Equal(t, types.DomainProcessingStatusTypeCreating, st.DomainProcessingStatus)
	assert.Equal(t, types.OptionStateProcessing, optionState(t))
	assert.Equal(t, types.OverallChangeStatusProcessing, changeStatus(t))

	clock.advance(delay)

	st = describe(t)
	assert.False(t, aws.ToBool(st.Processing))
	assert.Equal(t, types.DomainProcessingStatusTypeActive, st.DomainProcessingStatus)
	assert.Equal(t, types.OptionStateActive, optionState(t))
	assert.Equal(t, types.OverallChangeStatusCompleted, changeStatus(t))

	_, err = client.UpdateElasticsearchDomainConfig(ctx, &elasticsearchsdk.UpdateElasticsearchDomainConfigInput{
		DomainName:                 name,
		ElasticsearchClusterConfig: &types.ElasticsearchClusterConfig{InstanceCount: aws.Int32(2)},
	})
	require.NoError(t, err)

	st = describe(t)
	assert.True(t, aws.ToBool(st.Processing))
	assert.Equal(t, types.DomainProcessingStatusTypeModifying, st.DomainProcessingStatus)

	clock.advance(delay)

	_, err = client.DeleteElasticsearchDomain(ctx, &elasticsearchsdk.DeleteElasticsearchDomainInput{DomainName: name})
	require.NoError(t, err)

	st = describe(t)
	assert.True(t, aws.ToBool(st.Deleted))
	assert.Equal(t, types.DomainProcessingStatusTypeDeleting, st.DomainProcessingStatus)

	names, err := client.ListDomainNames(ctx, &elasticsearchsdk.ListDomainNamesInput{})
	require.NoError(t, err)
	assert.Len(t, names.DomainNames, 1)

	clock.advance(delay)

	_, err = client.DescribeElasticsearchDomain(
		ctx,
		&elasticsearchsdk.DescribeElasticsearchDomainInput{DomainName: name},
	)
	require.Error(t, err)

	names, err = client.ListDomainNames(ctx, &elasticsearchsdk.ListDomainNamesInput{})
	require.NoError(t, err)
	assert.Empty(t, names.DomainNames)
}

type fakeSubnets struct{}

func (fakeSubnets) ResolveSubnets(string, []string) (string, []string) {
	return "vpc-0abc", []string{"us-east-1a", "us-east-1b"}
}

func TestVPCOptionsResolvedFromSubnets(t *testing.T) {
	t.Parallel()

	backend := elasticsearch.NewInMemoryBackend("123456789012", rtTestRegion)
	backend.SetSubnetResolver(fakeSubnets{})
	client := newTestElasticsearchClient(t, elasticsearch.NewHandler(backend))
	ctx := t.Context()

	out, err := client.CreateElasticsearchDomain(ctx, &elasticsearchsdk.CreateElasticsearchDomainInput{
		DomainName: aws.String("vpc-domain"),
		VPCOptions: &types.VPCOptions{
			SubnetIds:        []string{"subnet-1", "subnet-2"},
			SecurityGroupIds: []string{"sg-1"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.DomainStatus.VPCOptions)
	assert.Equal(t, "vpc-0abc", aws.ToString(out.DomainStatus.VPCOptions.VPCId))
	assert.Equal(t, []string{"us-east-1a", "us-east-1b"}, out.DomainStatus.VPCOptions.AvailabilityZones)

	cfg, err := client.DescribeElasticsearchDomainConfig(ctx, &elasticsearchsdk.DescribeElasticsearchDomainConfigInput{
		DomainName: aws.String("vpc-domain"),
	})
	require.NoError(t, err)
	assert.Equal(t, "vpc-0abc", aws.ToString(cfg.DomainConfig.VPCOptions.Options.VPCId))
}
