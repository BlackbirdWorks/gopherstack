package lightsail_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	lightsailtypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/lightsail"
)

const tagsRTRegion = "us-east-1"

// newTestLightsailClient stands up the real aws-sdk-go-v2 lightsail client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestLightsailClient(t *testing.T, h *lightsail.Handler) *lightsailsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(tagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return lightsailsdk.NewFromConfig(cfg, func(o *lightsailsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func oneTag() []lightsailtypes.Tag {
	return []lightsailtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}}
}

func assertOneTag(t *testing.T, got []lightsailtypes.Tag) {
	t.Helper()

	require.Len(t, got, 1)
	assert.Equal(t, "env", aws.ToString(got[0].Key))
	assert.Equal(t, "test", aws.ToString(got[0].Value))
}

// TestCreateOpsWithTags_RoundTrip drives every lightsail Create op whose real
// Input struct accepts Tags (lightsail@v1.58.4: 18 Create* ops across
// api_op_Create*.go) through the real SDK client and asserts the resource's
// own Describe/Get response renders what was supplied at creation
// (gopherstack-2mwl). Lightsail has no ListTagsForResource op -- tags travel
// only through each resource's own describe response, so that is the shape
// this test must exercise, not a generic tag-list API.
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		verify func(t *testing.T, client *lightsailsdk.Client)
		name   string
	}{
		{
			name: "bucket",
			verify: func(t *testing.T, client *lightsailsdk.Client) {
				t.Helper()

				_, err := client.CreateBucket(t.Context(), &lightsailsdk.CreateBucketInput{
					BucketName: aws.String("tagged-bucket"),
					BundleId:   aws.String("small_1_0"),
					Tags:       oneTag(),
				})
				require.NoError(t, err)

				out, err := client.GetBuckets(
					t.Context(),
					&lightsailsdk.GetBucketsInput{BucketName: aws.String("tagged-bucket")},
				)
				require.NoError(t, err)
				require.Len(t, out.Buckets, 1)
				assertOneTag(t, out.Buckets[0].Tags)
			},
		},
		{
			name: "certificate",
			verify: func(t *testing.T, client *lightsailsdk.Client) {
				t.Helper()

				_, err := client.CreateCertificate(t.Context(), &lightsailsdk.CreateCertificateInput{
					CertificateName: aws.String("tagged-cert"),
					DomainName:      aws.String("example.com"),
					Tags:            oneTag(),
				})
				require.NoError(t, err)

				out, err := client.GetCertificates(t.Context(), &lightsailsdk.GetCertificatesInput{
					CertificateName: aws.String("tagged-cert"),
				})
				require.NoError(t, err)
				require.Len(t, out.Certificates, 1)
				assertOneTag(t, out.Certificates[0].Tags)
			},
		},
		{
			name: "contact method",
			verify: func(t *testing.T, client *lightsailsdk.Client) {
				t.Helper()

				_, err := client.CreateContactMethod(t.Context(), &lightsailsdk.CreateContactMethodInput{
					ContactEndpoint: aws.String("test@example.com"),
					Protocol:        lightsailtypes.ContactProtocolEmail,
					Tags:            oneTag(),
				})
				require.NoError(t, err)

				out, err := client.GetContactMethods(t.Context(), &lightsailsdk.GetContactMethodsInput{})
				require.NoError(t, err)
				require.Len(t, out.ContactMethods, 1)
				assertOneTag(t, out.ContactMethods[0].Tags)
			},
		},
		{
			name: "container service",
			verify: func(t *testing.T, client *lightsailsdk.Client) {
				t.Helper()

				_, err := client.CreateContainerService(t.Context(), &lightsailsdk.CreateContainerServiceInput{
					ServiceName: aws.String("tagged-svc"),
					Power:       lightsailtypes.ContainerServicePowerNameNano,
					Scale:       aws.Int32(1),
					Tags:        oneTag(),
				})
				require.NoError(t, err)

				out, err := client.GetContainerServices(t.Context(), &lightsailsdk.GetContainerServicesInput{
					ServiceName: aws.String("tagged-svc"),
				})
				require.NoError(t, err)
				require.Len(t, out.ContainerServices, 1)
				assertOneTag(t, out.ContainerServices[0].Tags)
			},
		},
		{
			name: "disk",
			verify: func(t *testing.T, client *lightsailsdk.Client) {
				t.Helper()

				_, err := client.CreateDisk(t.Context(), &lightsailsdk.CreateDiskInput{
					AvailabilityZone: aws.String("us-east-1a"),
					DiskName:         aws.String("tagged-disk"),
					SizeInGb:         aws.Int32(8),
					Tags:             oneTag(),
				})
				require.NoError(t, err)

				out, err := client.GetDisk(t.Context(), &lightsailsdk.GetDiskInput{DiskName: aws.String("tagged-disk")})
				require.NoError(t, err)
				assertOneTag(t, out.Disk.Tags)
			},
		},
		{
			name: "distribution",
			verify: func(t *testing.T, client *lightsailsdk.Client) {
				t.Helper()

				_, err := client.CreateBucket(t.Context(), &lightsailsdk.CreateBucketInput{
					BucketName: aws.String("dist-origin-bucket"),
					BundleId:   aws.String("small_1_0"),
				})
				require.NoError(t, err)

				_, err = client.CreateDistribution(t.Context(), &lightsailsdk.CreateDistributionInput{
					DistributionName: aws.String("tagged-dist"),
					BundleId:         aws.String("small_1_0"),
					Origin: &lightsailtypes.InputOrigin{
						Name:           aws.String("dist-origin-bucket"),
						RegionName:     lightsailtypes.RegionNameUsEast1,
						ProtocolPolicy: lightsailtypes.OriginProtocolPolicyEnumHTTPOnly,
					},
					DefaultCacheBehavior: &lightsailtypes.CacheBehavior{
						Behavior: lightsailtypes.BehaviorEnumCacheSetting,
					},
					Tags: oneTag(),
				})
				require.NoError(t, err)

				out, err := client.GetDistributions(t.Context(), &lightsailsdk.GetDistributionsInput{
					DistributionName: aws.String("tagged-dist"),
				})
				require.NoError(t, err)
				require.Len(t, out.Distributions, 1)
				assertOneTag(t, out.Distributions[0].Tags)
			},
		},
		{
			name: "domain",
			verify: func(t *testing.T, client *lightsailsdk.Client) {
				t.Helper()

				_, err := client.CreateDomain(t.Context(), &lightsailsdk.CreateDomainInput{
					DomainName: aws.String("tagged-domain.com"),
					Tags:       oneTag(),
				})
				require.NoError(t, err)

				out, err := client.GetDomain(
					t.Context(),
					&lightsailsdk.GetDomainInput{DomainName: aws.String("tagged-domain.com")},
				)
				require.NoError(t, err)
				assertOneTag(t, out.Domain.Tags)
			},
		},
		{
			name: "instances",
			verify: func(t *testing.T, client *lightsailsdk.Client) {
				t.Helper()

				_, err := client.CreateInstances(t.Context(), &lightsailsdk.CreateInstancesInput{
					AvailabilityZone: aws.String("us-east-1a"),
					BlueprintId:      aws.String("amazon_linux_2023"),
					BundleId:         aws.String("nano_3_0"),
					InstanceNames:    []string{"tagged-instance"},
					Tags:             oneTag(),
				})
				require.NoError(t, err)

				out, err := client.GetInstance(
					t.Context(),
					&lightsailsdk.GetInstanceInput{InstanceName: aws.String("tagged-instance")},
				)
				require.NoError(t, err)
				assertOneTag(t, out.Instance.Tags)
			},
		},
		{
			name: "instance snapshot",
			verify: func(t *testing.T, client *lightsailsdk.Client) {
				t.Helper()

				_, err := client.CreateInstances(t.Context(), &lightsailsdk.CreateInstancesInput{
					AvailabilityZone: aws.String("us-east-1a"),
					BlueprintId:      aws.String("amazon_linux_2023"),
					BundleId:         aws.String("nano_3_0"),
					InstanceNames:    []string{"snap-source-instance"},
				})
				require.NoError(t, err)

				_, err = client.CreateInstanceSnapshot(t.Context(), &lightsailsdk.CreateInstanceSnapshotInput{
					InstanceName:         aws.String("snap-source-instance"),
					InstanceSnapshotName: aws.String("tagged-instance-snap"),
					Tags:                 oneTag(),
				})
				require.NoError(t, err)

				out, err := client.GetInstanceSnapshot(t.Context(), &lightsailsdk.GetInstanceSnapshotInput{
					InstanceSnapshotName: aws.String("tagged-instance-snap"),
				})
				require.NoError(t, err)
				assertOneTag(t, out.InstanceSnapshot.Tags)
			},
		},
		{
			name: "instances from snapshot",
			verify: func(t *testing.T, client *lightsailsdk.Client) {
				t.Helper()

				_, err := client.CreateInstances(t.Context(), &lightsailsdk.CreateInstancesInput{
					AvailabilityZone: aws.String("us-east-1a"),
					BlueprintId:      aws.String("amazon_linux_2023"),
					BundleId:         aws.String("nano_3_0"),
					InstanceNames:    []string{"restore-source-instance"},
				})
				require.NoError(t, err)

				_, err = client.CreateInstanceSnapshot(t.Context(), &lightsailsdk.CreateInstanceSnapshotInput{
					InstanceName:         aws.String("restore-source-instance"),
					InstanceSnapshotName: aws.String("restore-source-snap"),
				})
				require.NoError(t, err)

				_, err = client.CreateInstancesFromSnapshot(t.Context(), &lightsailsdk.CreateInstancesFromSnapshotInput{
					AvailabilityZone:     aws.String("us-east-1a"),
					BundleId:             aws.String("nano_3_0"),
					InstanceNames:        []string{"restored-instance"},
					InstanceSnapshotName: aws.String("restore-source-snap"),
					Tags:                 oneTag(),
				})
				require.NoError(t, err)

				out, err := client.GetInstance(
					t.Context(),
					&lightsailsdk.GetInstanceInput{InstanceName: aws.String("restored-instance")},
				)
				require.NoError(t, err)
				assertOneTag(t, out.Instance.Tags)
			},
		},
		{
			name: "key pair",
			verify: func(t *testing.T, client *lightsailsdk.Client) {
				t.Helper()

				_, err := client.CreateKeyPair(t.Context(), &lightsailsdk.CreateKeyPairInput{
					KeyPairName: aws.String("tagged-keypair"),
					Tags:        oneTag(),
				})
				require.NoError(t, err)

				out, err := client.GetKeyPair(
					t.Context(),
					&lightsailsdk.GetKeyPairInput{KeyPairName: aws.String("tagged-keypair")},
				)
				require.NoError(t, err)
				assertOneTag(t, out.KeyPair.Tags)
			},
		},
		{
			name: "load balancer",
			verify: func(t *testing.T, client *lightsailsdk.Client) {
				t.Helper()

				_, err := client.CreateLoadBalancer(t.Context(), &lightsailsdk.CreateLoadBalancerInput{
					LoadBalancerName: aws.String("tagged-lb"),
					InstancePort:     80,
					Tags:             oneTag(),
				})
				require.NoError(t, err)

				out, err := client.GetLoadBalancer(t.Context(), &lightsailsdk.GetLoadBalancerInput{
					LoadBalancerName: aws.String("tagged-lb"),
				})
				require.NoError(t, err)
				assertOneTag(t, out.LoadBalancer.Tags)
			},
		},
		{
			name: "load balancer tls certificate",
			verify: func(t *testing.T, client *lightsailsdk.Client) {
				t.Helper()

				_, err := client.CreateLoadBalancer(t.Context(), &lightsailsdk.CreateLoadBalancerInput{
					LoadBalancerName: aws.String("tls-host-lb"),
					InstancePort:     80,
				})
				require.NoError(t, err)

				_, err = client.CreateLoadBalancerTlsCertificate(
					t.Context(),
					&lightsailsdk.CreateLoadBalancerTlsCertificateInput{
						LoadBalancerName:      aws.String("tls-host-lb"),
						CertificateName:       aws.String("tagged-tls-cert"),
						CertificateDomainName: aws.String("example.com"),
						Tags:                  oneTag(),
					},
				)
				require.NoError(t, err)

				out, err := client.GetLoadBalancerTlsCertificates(
					t.Context(),
					&lightsailsdk.GetLoadBalancerTlsCertificatesInput{
						LoadBalancerName: aws.String("tls-host-lb"),
					},
				)
				require.NoError(t, err)
				require.Len(t, out.TlsCertificates, 1)
				assertOneTag(t, out.TlsCertificates[0].Tags)
			},
		},
		{
			name: "relational database",
			verify: func(t *testing.T, client *lightsailsdk.Client) {
				t.Helper()

				_, err := client.CreateRelationalDatabase(t.Context(), &lightsailsdk.CreateRelationalDatabaseInput{
					RelationalDatabaseName:        aws.String("tagged-db"),
					RelationalDatabaseBlueprintId: aws.String("mysql_8_0"),
					RelationalDatabaseBundleId:    aws.String("micro_2_0"),
					MasterDatabaseName:            aws.String("maindb"),
					MasterUsername:                aws.String("admin"),
					Tags:                          oneTag(),
				})
				require.NoError(t, err)

				out, err := client.GetRelationalDatabase(t.Context(), &lightsailsdk.GetRelationalDatabaseInput{
					RelationalDatabaseName: aws.String("tagged-db"),
				})
				require.NoError(t, err)
				assertOneTag(t, out.RelationalDatabase.Tags)
			},
		},
		{
			name: "relational database snapshot",
			verify: func(t *testing.T, client *lightsailsdk.Client) {
				t.Helper()

				_, err := client.CreateRelationalDatabase(t.Context(), &lightsailsdk.CreateRelationalDatabaseInput{
					RelationalDatabaseName:        aws.String("snap-source-db"),
					RelationalDatabaseBlueprintId: aws.String("mysql_8_0"),
					RelationalDatabaseBundleId:    aws.String("micro_2_0"),
					MasterDatabaseName:            aws.String("maindb"),
					MasterUsername:                aws.String("admin"),
				})
				require.NoError(t, err)

				_, err = client.CreateRelationalDatabaseSnapshot(
					t.Context(),
					&lightsailsdk.CreateRelationalDatabaseSnapshotInput{
						RelationalDatabaseName:         aws.String("snap-source-db"),
						RelationalDatabaseSnapshotName: aws.String("tagged-db-snap"),
						Tags:                           oneTag(),
					},
				)
				require.NoError(t, err)

				out, err := client.GetRelationalDatabaseSnapshot(
					t.Context(),
					&lightsailsdk.GetRelationalDatabaseSnapshotInput{
						RelationalDatabaseSnapshotName: aws.String("tagged-db-snap"),
					},
				)
				require.NoError(t, err)
				assertOneTag(t, out.RelationalDatabaseSnapshot.Tags)
			},
		},
		{
			name: "relational database from snapshot",
			verify: func(t *testing.T, client *lightsailsdk.Client) {
				t.Helper()

				_, err := client.CreateRelationalDatabase(t.Context(), &lightsailsdk.CreateRelationalDatabaseInput{
					RelationalDatabaseName:        aws.String("restore-source-db"),
					RelationalDatabaseBlueprintId: aws.String("mysql_8_0"),
					RelationalDatabaseBundleId:    aws.String("micro_2_0"),
					MasterDatabaseName:            aws.String("maindb"),
					MasterUsername:                aws.String("admin"),
				})
				require.NoError(t, err)

				_, err = client.CreateRelationalDatabaseSnapshot(
					t.Context(),
					&lightsailsdk.CreateRelationalDatabaseSnapshotInput{
						RelationalDatabaseName:         aws.String("restore-source-db"),
						RelationalDatabaseSnapshotName: aws.String("restore-source-db-snap"),
					},
				)
				require.NoError(t, err)

				_, err = client.CreateRelationalDatabaseFromSnapshot(
					t.Context(),
					&lightsailsdk.CreateRelationalDatabaseFromSnapshotInput{
						RelationalDatabaseName:         aws.String("restored-db"),
						RelationalDatabaseSnapshotName: aws.String("restore-source-db-snap"),
						Tags:                           oneTag(),
					},
				)
				require.NoError(t, err)

				out, err := client.GetRelationalDatabase(t.Context(), &lightsailsdk.GetRelationalDatabaseInput{
					RelationalDatabaseName: aws.String("restored-db"),
				})
				require.NoError(t, err)
				assertOneTag(t, out.RelationalDatabase.Tags)
			},
		},
		{
			name: "disk snapshot",
			verify: func(t *testing.T, client *lightsailsdk.Client) {
				t.Helper()

				_, err := client.CreateDisk(t.Context(), &lightsailsdk.CreateDiskInput{
					AvailabilityZone: aws.String("us-east-1a"),
					DiskName:         aws.String("snap-source-disk"),
					SizeInGb:         aws.Int32(8),
				})
				require.NoError(t, err)

				_, err = client.CreateDiskSnapshot(t.Context(), &lightsailsdk.CreateDiskSnapshotInput{
					DiskName:         aws.String("snap-source-disk"),
					DiskSnapshotName: aws.String("tagged-disk-snap"),
					Tags:             oneTag(),
				})
				require.NoError(t, err)

				out, err := client.GetDiskSnapshot(t.Context(), &lightsailsdk.GetDiskSnapshotInput{
					DiskSnapshotName: aws.String("tagged-disk-snap"),
				})
				require.NoError(t, err)
				assertOneTag(t, out.DiskSnapshot.Tags)
			},
		},
		{
			name: "disk from snapshot",
			verify: func(t *testing.T, client *lightsailsdk.Client) {
				t.Helper()

				_, err := client.CreateDisk(t.Context(), &lightsailsdk.CreateDiskInput{
					AvailabilityZone: aws.String("us-east-1a"),
					DiskName:         aws.String("restore-source-disk"),
					SizeInGb:         aws.Int32(8),
				})
				require.NoError(t, err)

				_, err = client.CreateDiskSnapshot(t.Context(), &lightsailsdk.CreateDiskSnapshotInput{
					DiskName:         aws.String("restore-source-disk"),
					DiskSnapshotName: aws.String("restore-source-disk-snap"),
				})
				require.NoError(t, err)

				_, err = client.CreateDiskFromSnapshot(t.Context(), &lightsailsdk.CreateDiskFromSnapshotInput{
					AvailabilityZone: aws.String("us-east-1a"),
					DiskName:         aws.String("restored-disk"),
					SizeInGb:         aws.Int32(8),
					DiskSnapshotName: aws.String("restore-source-disk-snap"),
					Tags:             oneTag(),
				})
				require.NoError(t, err)

				out, err := client.GetDisk(
					t.Context(),
					&lightsailsdk.GetDiskInput{DiskName: aws.String("restored-disk")},
				)
				require.NoError(t, err)
				assertOneTag(t, out.Disk.Tags)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := lightsail.NewInMemoryBackend(t.Context(), "000000000000", tagsRTRegion)
			client := newTestLightsailClient(t, lightsail.NewHandler(backend))

			tt.verify(t, client)
		})
	}
}
