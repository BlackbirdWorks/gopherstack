package cloudfront_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// assertPaginatesAllRecords drives list across pages of size pageSize until NextMarker is nil,
// and asserts: the first page is full, a cursor comes back when more records remain, every
// record is seen, and no record is seen twice. Before the pagination fix, every listing under
// test here ignored Marker/MaxItems, returned all `total` records on page one, and reported no
// truncation -- so require.Len(page1, pageSize) alone already fails against the old code; the
// no-duplicate/exactly-once checks additionally catch a broken cursor (e.g. a non-unique sort
// key) that a naive fix could introduce.
func assertPaginatesAllRecords[T any](
	t *testing.T,
	total, pageSize int,
	list func(marker *string, maxItems int32) (page []T, nextMarker *string),
	keyOf func(T) string,
) {
	t.Helper()

	seen := make(map[string]bool, total)

	var marker *string

	for pages := 0; ; pages++ {
		require.Less(t, pages, total, "pagination did not terminate")

		page, next := list(marker, int32(pageSize))
		if pages == 0 {
			require.Len(t, page, pageSize, "first page should be full")
			require.NotNil(t, next, "first page should report a cursor")
		}

		for _, item := range page {
			k := keyOf(item)
			require.False(t, seen[k], "record %q seen twice across pages", k)
			seen[k] = true
		}

		if next == nil {
			break
		}

		marker = next
	}

	require.Len(t, seen, total, "did not see every record exactly once")
}

func TestListCachePolicies_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	for i := range total {
		_, err := backend.CreateCachePolicy(fmt.Sprintf("pg-cp-%02d", i), "pagination test", 0, 100, 0)
		require.NoError(t, err)
	}

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.CachePolicySummary, *string) {
			out, err := client.ListCachePolicies(t.Context(), &cfsdk.ListCachePoliciesInput{
				Type: types.CachePolicyTypeCustom, Marker: marker, MaxItems: aws.Int32(maxItems),
			})
			require.NoError(t, err)
			require.NotNil(t, out.CachePolicyList)

			return out.CachePolicyList.Items, out.CachePolicyList.NextMarker
		},
		func(s types.CachePolicySummary) string { return aws.ToString(s.CachePolicy.Id) },
	)
}

func TestListOriginRequestPolicies_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	for i := range total {
		_, err := backend.CreateOriginRequestPolicy(fmt.Sprintf("pg-orp-%02d", i), "pagination test")
		require.NoError(t, err)
	}

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.OriginRequestPolicySummary, *string) {
			out, err := client.ListOriginRequestPolicies(
				t.Context(), &cfsdk.ListOriginRequestPoliciesInput{
					Type: types.OriginRequestPolicyTypeCustom, Marker: marker, MaxItems: aws.Int32(maxItems),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, out.OriginRequestPolicyList)

			return out.OriginRequestPolicyList.Items, out.OriginRequestPolicyList.NextMarker
		},
		func(s types.OriginRequestPolicySummary) string { return aws.ToString(s.OriginRequestPolicy.Id) },
	)
}

func TestListResponseHeadersPolicies_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	for i := range total {
		_, err := backend.CreateResponseHeadersPolicy(fmt.Sprintf("pg-rhp-%02d", i), "pagination test")
		require.NoError(t, err)
	}

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.ResponseHeadersPolicySummary, *string) {
			out, err := client.ListResponseHeadersPolicies(
				t.Context(), &cfsdk.ListResponseHeadersPoliciesInput{
					Type: types.ResponseHeadersPolicyTypeCustom, Marker: marker, MaxItems: aws.Int32(maxItems),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, out.ResponseHeadersPolicyList)

			return out.ResponseHeadersPolicyList.Items, out.ResponseHeadersPolicyList.NextMarker
		},
		func(s types.ResponseHeadersPolicySummary) string { return aws.ToString(s.ResponseHeadersPolicy.Id) },
	)
}

func TestListOAIs_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	for i := range total {
		_, err := backend.CreateOAI(fmt.Sprintf("pg-oai-cr-%02d", i), "pagination test")
		require.NoError(t, err)
	}

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.CloudFrontOriginAccessIdentitySummary, *string) {
			out, err := client.ListCloudFrontOriginAccessIdentities(
				t.Context(),
				&cfsdk.ListCloudFrontOriginAccessIdentitiesInput{Marker: marker, MaxItems: aws.Int32(maxItems)},
			)
			require.NoError(t, err)
			require.NotNil(t, out.CloudFrontOriginAccessIdentityList)

			return out.CloudFrontOriginAccessIdentityList.Items, out.CloudFrontOriginAccessIdentityList.NextMarker
		},
		func(s types.CloudFrontOriginAccessIdentitySummary) string { return aws.ToString(s.Id) },
	)
}

func TestListOriginAccessControls_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	for i := range total {
		_, err := backend.CreateOriginAccessControl(
			fmt.Sprintf("pg-oac-%02d", i), "pagination test", "s3", "always", "sigv4",
		)
		require.NoError(t, err)
	}

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.OriginAccessControlSummary, *string) {
			out, err := client.ListOriginAccessControls(
				t.Context(), &cfsdk.ListOriginAccessControlsInput{Marker: marker, MaxItems: aws.Int32(maxItems)},
			)
			require.NoError(t, err)
			require.NotNil(t, out.OriginAccessControlList)

			return out.OriginAccessControlList.Items, out.OriginAccessControlList.NextMarker
		},
		func(s types.OriginAccessControlSummary) string { return aws.ToString(s.Id) },
	)
}

func TestListFieldLevelEncryptionConfigs_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	for i := range total {
		_, err := backend.CreateFieldLevelEncryption(fmt.Sprintf("pg-fle-%02d", i), "pagination test", nil)
		require.NoError(t, err)
	}

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.FieldLevelEncryptionSummary, *string) {
			out, err := client.ListFieldLevelEncryptionConfigs(
				t.Context(), &cfsdk.ListFieldLevelEncryptionConfigsInput{Marker: marker, MaxItems: aws.Int32(maxItems)},
			)
			require.NoError(t, err)
			require.NotNil(t, out.FieldLevelEncryptionList)

			return out.FieldLevelEncryptionList.Items, out.FieldLevelEncryptionList.NextMarker
		},
		func(s types.FieldLevelEncryptionSummary) string { return aws.ToString(s.Id) },
	)
}

func TestListFieldLevelEncryptionProfiles_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	for i := range total {
		_, err := backend.CreateFieldLevelEncryptionProfile(fmt.Sprintf("pg-flep-%02d", i), "pagination test", nil)
		require.NoError(t, err)
	}

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.FieldLevelEncryptionProfileSummary, *string) {
			out, err := client.ListFieldLevelEncryptionProfiles(
				t.Context(),
				&cfsdk.ListFieldLevelEncryptionProfilesInput{Marker: marker, MaxItems: aws.Int32(maxItems)},
			)
			require.NoError(t, err)
			require.NotNil(t, out.FieldLevelEncryptionProfileList)

			return out.FieldLevelEncryptionProfileList.Items, out.FieldLevelEncryptionProfileList.NextMarker
		},
		func(s types.FieldLevelEncryptionProfileSummary) string { return aws.ToString(s.Id) },
	)
}

func TestListPublicKeys_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	for i := range total {
		_, err := backend.CreatePublicKey(
			fmt.Sprintf("pg-pk-cr-%02d", i),
			fmt.Sprintf("pg-pk-%02d", i),
			"pagination test",
			"",
		)
		require.NoError(t, err)
	}

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.PublicKeySummary, *string) {
			out, err := client.ListPublicKeys(
				t.Context(), &cfsdk.ListPublicKeysInput{Marker: marker, MaxItems: aws.Int32(maxItems)},
			)
			require.NoError(t, err)
			require.NotNil(t, out.PublicKeyList)

			return out.PublicKeyList.Items, out.PublicKeyList.NextMarker
		},
		func(s types.PublicKeySummary) string { return aws.ToString(s.Id) },
	)
}

func TestListKeyGroups_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	for i := range total {
		_, err := backend.CreateKeyGroup(fmt.Sprintf("pg-kg-%02d", i), "pagination test", nil)
		require.NoError(t, err)
	}

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.KeyGroupSummary, *string) {
			out, err := client.ListKeyGroups(
				t.Context(), &cfsdk.ListKeyGroupsInput{Marker: marker, MaxItems: aws.Int32(maxItems)},
			)
			require.NoError(t, err)
			require.NotNil(t, out.KeyGroupList)

			return out.KeyGroupList.Items, out.KeyGroupList.NextMarker
		},
		func(s types.KeyGroupSummary) string { return aws.ToString(s.KeyGroup.Id) },
	)
}

func TestListRealtimeLogConfigs_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	endPoints := []cloudfront.RealtimeLogEndPoint{{
		StreamType: "Kinesis",
		RoleARN:    "arn:aws:iam::123456789012:role/pg-rlc-role",
		StreamARN:  "arn:aws:kinesis:us-east-1:123456789012:stream/pg-rlc-stream",
	}}
	for i := range total {
		_, err := backend.CreateRealtimeLogConfig(fmt.Sprintf("pg-rlc-%02d", i), 50, []string{"timestamp"}, endPoints)
		require.NoError(t, err)
	}

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.RealtimeLogConfig, *string) {
			out, err := client.ListRealtimeLogConfigs(
				t.Context(), &cfsdk.ListRealtimeLogConfigsInput{Marker: marker, MaxItems: aws.Int32(maxItems)},
			)
			require.NoError(t, err)
			require.NotNil(t, out.RealtimeLogConfigs)

			return out.RealtimeLogConfigs.Items, out.RealtimeLogConfigs.NextMarker
		},
		func(cfg types.RealtimeLogConfig) string { return aws.ToString(cfg.Name) },
	)
}

func TestListVpcOrigins_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	for i := range total {
		_, err := backend.CreateVpcOrigin(cloudfront.VpcOriginEndpointConfig{
			Name: fmt.Sprintf("pg-vpc-%02d", i),
			Arn: fmt.Sprintf(
				"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/pg-%02d",
				i,
			),
			OriginProtocolPolicy: "https-only",
			HTTPPort:             80,
			HTTPSPort:            443,
		}, nil)
		require.NoError(t, err)
	}

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.VpcOriginSummary, *string) {
			out, err := client.ListVpcOrigins(
				t.Context(), &cfsdk.ListVpcOriginsInput{Marker: marker, MaxItems: aws.Int32(maxItems)},
			)
			require.NoError(t, err)
			require.NotNil(t, out.VpcOriginList)

			return out.VpcOriginList.Items, out.VpcOriginList.NextMarker
		},
		func(s types.VpcOriginSummary) string { return aws.ToString(s.Id) },
	)
}

func TestListContinuousDeploymentPolicies_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	for i := range total {
		_, err := backend.CreateContinuousDeploymentPolicy(true, fmt.Sprintf("pg-cdp-%02d.cloudfront.net", i))
		require.NoError(t, err)
	}

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.ContinuousDeploymentPolicySummary, *string) {
			out, err := client.ListContinuousDeploymentPolicies(
				t.Context(),
				&cfsdk.ListContinuousDeploymentPoliciesInput{Marker: marker, MaxItems: aws.Int32(maxItems)},
			)
			require.NoError(t, err)
			require.NotNil(t, out.ContinuousDeploymentPolicyList)

			return out.ContinuousDeploymentPolicyList.Items, out.ContinuousDeploymentPolicyList.NextMarker
		},
		func(s types.ContinuousDeploymentPolicySummary) string {
			return aws.ToString(s.ContinuousDeploymentPolicy.Id)
		},
	)
}

func TestListStreamingDistributions_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	for i := range total {
		_, err := backend.CreateStreamingDistribution(cloudfront.StreamingDistributionConfig{
			CallerReference: fmt.Sprintf("pg-sd-%02d", i),
			Enabled:         true,
		}, nil)
		require.NoError(t, err)
	}

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.StreamingDistributionSummary, *string) {
			out, err := client.ListStreamingDistributions(
				t.Context(), &cfsdk.ListStreamingDistributionsInput{Marker: marker, MaxItems: aws.Int32(maxItems)},
			)
			require.NoError(t, err)
			require.NotNil(t, out.StreamingDistributionList)

			return out.StreamingDistributionList.Items, out.StreamingDistributionList.NextMarker
		},
		func(s types.StreamingDistributionSummary) string { return aws.ToString(s.Id) },
	)
}

func TestListTrustStores_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	for i := range total {
		_, err := backend.CreateTrustStore(
			fmt.Sprintf("pg-ts-%02d", i),
			"pagination test",
			cloudfront.TrustStoreCertificateBundle{
				InlineCertificateBundle: "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----",
			},
			nil,
		)
		require.NoError(t, err)
	}

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.TrustStoreSummary, *string) {
			out, err := client.ListTrustStores(
				t.Context(), &cfsdk.ListTrustStoresInput{Marker: marker, MaxItems: aws.Int32(maxItems)},
			)
			require.NoError(t, err)

			return out.TrustStoreList, out.NextMarker
		},
		func(s types.TrustStoreSummary) string { return aws.ToString(s.Id) },
	)
}

// TestListConflictingAliases_SDKRoundTrip_Pagination also proves ListConflictingAliasesByDomain
// (distributions.go) no longer returns map-iteration order: before the fix it ranged
// b.distributionWebACLs-style state with zero sort calls, so a paginated cursor built on that
// order would drop or duplicate records across a page boundary.
func TestListConflictingAliases_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	firstDist, err := backend.CreateDistribution("pg-ca-owner", "", true, nil)
	require.NoError(t, err)

	const total = 25
	for i := range total {
		d, createErr := backend.CreateDistribution(fmt.Sprintf("pg-ca-%02d", i), "", true, nil)
		require.NoError(t, createErr)
		require.NoError(t, backend.AssociateAlias(d.ID, "pg-conflict.example.com"))
	}

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.ConflictingAlias, *string) {
			out, listErr := client.ListConflictingAliases(t.Context(), &cfsdk.ListConflictingAliasesInput{
				Alias:          aws.String("pg-conflict.example.com"),
				DistributionId: aws.String(firstDist.ID),
				Marker:         marker,
				MaxItems:       aws.Int32(maxItems),
			})
			require.NoError(t, listErr)
			require.NotNil(t, out.ConflictingAliasesList)

			return out.ConflictingAliasesList.Items, out.ConflictingAliasesList.NextMarker
		},
		func(a types.ConflictingAlias) string { return aws.ToString(a.DistributionId) },
	)
}

// TestListDomainConflicts_SDKRoundTrip_Pagination also proves findDomainConflicts
// (distribution_tenants.go) sorts its combined tenant+distribution results by ResourceID --
// without that sort, a paginated cursor over the two concatenated, differently-ordered halves
// could drop or duplicate records across a page boundary.
func TestListDomainConflicts_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	validationDist, err := backend.CreateDistribution("pg-dc-owner", "", true, nil)
	require.NoError(t, err)

	const total = 25
	for i := range total {
		d, createErr := backend.CreateDistribution(fmt.Sprintf("pg-dc-%02d", i), "", true, nil)
		require.NoError(t, createErr)
		require.NoError(t, backend.AssociateAlias(d.ID, "pg-dc-conflict.example.com"))
	}

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.DomainConflict, *string) {
			out, listErr := client.ListDomainConflicts(t.Context(), &cfsdk.ListDomainConflictsInput{
				Domain: aws.String("pg-dc-conflict.example.com"),
				DomainControlValidationResource: &types.DistributionResourceId{
					DistributionId: aws.String(validationDist.ID),
				},
				Marker:   marker,
				MaxItems: aws.Int32(maxItems),
			})
			require.NoError(t, listErr)

			return out.DomainConflicts, out.NextMarker
		},
		func(dc types.DomainConflict) string { return aws.ToString(dc.ResourceId) },
	)
}

// --- ListDistributionsBy* family: three distinct output shapes ---

// distsWithTokenCount matches every caller's `total` below (all use it as the pagination
// fixture size).
const distsWithTokenCount = 25

func createDistsWithToken(t *testing.T, backend *cloudfront.InMemoryBackend, prefix, token string) {
	t.Helper()

	for i := range distsWithTokenCount {
		body := fmt.Appendf(nil,
			`<DistributionConfig><CallerReference>%s-%02d</CallerReference><Enabled>true</Enabled>`+
				`<Marker>%s</Marker></DistributionConfig>`,
			prefix, i, token,
		)
		_, err := backend.CreateDistribution(fmt.Sprintf("%s-%02d", prefix, i), "", true, body)
		require.NoError(t, err)
	}
}

func TestListDistributionsByCachePolicyId_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	createDistsWithToken(t, backend, "pg-dbcp", "pg-shared-cache-policy-01")

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]string, *string) {
			out, err := client.ListDistributionsByCachePolicyId(
				t.Context(),
				&cfsdk.ListDistributionsByCachePolicyIdInput{
					CachePolicyId: aws.String(
						"pg-shared-cache-policy-01",
					), Marker: marker, MaxItems: aws.Int32(maxItems),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, out.DistributionIdList)

			return out.DistributionIdList.Items, out.DistributionIdList.NextMarker
		},
		func(id string) string { return id },
	)
}

func TestListDistributionsByKeyGroup_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	createDistsWithToken(t, backend, "pg-dbkg", "pg-shared-key-group-01")

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]string, *string) {
			out, err := client.ListDistributionsByKeyGroup(t.Context(), &cfsdk.ListDistributionsByKeyGroupInput{
				KeyGroupId: aws.String("pg-shared-key-group-01"), Marker: marker, MaxItems: aws.Int32(maxItems),
			})
			require.NoError(t, err)
			require.NotNil(t, out.DistributionIdList)

			return out.DistributionIdList.Items, out.DistributionIdList.NextMarker
		},
		func(id string) string { return id },
	)
}

func TestListDistributionsByOriginRequestPolicyId_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	createDistsWithToken(t, backend, "pg-dborp", "pg-shared-orp-01")

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]string, *string) {
			out, err := client.ListDistributionsByOriginRequestPolicyId(
				t.Context(), &cfsdk.ListDistributionsByOriginRequestPolicyIdInput{
					OriginRequestPolicyId: aws.String(
						"pg-shared-orp-01",
					), Marker: marker, MaxItems: aws.Int32(maxItems),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, out.DistributionIdList)

			return out.DistributionIdList.Items, out.DistributionIdList.NextMarker
		},
		func(id string) string { return id },
	)
}

func TestListDistributionsByResponseHeadersPolicyId_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	createDistsWithToken(t, backend, "pg-dbrhp", "pg-shared-rhp-01")

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]string, *string) {
			out, err := client.ListDistributionsByResponseHeadersPolicyId(
				t.Context(), &cfsdk.ListDistributionsByResponseHeadersPolicyIdInput{
					ResponseHeadersPolicyId: aws.String(
						"pg-shared-rhp-01",
					), Marker: marker, MaxItems: aws.Int32(maxItems),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, out.DistributionIdList)

			return out.DistributionIdList.Items, out.DistributionIdList.NextMarker
		},
		func(id string) string { return id },
	)
}

func TestListDistributionsByVpcOriginId_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	createDistsWithToken(t, backend, "pg-dbvo", "pg-shared-vpc-origin-01")

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]string, *string) {
			out, err := client.ListDistributionsByVpcOriginId(t.Context(), &cfsdk.ListDistributionsByVpcOriginIdInput{
				VpcOriginId: aws.String("pg-shared-vpc-origin-01"), Marker: marker, MaxItems: aws.Int32(maxItems),
			})
			require.NoError(t, err)
			require.NotNil(t, out.DistributionIdList)

			return out.DistributionIdList.Items, out.DistributionIdList.NextMarker
		},
		func(id string) string { return id },
	)
}

func TestListDistributionsByAnycastIpListId_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	createDistsWithToken(t, backend, "pg-dbail", "pg-shared-anycast-01")

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.DistributionSummary, *string) {
			out, err := client.ListDistributionsByAnycastIpListId(
				t.Context(), &cfsdk.ListDistributionsByAnycastIpListIdInput{
					AnycastIpListId: aws.String("pg-shared-anycast-01"), Marker: marker, MaxItems: aws.Int32(maxItems),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, out.DistributionList)

			return out.DistributionList.Items, out.DistributionList.NextMarker
		},
		func(s types.DistributionSummary) string { return aws.ToString(s.Id) },
	)
}

func TestListDistributionsByConnectionFunction_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	createDistsWithToken(t, backend, "pg-dbcf", "pg-shared-connfn-01")

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.DistributionSummary, *string) {
			out, err := client.ListDistributionsByConnectionFunction(
				t.Context(), &cfsdk.ListDistributionsByConnectionFunctionInput{
					ConnectionFunctionIdentifier: aws.String(
						"pg-shared-connfn-01",
					), Marker: marker, MaxItems: aws.Int32(maxItems),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, out.DistributionList)

			return out.DistributionList.Items, out.DistributionList.NextMarker
		},
		func(s types.DistributionSummary) string { return aws.ToString(s.Id) },
	)
}

func TestListDistributionsByConnectionMode_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	createDistsWithToken(t, backend, "pg-dbcm", "direct")

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.DistributionSummary, *string) {
			out, err := client.ListDistributionsByConnectionMode(
				t.Context(), &cfsdk.ListDistributionsByConnectionModeInput{
					ConnectionMode: types.ConnectionModeDirect, Marker: marker, MaxItems: aws.Int32(maxItems),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, out.DistributionList)

			return out.DistributionList.Items, out.DistributionList.NextMarker
		},
		func(s types.DistributionSummary) string { return aws.ToString(s.Id) },
	)
}

func TestListDistributionsByTrustStore_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	createDistsWithToken(t, backend, "pg-dbts", "pg-shared-truststore-01")

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.DistributionSummary, *string) {
			out, err := client.ListDistributionsByTrustStore(t.Context(), &cfsdk.ListDistributionsByTrustStoreInput{
				TrustStoreIdentifier: aws.String(
					"pg-shared-truststore-01",
				), Marker: marker, MaxItems: aws.Int32(maxItems),
			})
			require.NoError(t, err)
			require.NotNil(t, out.DistributionList)

			return out.DistributionList.Items, out.DistributionList.NextMarker
		},
		func(s types.DistributionSummary) string { return aws.ToString(s.Id) },
	)
}

func TestListDistributionsByWebACLId_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	for i := range total {
		d, err := backend.CreateDistribution(fmt.Sprintf("pg-dbwacl-%02d", i), "", true, nil)
		require.NoError(t, err)
		require.NoError(t, backend.AssociateDistributionWebACL(d.ID, "pg-shared-webacl-01"))
	}

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.DistributionSummary, *string) {
			out, err := client.ListDistributionsByWebACLId(t.Context(), &cfsdk.ListDistributionsByWebACLIdInput{
				WebACLId: aws.String("pg-shared-webacl-01"), Marker: marker, MaxItems: aws.Int32(maxItems),
			})
			require.NoError(t, err)
			require.NotNil(t, out.DistributionList)

			return out.DistributionList.Items, out.DistributionList.NextMarker
		},
		func(s types.DistributionSummary) string { return aws.ToString(s.Id) },
	)
}

// TestListDistributionsByRealtimeLogConfig_SDKRoundTrip_Pagination also proves Marker/MaxItems
// are read correctly when they travel in the request body rather than the query string
// (cloudfront@v1.67.4 serializers.go: awsRestxml_serializeOpDocumentListDistributionsByRealtimeLogConfigInput).
func TestListDistributionsByRealtimeLogConfig_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	createDistsWithToken(
		t,
		backend,
		"pg-dbrlc",
		"arn:aws:cloudfront::123456789012:realtime-log-config/pg-shared-01",
	)

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.DistributionSummary, *string) {
			out, err := client.ListDistributionsByRealtimeLogConfig(
				t.Context(), &cfsdk.ListDistributionsByRealtimeLogConfigInput{
					RealtimeLogConfigArn: aws.String(
						"arn:aws:cloudfront::123456789012:realtime-log-config/pg-shared-01",
					),
					Marker:   marker,
					MaxItems: aws.Int32(maxItems),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, out.DistributionList)

			return out.DistributionList.Items, out.DistributionList.NextMarker
		},
		func(s types.DistributionSummary) string { return aws.ToString(s.Id) },
	)
}

func TestListDistributionsByOwnedResource_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestCloudFrontClient(t, cloudfront.NewHandler(backend))

	const total = 25
	createDistsWithToken(t, backend, "pg-dbor", "arn:aws:s3:::pg-shared-owned-resource-01")

	assertPaginatesAllRecords(t, total, 10,
		func(marker *string, maxItems int32) ([]types.DistributionIdOwner, *string) {
			out, err := client.ListDistributionsByOwnedResource(
				t.Context(), &cfsdk.ListDistributionsByOwnedResourceInput{
					ResourceArn: aws.String(
						"arn:aws:s3:::pg-shared-owned-resource-01",
					), Marker: marker, MaxItems: aws.Int32(maxItems),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, out.DistributionList)

			return out.DistributionList.Items, out.DistributionList.NextMarker
		},
		func(o types.DistributionIdOwner) string { return aws.ToString(o.DistributionId) },
	)
}
