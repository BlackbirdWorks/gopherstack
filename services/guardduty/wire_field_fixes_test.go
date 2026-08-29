package guardduty_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	guarddutysdk "github.com/aws/aws-sdk-go-v2/service/guardduty"
	"github.com/aws/aws-sdk-go-v2/service/guardduty/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

// TestGetFilter_TimestampsAndVersion proves GetFilter emits createdAt/
// updatedAt/version -- the backend already tracked all three (Filter.CreatedAt/
// UpdatedAt/Version) but the handler never emitted them, so a real client's
// typed fields were always nil/zero regardless of backend state
// (gopherstack-6flj).
func TestGetFilter_TimestampsAndVersion(t *testing.T) {
	t.Parallel()

	backend := guardduty.NewInMemoryBackend("123456789012", "us-east-1")
	h := guardduty.NewHandler(backend)
	client := newTestGuardDutyClient(t, h)

	det, err := client.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{Enable: aws.Bool(true)})
	require.NoError(t, err)
	detectorID := aws.ToString(det.DetectorId)

	_, err = client.CreateFilter(t.Context(), &guarddutysdk.CreateFilterInput{
		DetectorId: aws.String(detectorID),
		Name:       aws.String("wire-filter"),
		Action:     types.FilterActionNoop,
		FindingCriteria: &types.FindingCriteria{
			Criterion: map[string]types.Condition{"severity": {Gte: aws.Int32(4)}},
		},
	})
	require.NoError(t, err)

	first, err := client.GetFilter(t.Context(), &guarddutysdk.GetFilterInput{
		DetectorId: aws.String(detectorID),
		FilterName: aws.String("wire-filter"),
	})
	require.NoError(t, err)
	require.NotNil(t, first.CreatedAt)
	require.NotNil(t, first.UpdatedAt)
	require.NotNil(t, first.Version)
	assert.EqualValues(t, 1, aws.ToInt64(first.Version))

	_, err = client.UpdateFilter(t.Context(), &guarddutysdk.UpdateFilterInput{
		DetectorId: aws.String(detectorID),
		FilterName: aws.String("wire-filter"),
		Rank:       aws.Int32(3),
	})
	require.NoError(t, err)

	second, err := client.GetFilter(t.Context(), &guarddutysdk.GetFilterInput{
		DetectorId: aws.String(detectorID),
		FilterName: aws.String("wire-filter"),
	})
	require.NoError(t, err)
	require.NotNil(t, second.Version)
	assert.EqualValues(t, 2, aws.ToInt64(second.Version))
}

// TestIPSetAndThreatIntelSet_ExpectedBucketOwner proves ExpectedBucketOwner
// (a real member on both Create/UpdateIPSetInput and Create/
// UpdateThreatIntelSetInput, and on GetIPSetOutput/GetThreatIntelSetOutput)
// round-trips through create and update -- gopherstack's IPSet/ThreatIntelSet
// models had no field for it at all, silently dropping a value a real client
// supplied, even though the sibling ThreatEntitySet/TrustedEntitySet types in
// the same service already modeled it correctly (gopherstack-6flj).
func TestIPSetAndThreatIntelSet_ExpectedBucketOwner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, c *guarddutysdk.Client, detectorID string) string
		get    func(t *testing.T, c *guarddutysdk.Client, detectorID, id string) *string
		update func(t *testing.T, c *guarddutysdk.Client, detectorID, id string)
		name   string
	}{
		{
			name: "ip set",
			create: func(t *testing.T, c *guarddutysdk.Client, detectorID string) string {
				t.Helper()
				out, err := c.CreateIPSet(t.Context(), &guarddutysdk.CreateIPSetInput{
					DetectorId:          aws.String(detectorID),
					Name:                aws.String("wire-ipset"),
					Format:              types.IpSetFormatTxt,
					Location:            aws.String("s3://bucket/ipset.txt"),
					Activate:            aws.Bool(false),
					ExpectedBucketOwner: aws.String("111122223333"),
				})
				require.NoError(t, err)

				return aws.ToString(out.IpSetId)
			},
			get: func(t *testing.T, c *guarddutysdk.Client, detectorID, id string) *string {
				t.Helper()
				out, err := c.GetIPSet(t.Context(), &guarddutysdk.GetIPSetInput{
					DetectorId: aws.String(detectorID),
					IpSetId:    aws.String(id),
				})
				require.NoError(t, err)

				return out.ExpectedBucketOwner
			},
			update: func(t *testing.T, c *guarddutysdk.Client, detectorID, id string) {
				t.Helper()
				_, err := c.UpdateIPSet(t.Context(), &guarddutysdk.UpdateIPSetInput{
					DetectorId:          aws.String(detectorID),
					IpSetId:             aws.String(id),
					ExpectedBucketOwner: aws.String("444455556666"),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "threat intel set",
			create: func(t *testing.T, c *guarddutysdk.Client, detectorID string) string {
				t.Helper()
				out, err := c.CreateThreatIntelSet(t.Context(), &guarddutysdk.CreateThreatIntelSetInput{
					DetectorId:          aws.String(detectorID),
					Name:                aws.String("wire-tiset"),
					Format:              types.ThreatIntelSetFormatTxt,
					Location:            aws.String("s3://bucket/tiset.txt"),
					Activate:            aws.Bool(false),
					ExpectedBucketOwner: aws.String("111122223333"),
				})
				require.NoError(t, err)

				return aws.ToString(out.ThreatIntelSetId)
			},
			get: func(t *testing.T, c *guarddutysdk.Client, detectorID, id string) *string {
				t.Helper()
				out, err := c.GetThreatIntelSet(t.Context(), &guarddutysdk.GetThreatIntelSetInput{
					DetectorId:       aws.String(detectorID),
					ThreatIntelSetId: aws.String(id),
				})
				require.NoError(t, err)

				return out.ExpectedBucketOwner
			},
			update: func(t *testing.T, c *guarddutysdk.Client, detectorID, id string) {
				t.Helper()
				_, err := c.UpdateThreatIntelSet(t.Context(), &guarddutysdk.UpdateThreatIntelSetInput{
					DetectorId:          aws.String(detectorID),
					ThreatIntelSetId:    aws.String(id),
					ExpectedBucketOwner: aws.String("444455556666"),
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := guardduty.NewInMemoryBackend("123456789012", "us-east-1")
			h := guardduty.NewHandler(backend)
			client := newTestGuardDutyClient(t, h)

			det, err := client.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{Enable: aws.Bool(true)})
			require.NoError(t, err)
			detectorID := aws.ToString(det.DetectorId)

			id := tt.create(t, client, detectorID)
			assert.Equal(t, "111122223333", aws.ToString(tt.get(t, client, detectorID, id)))

			tt.update(t, client, detectorID, id)
			assert.Equal(t, "444455556666", aws.ToString(tt.get(t, client, detectorID, id)))
		})
	}
}

// TestOrganizationConfiguration_AutoEnableOrganizationMembers proves
// UpdateOrganizationConfiguration/DescribeOrganizationConfiguration round-trip
// AutoEnableOrganizationMembers, the non-deprecated replacement for AutoEnable
// that gopherstack's OrgConfig model had no slot for at all (gopherstack-6flj).
func TestOrganizationConfiguration_AutoEnableOrganizationMembers(t *testing.T) {
	t.Parallel()

	backend := guardduty.NewInMemoryBackend("123456789012", "us-east-1")
	h := guardduty.NewHandler(backend)
	client := newTestGuardDutyClient(t, h)

	det, err := client.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{Enable: aws.Bool(true)})
	require.NoError(t, err)
	detectorID := aws.ToString(det.DetectorId)

	_, err = client.UpdateOrganizationConfiguration(t.Context(), &guarddutysdk.UpdateOrganizationConfigurationInput{
		DetectorId:                    aws.String(detectorID),
		AutoEnableOrganizationMembers: types.AutoEnableMembersNew,
	})
	require.NoError(t, err)

	got, err := client.DescribeOrganizationConfiguration(
		t.Context(), &guarddutysdk.DescribeOrganizationConfigurationInput{DetectorId: aws.String(detectorID)},
	)
	require.NoError(t, err)
	assert.Equal(t, types.AutoEnableMembersNew, got.AutoEnableOrganizationMembers)
}

// TestGetUsageStatistics_SumByDataSource_RealDataSourceValues proves
// SumByDataSource emits real types.DataSource enum values, not the
// DetectorFeature name reused verbatim under the wrong enum's key
// (gopherstack-6flj/21my). types.DataSource has exactly six members
// (FLOW_LOGS/CLOUD_TRAIL/DNS_LOGS/S3_LOGS/KUBERNETES_AUDIT_LOGS/
// EC2_MALWARE_SCAN, types/enums.go) -- distinct from types.UsageFeature,
// which uses different names for the same underlying concept (S3_DATA_EVENTS
// vs S3_LOGS, EKS_AUDIT_LOGS vs KUBERNETES_AUDIT_LOGS) plus several
// feature-only members with no DataSource counterpart at all
// (EBS_MALWARE_PROTECTION, RDS_LOGIN_EVENTS, ...). Before the fix,
// usageByFeature was called with the same detector-feature-name list for
// both sumByFeature and sumByDataSource, so an enabled S3_DATA_EVENTS
// feature produced a sumByDataSource entry of {dataSource:
// "S3_DATA_EVENTS"} -- a string with no equivalent among DataSource's six
// real values, decoded by a real client into a DataSource holding a value
// it can never actually be.
func TestGetUsageStatistics_SumByDataSource_RealDataSourceValues(t *testing.T) {
	t.Parallel()

	backend := guardduty.NewInMemoryBackend("123456789012", "us-east-1")
	h := guardduty.NewHandler(backend)
	client := newTestGuardDutyClient(t, h)

	det, err := client.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{
		Enable: aws.Bool(true),
		Features: []types.DetectorFeatureConfiguration{
			{Name: types.DetectorFeatureS3DataEvents, Status: types.FeatureStatusEnabled},
			{Name: types.DetectorFeatureEksAuditLogs, Status: types.FeatureStatusEnabled},
		},
	})
	require.NoError(t, err)
	detectorID := aws.ToString(det.DetectorId)

	out, err := client.GetUsageStatistics(t.Context(), &guarddutysdk.GetUsageStatisticsInput{
		DetectorId:         aws.String(detectorID),
		UsageStatisticType: types.UsageStatisticTypeSumByDataSource,
		UsageCriteria:      &types.UsageCriteria{},
	})
	require.NoError(t, err)
	require.NotNil(t, out.UsageStatistics)
	require.NotEmpty(t, out.UsageStatistics.SumByDataSource)

	seen := make(map[types.DataSource]bool)
	for _, entry := range out.UsageStatistics.SumByDataSource {
		seen[entry.DataSource] = true
	}

	assert.True(t, seen[types.DataSourceS3Logs], "expected S3_LOGS (from the S3_DATA_EVENTS feature), got %v", seen)
	assert.True(t,
		seen[types.DataSourceKubernetesAuditLogs],
		"expected KUBERNETES_AUDIT_LOGS (from the EKS_AUDIT_LOGS feature), got %v", seen,
	)
	assert.False(t, seen[types.DataSource("S3_DATA_EVENTS")], "S3_DATA_EVENTS is not a real DataSource value")
	assert.False(t, seen[types.DataSource("EKS_AUDIT_LOGS")], "EKS_AUDIT_LOGS is not a real DataSource value")
}

// TestListMalwareProtectionPlans_NoInventedArn proves ListMalwareProtectionPlans
// only ever emits malwareProtectionPlanId per entry (gopherstack-21my).
// types.MalwareProtectionPlanSummary (the real ListMalwareProtectionPlansOutput
// item shape) has exactly one member; arn is real on the singular
// GetMalwareProtectionPlanOutput but does not exist on the list summary at
// all, so it must be checked at the raw body -- the typed SDK client has no
// field to decode an invented "arn" key into, and would silently discard it.
func TestListMalwareProtectionPlans_NoInventedArn(t *testing.T) {
	t.Parallel()

	backend := guardduty.NewInMemoryBackend("123456789012", "us-east-1")
	h := guardduty.NewHandler(backend)
	client := newTestGuardDutyClient(t, h)

	det, err := client.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{Enable: aws.Bool(true)})
	require.NoError(t, err)
	_ = det

	_, err = client.CreateMalwareProtectionPlan(t.Context(), &guarddutysdk.CreateMalwareProtectionPlanInput{
		Role: aws.String("arn:aws:iam::123456789012:role/malware-role"),
		ProtectedResource: &types.CreateProtectedResource{
			S3Bucket: &types.CreateS3BucketResource{BucketName: aws.String("wire-mpp-bucket")},
		},
	})
	require.NoError(t, err)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/malware-protection-plan", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var raw struct {
		MalwareProtectionPlans []map[string]any `json:"malwareProtectionPlans"`
	}
	require.NoError(t, json.Unmarshal(body, &raw))
	require.NotEmpty(t, raw.MalwareProtectionPlans)

	for _, entry := range raw.MalwareProtectionPlans {
		_, hasArn := entry["arn"]
		assert.False(t, hasArn, "malwareProtectionPlans entry must not carry an invented arn key: %v", entry)
		assert.Contains(t, entry, "malwareProtectionPlanId")
	}
}

// TestListFilters_Pagination proves ListFiltersInput's MaxResults/NextToken
// (real HTTP query params -- aws-sdk-go-v2/service/guardduty@v1.85.4
// serializers.go's awsRestjson1_serializeOpHttpBindingsListFiltersInput
// encoder.SetQuery calls) were never read at all: the handler took no query
// parameter and always returned every filter in one page.
func TestListFilters_Pagination(t *testing.T) {
	t.Parallel()

	backend := guardduty.NewInMemoryBackend("123456789012", "us-east-1")
	det, err := backend.CreateDetector(true, "", nil, nil)
	require.NoError(t, err)

	_, err = backend.CreateFilter(det.DetectorID, "filter-a", "", "NOOP", 1, map[string]any{}, nil)
	require.NoError(t, err)
	_, err = backend.CreateFilter(det.DetectorID, "filter-b", "", "NOOP", 1, map[string]any{}, nil)
	require.NoError(t, err)

	client := newTestGuardDutyClient(t, guardduty.NewHandler(backend))

	page1, err := client.ListFilters(t.Context(), &guarddutysdk.ListFiltersInput{
		DetectorId: aws.String(det.DetectorID),
		MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.FilterNames, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken), "a second page must exist")

	page2, err := client.ListFilters(t.Context(), &guarddutysdk.ListFiltersInput{
		DetectorId: aws.String(det.DetectorID),
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.FilterNames, 1)
	assert.Empty(t, aws.ToString(page2.NextToken), "no third page")
	assert.NotEqual(t, page1.FilterNames[0], page2.FilterNames[0])
}

// TestListIPSets_Pagination mirrors TestListFilters_Pagination for ListIPSets.
func TestListIPSets_Pagination(t *testing.T) {
	t.Parallel()

	backend := guardduty.NewInMemoryBackend("123456789012", "us-east-1")
	det, err := backend.CreateDetector(true, "", nil, nil)
	require.NoError(t, err)

	_, err = backend.CreateIPSet(det.DetectorID, "ipset-a", "TXT", "s3://bucket/a", true, nil, "")
	require.NoError(t, err)
	_, err = backend.CreateIPSet(det.DetectorID, "ipset-b", "TXT", "s3://bucket/b", true, nil, "")
	require.NoError(t, err)

	client := newTestGuardDutyClient(t, guardduty.NewHandler(backend))

	page1, err := client.ListIPSets(t.Context(), &guarddutysdk.ListIPSetsInput{
		DetectorId: aws.String(det.DetectorID),
		MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.IpSetIds, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.ListIPSets(t.Context(), &guarddutysdk.ListIPSetsInput{
		DetectorId: aws.String(det.DetectorID),
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.IpSetIds, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
	assert.NotEqual(t, page1.IpSetIds[0], page2.IpSetIds[0])
}

// TestListThreatIntelSets_Pagination mirrors TestListFilters_Pagination for
// ListThreatIntelSets.
func TestListThreatIntelSets_Pagination(t *testing.T) {
	t.Parallel()

	backend := guardduty.NewInMemoryBackend("123456789012", "us-east-1")
	det, err := backend.CreateDetector(true, "", nil, nil)
	require.NoError(t, err)

	_, err = backend.CreateThreatIntelSet(det.DetectorID, "ti-a", "TXT", "s3://bucket/a", true, nil, "")
	require.NoError(t, err)
	_, err = backend.CreateThreatIntelSet(det.DetectorID, "ti-b", "TXT", "s3://bucket/b", true, nil, "")
	require.NoError(t, err)

	client := newTestGuardDutyClient(t, guardduty.NewHandler(backend))

	page1, err := client.ListThreatIntelSets(t.Context(), &guarddutysdk.ListThreatIntelSetsInput{
		DetectorId: aws.String(det.DetectorID),
		MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.ThreatIntelSetIds, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.ListThreatIntelSets(t.Context(), &guarddutysdk.ListThreatIntelSetsInput{
		DetectorId: aws.String(det.DetectorID),
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.ThreatIntelSetIds, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
	assert.NotEqual(t, page1.ThreatIntelSetIds[0], page2.ThreatIntelSetIds[0])
}

// TestListThreatEntitySets_Pagination mirrors TestListFilters_Pagination for
// ListThreatEntitySets.
func TestListThreatEntitySets_Pagination(t *testing.T) {
	t.Parallel()

	backend := guardduty.NewInMemoryBackend("123456789012", "us-east-1")
	det, err := backend.CreateDetector(true, "", nil, nil)
	require.NoError(t, err)

	_, err = backend.CreateThreatEntitySet(det.DetectorID, "te-a", "TXT", "s3://bucket/a", true, nil, "")
	require.NoError(t, err)
	_, err = backend.CreateThreatEntitySet(det.DetectorID, "te-b", "TXT", "s3://bucket/b", true, nil, "")
	require.NoError(t, err)

	client := newTestGuardDutyClient(t, guardduty.NewHandler(backend))

	page1, err := client.ListThreatEntitySets(t.Context(), &guarddutysdk.ListThreatEntitySetsInput{
		DetectorId: aws.String(det.DetectorID),
		MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.ThreatEntitySetIds, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.ListThreatEntitySets(t.Context(), &guarddutysdk.ListThreatEntitySetsInput{
		DetectorId: aws.String(det.DetectorID),
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.ThreatEntitySetIds, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
	assert.NotEqual(t, page1.ThreatEntitySetIds[0], page2.ThreatEntitySetIds[0])
}

// TestListTrustedEntitySets_Pagination mirrors TestListFilters_Pagination for
// ListTrustedEntitySets.
func TestListTrustedEntitySets_Pagination(t *testing.T) {
	t.Parallel()

	backend := guardduty.NewInMemoryBackend("123456789012", "us-east-1")
	det, err := backend.CreateDetector(true, "", nil, nil)
	require.NoError(t, err)

	_, err = backend.CreateTrustedEntitySet(det.DetectorID, "tr-a", "TXT", "s3://bucket/a", true, nil, "")
	require.NoError(t, err)
	_, err = backend.CreateTrustedEntitySet(det.DetectorID, "tr-b", "TXT", "s3://bucket/b", true, nil, "")
	require.NoError(t, err)

	client := newTestGuardDutyClient(t, guardduty.NewHandler(backend))

	page1, err := client.ListTrustedEntitySets(t.Context(), &guarddutysdk.ListTrustedEntitySetsInput{
		DetectorId: aws.String(det.DetectorID),
		MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.TrustedEntitySetIds, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.ListTrustedEntitySets(t.Context(), &guarddutysdk.ListTrustedEntitySetsInput{
		DetectorId: aws.String(det.DetectorID),
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.TrustedEntitySetIds, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
	assert.NotEqual(t, page1.TrustedEntitySetIds[0], page2.TrustedEntitySetIds[0])
}

// TestListPublishingDestinations_Pagination mirrors TestListFilters_Pagination
// for ListPublishingDestinations.
func TestListPublishingDestinations_Pagination(t *testing.T) {
	t.Parallel()

	backend := guardduty.NewInMemoryBackend("123456789012", "us-east-1")
	det, err := backend.CreateDetector(true, "", nil, nil)
	require.NoError(t, err)

	dest1, err := backend.CreatePublishingDestination(det.DetectorID, "S3", guardduty.DestinationProperties{
		DestinationArn: "arn:aws:s3:::bucket-a",
	}, nil)
	require.NoError(t, err)
	dest2, err := backend.CreatePublishingDestination(det.DetectorID, "S3", guardduty.DestinationProperties{
		DestinationArn: "arn:aws:s3:::bucket-b",
	}, nil)
	require.NoError(t, err)

	client := newTestGuardDutyClient(t, guardduty.NewHandler(backend))

	page1, err := client.ListPublishingDestinations(t.Context(), &guarddutysdk.ListPublishingDestinationsInput{
		DetectorId: aws.String(det.DetectorID),
		MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.Destinations, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.ListPublishingDestinations(t.Context(), &guarddutysdk.ListPublishingDestinationsInput{
		DetectorId: aws.String(det.DetectorID),
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Destinations, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))

	seen := map[string]bool{
		aws.ToString(page1.Destinations[0].DestinationId): true,
		aws.ToString(page2.Destinations[0].DestinationId): true,
	}
	assert.True(t, seen[dest1.DestinationID])
	assert.True(t, seen[dest2.DestinationID])
}

// TestListOrganizationAdminAccounts_Pagination mirrors
// TestListFilters_Pagination for ListOrganizationAdminAccounts.
func TestListOrganizationAdminAccounts_Pagination(t *testing.T) {
	t.Parallel()

	backend := guardduty.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, backend.EnableOrganizationAdminAccount("111111111111"))
	require.NoError(t, backend.EnableOrganizationAdminAccount("222222222222"))

	client := newTestGuardDutyClient(t, guardduty.NewHandler(backend))

	page1, err := client.ListOrganizationAdminAccounts(t.Context(), &guarddutysdk.ListOrganizationAdminAccountsInput{
		MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.AdminAccounts, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.ListOrganizationAdminAccounts(t.Context(), &guarddutysdk.ListOrganizationAdminAccountsInput{
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.AdminAccounts, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
	assert.NotEqual(
		t,
		aws.ToString(page1.AdminAccounts[0].AdminAccountId),
		aws.ToString(page2.AdminAccounts[0].AdminAccountId),
	)
}

// malwareProtectionPlanPaginationTestCount is one more than
// ListMalwareProtectionPlansInput's documented default page size (100 plans,
// per its own NextToken doc comment) -- ListMalwareProtectionPlans has no
// MaxResults on the real wire at all, so this is the only way to force a
// real second page and prove NextToken is actually honored rather than the
// full set always coming back in one response.
const malwareProtectionPlanPaginationTestCount = 101

// TestListMalwareProtectionPlans_Pagination mirrors
// TestListFilters_Pagination for ListMalwareProtectionPlans, which has no
// MaxResults on the real wire (NextToken only, fixed 100-per-page default).
func TestListMalwareProtectionPlans_Pagination(t *testing.T) {
	t.Parallel()

	backend := guardduty.NewInMemoryBackend("123456789012", "us-east-1")

	ids := make(map[string]bool, malwareProtectionPlanPaginationTestCount)

	for range malwareProtectionPlanPaginationTestCount {
		plan, err := backend.CreateMalwareProtectionPlan(
			"arn:aws:iam::123456789012:role/scan-role", map[string]any{}, map[string]any{}, nil,
		)
		require.NoError(t, err)
		ids[plan.MalwareProtectionPlanID] = true
	}

	client := newTestGuardDutyClient(t, guardduty.NewHandler(backend))

	page1, err := client.ListMalwareProtectionPlans(t.Context(), &guarddutysdk.ListMalwareProtectionPlansInput{})
	require.NoError(t, err)
	require.Len(t, page1.MalwareProtectionPlans, 100)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.ListMalwareProtectionPlans(t.Context(), &guarddutysdk.ListMalwareProtectionPlansInput{
		NextToken: page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.MalwareProtectionPlans, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))

	for _, p := range page1.MalwareProtectionPlans {
		assert.True(t, ids[aws.ToString(p.MalwareProtectionPlanId)])
	}

	assert.True(t, ids[aws.ToString(page2.MalwareProtectionPlans[0].MalwareProtectionPlanId)])
}

// TestListInvitations_Pagination mirrors TestListFilters_Pagination for
// ListInvitations.
func TestListInvitations_Pagination(t *testing.T) {
	t.Parallel()

	backend := guardduty.NewInMemoryBackend("123456789012", "us-east-1")
	det, err := backend.CreateDetector(true, "", nil, nil)
	require.NoError(t, err)

	unprocessed, err := backend.InviteMembers(det.DetectorID, []string{"111111111111", "222222222222"})
	require.NoError(t, err)
	require.Empty(t, unprocessed)

	client := newTestGuardDutyClient(t, guardduty.NewHandler(backend))

	page1, err := client.ListInvitations(t.Context(), &guarddutysdk.ListInvitationsInput{
		MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.Invitations, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.ListInvitations(t.Context(), &guarddutysdk.ListInvitationsInput{
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Invitations, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
	assert.NotEqual(t, aws.ToString(page1.Invitations[0].AccountId), aws.ToString(page2.Invitations[0].AccountId))
}

// TestListMembers_Pagination mirrors TestListFilters_Pagination for
// ListMembers.
func TestListMembers_Pagination(t *testing.T) {
	t.Parallel()

	backend := guardduty.NewInMemoryBackend("123456789012", "us-east-1")
	det, err := backend.CreateDetector(true, "", nil, nil)
	require.NoError(t, err)

	created, unprocessed := backend.CreateMembers(det.DetectorID, []map[string]any{
		{"accountId": "111111111111", "email": "a@example.com"},
		{"accountId": "222222222222", "email": "b@example.com"},
	})
	require.Empty(t, unprocessed)
	require.Len(t, created, 2)

	client := newTestGuardDutyClient(t, guardduty.NewHandler(backend))

	page1, err := client.ListMembers(t.Context(), &guarddutysdk.ListMembersInput{
		DetectorId: aws.String(det.DetectorID),
		MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.Members, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.ListMembers(t.Context(), &guarddutysdk.ListMembersInput{
		DetectorId: aws.String(det.DetectorID),
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Members, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
	assert.NotEqual(t, aws.ToString(page1.Members[0].AccountId), aws.ToString(page2.Members[0].AccountId))
}
