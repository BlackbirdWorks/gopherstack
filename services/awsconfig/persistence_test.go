package awsconfig_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *awsconfig.InMemoryBackend) string
		verify func(t *testing.T, b *awsconfig.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *awsconfig.InMemoryBackend) string {
				err := b.PutConfigurationRecorder("test-recorder", "arn:aws:iam::000000000000:role/test", nil)
				if err != nil {
					return ""
				}

				return "test-recorder"
			},
			verify: func(t *testing.T, b *awsconfig.InMemoryBackend, id string) {
				t.Helper()

				recorders := b.DescribeConfigurationRecorders(nil)
				require.NotEmpty(t, recorders)
				assert.Equal(t, id, recorders[0].Name)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *awsconfig.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *awsconfig.InMemoryBackend, _ string) {
				t.Helper()

				recorders := b.DescribeConfigurationRecorders(nil)
				assert.Empty(t, recorders)
			},
		},
		{
			name: "stored_query_round_trip",
			setup: func(b *awsconfig.InMemoryBackend) string {
				require.NoError(t, b.PutStoredQuery("query-1"))

				return "query-1"
			},
			verify: func(t *testing.T, b *awsconfig.InMemoryBackend, id string) {
				t.Helper()

				q := b.GetStoredQuery(id)
				require.NotNil(t, q)
				assert.Equal(t, id, q.QueryName)
			},
		},
		{
			name: "retention_configuration_round_trip",
			setup: func(b *awsconfig.InMemoryBackend) string {
				require.NoError(t, b.PutRetentionConfiguration("retention-1", 30))

				return "retention-1"
			},
			verify: func(t *testing.T, b *awsconfig.InMemoryBackend, id string) {
				t.Helper()

				configs := b.DescribeRetentionConfigurations()
				require.Len(t, configs, 1)
				assert.Equal(t, id, configs[0].Name)
				assert.Equal(t, int32(30), configs[0].RetentionPeriodInDays)
			},
		},
		{
			name: "remediation_configuration_round_trip",
			setup: func(b *awsconfig.InMemoryBackend) string {
				require.NoError(t, b.PutRemediationConfigurations([]awsconfig.RemediationConfiguration{
					{ConfigRuleName: "rule-rem", TargetType: "SSM_DOCUMENT", TargetID: "doc-1"},
				}))

				return "rule-rem"
			},
			verify: func(t *testing.T, b *awsconfig.InMemoryBackend, id string) {
				t.Helper()

				configs := b.DescribeRemediationConfigurations(nil)
				require.Len(t, configs, 1)
				assert.Equal(t, id, configs[0].ConfigRuleName)
				assert.Equal(t, "doc-1", configs[0].TargetID)
			},
		},
		{
			name: "resource_evaluation_round_trip",
			setup: func(b *awsconfig.InMemoryBackend) string {
				return b.StartResourceEvaluation("AWS::S3::Bucket", "my-bucket", "DETAILED", `{"a":1}`)
			},
			verify: func(t *testing.T, b *awsconfig.InMemoryBackend, id string) {
				t.Helper()

				re := b.GetResourceEvaluationSummaryByID(id)
				require.NotNil(t, re)
				assert.Equal(t, "my-bucket", re.ResourceID)
			},
		},
		{
			name: "resource_config_round_trip",
			setup: func(b *awsconfig.InMemoryBackend) string {
				require.NoError(t, b.PutResourceConfig("AWS::S3::Bucket", "bucket-1", `{"a":1}`))

				return "bucket-1"
			},
			verify: func(t *testing.T, b *awsconfig.InMemoryBackend, id string) {
				t.Helper()

				items := b.ListDiscoveredResources("AWS::S3::Bucket")
				require.Len(t, items, 1)
				assert.Equal(t, id, items[0].ResourceID)
			},
		},
		{
			name: "connector_round_trip",
			setup: func(b *awsconfig.InMemoryBackend) string {
				arn, err := b.PutConnector(&awsconfig.ConnectorConfiguration{
					Azure: &awsconfig.AzureConnectorConfiguration{
						ClientIdentifier: "client-1", TenantIdentifier: "tenant-1",
					},
				}, nil)
				if err != nil {
					return ""
				}

				return arn
			},
			verify: func(t *testing.T, b *awsconfig.InMemoryBackend, id string) {
				t.Helper()

				got, err := b.GetConnector(id)
				require.NoError(t, err)
				assert.Equal(t, id, got.Arn)
			},
		},
		{
			name: "third_party_service_linked_recorder_round_trip",
			setup: func(b *awsconfig.InMemoryBackend) string {
				connArn, err := b.PutConnector(&awsconfig.ConnectorConfiguration{
					Azure: &awsconfig.AzureConnectorConfiguration{
						ClientIdentifier: "client-1", TenantIdentifier: "tenant-1",
					},
				}, nil)
				if err != nil {
					return ""
				}

				name, _, err := b.PutThirdPartyServiceLinkedConfigurationRecorder(
					"thirdparty.amazonaws.com", connArn,
					&awsconfig.ScopeConfiguration{ScopeType: "tenant", AllRegions: true}, nil,
				)
				if err != nil {
					return ""
				}

				return name
			},
			verify: func(t *testing.T, b *awsconfig.InMemoryBackend, id string) {
				t.Helper()

				recorders := b.DescribeConfigurationRecorders([]string{id})
				require.Len(t, recorders, 1)
				assert.Equal(t, "thirdparty.amazonaws.com", recorders[0].ServicePrincipal)
			},
		},
		{
			name: "rule_resource_evaluation_round_trip",
			setup: func(b *awsconfig.InMemoryBackend) string {
				require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-ext"}))
				require.NoError(t, b.PutExternalEvaluation(awsconfig.EvaluationResult{
					ConfigRuleName: "rule-ext",
					ComplianceType: "NON_COMPLIANT",
					ResourceType:   "AWS::S3::Bucket",
					ResourceID:     "bucket-ext",
					Annotation:     "missing encryption",
				}))

				return "rule-ext"
			},
			verify: func(t *testing.T, b *awsconfig.InMemoryBackend, id string) {
				t.Helper()

				byRule, err := b.GetComplianceDetailsByConfigRule(id, nil)
				require.NoError(t, err)
				require.Len(t, byRule, 1)
				assert.Equal(t, "bucket-ext", byRule[0].EvaluationResultIdentifier.EvaluationResultQualifier.ResourceID)
				assert.Equal(t, "NON_COMPLIANT", byRule[0].ComplianceType)

				byResource := b.GetComplianceDetailsByResource("AWS::S3::Bucket", "bucket-ext", nil)
				require.Len(t, byResource, 1)
				assert.Equal(t, id, byResource[0].EvaluationResultIdentifier.EvaluationResultQualifier.ConfigRuleName)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := awsconfig.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := awsconfig.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestAWSConfigHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := awsconfig.NewInMemoryBackend()
	h := awsconfig.NewHandler(backend)

	err := backend.PutConfigurationRecorder("snap-recorder", "arn:aws:iam::000000000000:role/test", nil)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := awsconfig.NewInMemoryBackend()
	freshH := awsconfig.NewHandler(fresh)
	require.NoError(t, freshH.Restore(t.Context(), snap))

	recorders := fresh.DescribeConfigurationRecorders(nil)
	assert.Len(t, recorders, 1)
}

func TestAWSConfigBackend_DeleteOperations(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	// Create a recorder and channel
	err := b.PutConfigurationRecorder("test-recorder", "arn:aws:iam::000000000000:role/test", nil)
	require.NoError(t, err)

	err = b.PutDeliveryChannel("test-channel", "my-bucket", "", "", nil)
	require.NoError(t, err)

	// Delete delivery channel
	err = b.DeleteDeliveryChannel("test-channel")
	require.NoError(t, err)

	channels := b.DescribeDeliveryChannels(nil)
	assert.Empty(t, channels)

	// Delete configuration recorder
	err = b.DeleteConfigurationRecorder("test-recorder")
	require.NoError(t, err)

	recorders := b.DescribeConfigurationRecorders(nil)
	assert.Empty(t, recorders)
}

func TestAWSConfigHandler_Routing(t *testing.T) {
	t.Parallel()

	h := awsconfig.NewHandler(awsconfig.NewInMemoryBackend())

	assert.Equal(t, "AWSConfig", h.Name())
	assert.Positive(t, h.MatchPriority())

	e := echo.New()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{"config target", "StarlingDoveService.PutConfigurationRecorder", true},
		{"other target", "SQS.SendMessage", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}
}

func TestAWSConfigHandler_DeleteOperations(t *testing.T) {
	t.Parallel()

	backend := awsconfig.NewInMemoryBackend()
	h := awsconfig.NewHandler(backend)

	// Put and then delete delivery channel via handler
	_ = backend.PutDeliveryChannel("test-channel", "my-bucket", "", "", nil)
	_ = backend.PutConfigurationRecorder("test-recorder", "arn:aws:iam::000000000000:role/test", nil)

	e := echo.New()

	// Delete delivery channel via handler
	body := `{"DeliveryChannelName":"test-channel"}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "StarlingDoveService.DeleteDeliveryChannel")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete config recorder via handler
	body2 := `{"ConfigurationRecorderName":"test-recorder"}`
	req2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body2))
	req2.Header.Set("X-Amz-Target", "StarlingDoveService.DeleteConfigurationRecorder")
	req2.Header.Set("Content-Type", "application/x-amz-json-1.1")
	rec2 := httptest.NewRecorder()
	c2 := e.NewContext(req2, rec2)
	err = h.Handler()(c2)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec2.Code)
}

func TestInMemoryBackend_Snapshot_AllMaps(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	require.NoError(t, b.PutConfigurationRecorder("rec", "arn:aws:iam::000:role/r", nil))
	require.NoError(t, b.PutDeliveryChannel("chan", "bucket", "", "", nil))
	require.NoError(t, b.PutAggregationAuthorization("123456789012", "us-east-1"))
	require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-x"}))
	require.NoError(t, b.PutConfigurationAggregator("agg-1", nil, nil))
	require.NoError(t, b.PutConformancePack("pack-1", "", "", ""))
	require.NoError(t, b.PutOrganizationConfigRule("org-rule-1"))
	require.NoError(t, b.PutOrganizationConformancePack("org-pack-1"))

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := awsconfig.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Verify all maps were restored
	recorders := fresh.DescribeConfigurationRecorders(nil)
	require.Len(t, recorders, 1)
	assert.Equal(t, "rec", recorders[0].Name)

	channels := fresh.DescribeDeliveryChannels(nil)
	require.Len(t, channels, 1)
	assert.Equal(t, "chan", channels[0].Name)

	auths := fresh.DescribeAggregationAuthorizations()
	require.Len(t, auths, 1)
	assert.Equal(t, "123456789012", auths[0].AuthorizedAccountID)

	rules, err := fresh.DescribeConfigRules(nil)
	require.NoError(t, err)
	require.Len(t, rules, 1)
	assert.Equal(t, "rule-x", rules[0].ConfigRuleName)

	// Verify the other maps by attempting deletes (they would fail if not restored)
	require.NoError(t, fresh.DeleteConfigurationAggregator("agg-1"))
	require.NoError(t, fresh.DeleteConformancePack("pack-1"))
	require.NoError(t, fresh.DeleteOrganizationConfigRule("org-rule-1"))
	require.NoError(t, fresh.DeleteOrganizationConformancePack("org-pack-1"))
}

// TestInMemoryBackend_Snapshot_AllTables_FullState populates every
// store.Table registered in store_setup.go's registerAllTables plus one
// entry that exercises the two flattened, index-backed tables
// (resourceConfigs, ruleResourceEvals) via their secondary lookups, then
// verifies a Snapshot -> Restore round trip preserves every one of them.
func TestInMemoryBackend_Snapshot_AllTables_FullState(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	require.NoError(t, b.PutConfigurationRecorder("rec", "arn:aws:iam::000:role/r", nil))
	require.NoError(t, b.PutDeliveryChannel("chan", "bucket", "", "", nil))
	require.NoError(t, b.PutAggregationAuthorization("123456789012", "us-east-1"))
	require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-x"}))
	require.NoError(t, b.PutConfigurationAggregator("agg-1", nil, nil))
	require.NoError(t, b.PutConformancePack("pack-1", "", "", ""))
	require.NoError(t, b.PutOrganizationConfigRule("org-rule-1"))
	require.NoError(t, b.PutOrganizationConformancePack("org-pack-1"))
	require.NoError(t, b.PutStoredQuery("query-1"))
	require.NoError(t, b.PutRetentionConfiguration("retention-1", 30))
	require.NoError(t, b.PutRemediationConfigurations([]awsconfig.RemediationConfiguration{
		{ConfigRuleName: "rule-rem", TargetType: "SSM_DOCUMENT", TargetID: "doc-1"},
	}))
	evalID := b.StartResourceEvaluation("AWS::S3::Bucket", "eval-bucket", "DETAILED", `{"a":1}`)
	require.NoError(t, b.PutResourceConfig("AWS::S3::Bucket", "bucket-1", `{"a":1}`))
	require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-ext"}))
	require.NoError(t, b.PutExternalEvaluation(awsconfig.EvaluationResult{
		ConfigRuleName: "rule-ext",
		ComplianceType: "NON_COMPLIANT",
		ResourceType:   "AWS::S3::Bucket",
		ResourceID:     "bucket-ext",
	}))
	connArn, err := b.PutConnector(&awsconfig.ConnectorConfiguration{
		Azure: &awsconfig.AzureConnectorConfiguration{ClientIdentifier: "client-1", TenantIdentifier: "tenant-1"},
	}, nil)
	require.NoError(t, err)
	thirdPartyName, _, err := b.PutThirdPartyServiceLinkedConfigurationRecorder(
		"thirdparty.amazonaws.com", connArn, &awsconfig.ScopeConfiguration{ScopeType: "tenant", AllRegions: true}, nil,
	)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := awsconfig.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assert.Len(t, fresh.DescribeConfigurationRecorders(nil), 2) // "rec" and the third-party service-linked recorder
	assert.Len(t, fresh.DescribeDeliveryChannels(nil), 1)
	assert.Len(t, fresh.DescribeAggregationAuthorizations(), 1)

	restoredConnector, err := fresh.GetConnector(connArn)
	require.NoError(t, err)
	assert.Equal(t, connArn, restoredConnector.Arn)

	restoredThirdPartyRecorders := fresh.DescribeConfigurationRecorders([]string{thirdPartyName})
	require.Len(t, restoredThirdPartyRecorders, 1)
	assert.Equal(t, "thirdparty.amazonaws.com", restoredThirdPartyRecorders[0].ServicePrincipal)
	assert.Equal(t, connArn, restoredThirdPartyRecorders[0].ConnectorArn)

	// The recordersByServicePrincipal index must also have survived the
	// round trip (not just the raw recorders table), or a second Put for the
	// same principal would wrongly create a duplicate instead of updating.
	_, _, err = fresh.PutThirdPartyServiceLinkedConfigurationRecorder(
		"thirdparty.amazonaws.com", connArn,
		&awsconfig.ScopeConfiguration{ScopeType: "subscription", AllRegions: false}, nil,
	)
	require.NoError(t, err)
	assert.Len(t, fresh.DescribeConfigurationRecorders(nil), 2)

	restoredRules, err := fresh.DescribeConfigRules(nil)
	require.NoError(t, err)
	assert.Len(t, restoredRules, 2) // "rule-x" and "rule-ext"

	assert.Len(t, fresh.DescribeConfigurationAggregators(), 1)
	assert.Len(t, fresh.DescribeConformancePacks(), 1)
	assert.Len(t, fresh.DescribeOrganizationConfigRules(), 1)
	assert.Len(t, fresh.DescribeOrganizationConformancePacks(), 1)
	require.NotNil(t, fresh.GetStoredQuery("query-1"))
	assert.Len(t, fresh.DescribeRetentionConfigurations(), 1)
	assert.Len(t, fresh.DescribeRemediationConfigurations(nil), 1)
	require.NotNil(t, fresh.GetResourceEvaluationSummaryByID(evalID))
	assert.Len(t, fresh.ListDiscoveredResources("AWS::S3::Bucket"), 1)

	restoredDetails, err := fresh.GetComplianceDetailsByConfigRule("rule-ext", nil)
	require.NoError(t, err)
	assert.Len(t, restoredDetails, 1)
	assert.Len(t, fresh.GetComplianceDetailsByResource("AWS::S3::Bucket", "bucket-ext", nil), 1)
}

// TestInMemoryBackend_Restore_VersionMismatch verifies that a snapshot whose
// version field does not match the current awsconfigSnapshotVersion is
// discarded wholesale (registry reset to empty, no partial decode) rather
// than erroring or corrupting state, covering both an explicit future version
// and an absent version field (the pre-Phase-3.3 snapshot shape, which
// decodes as version 0).
func TestInMemoryBackend_Restore_VersionMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload string
	}{
		{name: "future_version", payload: `{"version":999,"tables":{}}`},
		{name: "absent_version_pre_phase_3_3_shape", payload: `{"recorders":{"rec":{"name":"rec"}}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			require.NoError(t, b.PutConfigurationRecorder("existing", "arn:aws:iam::000:role/r", nil))

			require.NoError(t, b.Restore(t.Context(), []byte(tt.payload)))

			assert.Empty(t, b.DescribeConfigurationRecorders(nil))
		})
	}
}
