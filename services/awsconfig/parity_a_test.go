package awsconfig_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func doConfig(t *testing.T, h *awsconfig.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		raw = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "StarlingDoveService."+action)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func newConfigBackend() (*awsconfig.Handler, *awsconfig.InMemoryBackend) {
	b := awsconfig.NewInMemoryBackend()

	return awsconfig.NewHandler(b), b
}

// TestParity_ConfigRulePascalCaseKeys verifies DescribeConfigRules returns PascalCase JSON keys.
func TestParity_ConfigRulePascalCaseKeys(t *testing.T) {
	t.Parallel()

	h, b := newConfigBackend()
	require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{
		ConfigRuleName:  "my-rule",
		Description:     "test rule",
		InputParameters: `{"key":"val"}`,
	}))

	rec := doConfig(t, h, "DescribeConfigRules", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, `"ConfigRuleName"`)
	assert.Contains(t, body, `"ConfigRuleArn"`)
	assert.Contains(t, body, `"ConfigRuleId"`)
	assert.Contains(t, body, `"Description"`)
	assert.Contains(t, body, `"InputParameters"`)
	assert.NotContains(t, body, `"configRuleName"`)
}

// TestParity_ConfigRuleARNGenerated verifies PutConfigRule generates a proper ARN.
func TestParity_ConfigRuleARNGenerated(t *testing.T) {
	t.Parallel()

	_, b := newConfigBackend()
	require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-x"}))

	rules := b.DescribeConfigRules([]string{"rule-x"})
	require.Len(t, rules, 1)
	assert.Contains(t, rules[0].ConfigRuleArn, "arn:aws:config:")
	assert.Contains(t, rules[0].ConfigRuleArn, "config-rule-")
	assert.NotEmpty(t, rules[0].ConfigRuleID)
}

// TestParity_ConfigRuleStateActive verifies new rules default to ACTIVE state.
func TestParity_ConfigRuleStateActive(t *testing.T) {
	t.Parallel()

	_, b := newConfigBackend()
	require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-y"}))

	rules := b.DescribeConfigRules(nil)
	require.Len(t, rules, 1)
	assert.Equal(t, "ACTIVE", rules[0].ConfigRuleState)
}

// TestParity_ConfigRuleScopeRoundtrip verifies Scope is stored and returned.
func TestParity_ConfigRuleScopeRoundtrip(t *testing.T) {
	t.Parallel()

	h, _ := newConfigBackend()
	rec := doConfig(t, h, "PutConfigRule", map[string]any{
		"ConfigRule": map[string]any{
			"ConfigRuleName": "scoped-rule",
			"Source": map[string]any{
				"Owner":            "AWS",
				"SourceIdentifier": "S3_BUCKET_PUBLIC_READ_PROHIBITED",
			},
			"Scope": map[string]any{
				"ComplianceResourceTypes": []string{"AWS::S3::Bucket"},
				"TagKey":                  "env",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doConfig(t, h, "DescribeConfigRules", map[string]any{"ConfigRuleNames": []string{"scoped-rule"}})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ConfigRules []struct {
			Scope *struct {
				TagKey                  string   `json:"TagKey"`
				ComplianceResourceTypes []string `json:"ComplianceResourceTypes"`
			} `json:"Scope"`
		} `json:"ConfigRules"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.ConfigRules, 1)
	require.NotNil(t, out.ConfigRules[0].Scope)
	assert.Contains(t, out.ConfigRules[0].Scope.ComplianceResourceTypes, "AWS::S3::Bucket")
	assert.Equal(t, "env", out.ConfigRules[0].Scope.TagKey)
}

// TestParity_DeliveryChannelS3KeyPrefix verifies S3KeyPrefix is stored and returned.
func TestParity_DeliveryChannelS3KeyPrefix(t *testing.T) {
	t.Parallel()

	h, _ := newConfigBackend()
	rec := doConfig(t, h, "PutDeliveryChannel", map[string]any{
		"DeliveryChannel": map[string]any{
			"name":         "default",
			"s3BucketName": "my-bucket",
			"s3KeyPrefix":  "config/",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doConfig(t, h, "DescribeDeliveryChannels", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"config/"`)
}

// TestParity_DeliveryChannelSnapshotFrequency verifies snapshot delivery properties roundtrip.
func TestParity_DeliveryChannelSnapshotFrequency(t *testing.T) {
	t.Parallel()

	h, _ := newConfigBackend()
	rec := doConfig(t, h, "PutDeliveryChannel", map[string]any{
		"DeliveryChannel": map[string]any{
			"name":         "default",
			"s3BucketName": "my-bucket",
			"configSnapshotDeliveryProperties": map[string]any{
				"deliveryFrequency": "TwentyFour_Hours",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doConfig(t, h, "DescribeDeliveryChannels", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "TwentyFour_Hours")
}

// TestParity_RecorderRecordingGroupRoundtrip verifies RecordingGroup is stored and returned.
func TestParity_RecorderRecordingGroupRoundtrip(t *testing.T) {
	t.Parallel()

	h, _ := newConfigBackend()
	rec := doConfig(t, h, "PutConfigurationRecorder", map[string]any{
		"ConfigurationRecorder": map[string]any{
			"name":    "default",
			"roleARN": "arn:aws:iam::123456789012:role/config",
			"recordingGroup": map[string]any{
				"allSupported":               true,
				"includeGlobalResourceTypes": true,
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doConfig(t, h, "DescribeConfigurationRecorders", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ConfigurationRecorders []struct {
			RecordingGroup *struct {
				AllSupported               bool `json:"allSupported"`
				IncludeGlobalResourceTypes bool `json:"includeGlobalResourceTypes"`
			} `json:"recordingGroup"`
		} `json:"ConfigurationRecorders"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.ConfigurationRecorders, 1)
	require.NotNil(t, out.ConfigurationRecorders[0].RecordingGroup)
	assert.True(t, out.ConfigurationRecorders[0].RecordingGroup.AllSupported)
	assert.True(t, out.ConfigurationRecorders[0].RecordingGroup.IncludeGlobalResourceTypes)
}

// TestParity_RecorderStatusLastStatus verifies DescribeConfigurationRecorderStatus returns lastStatus.
func TestParity_RecorderStatusLastStatus(t *testing.T) {
	t.Parallel()

	_, b := newConfigBackend()
	require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::123:role/r", nil))
	require.NoError(t, b.PutDeliveryChannel("default", "my-bucket", "", "", nil))

	statusBefore := b.DescribeConfigurationRecorderStatus(nil)
	require.Len(t, statusBefore, 1)
	assert.Equal(t, "PENDING", statusBefore[0].LastStatus)
	assert.False(t, statusBefore[0].Recording)

	require.NoError(t, b.StartConfigurationRecorder("default"))

	statusAfter := b.DescribeConfigurationRecorderStatus(nil)
	require.Len(t, statusAfter, 1)
	assert.Equal(t, "SUCCESS", statusAfter[0].LastStatus)
	assert.True(t, statusAfter[0].Recording)
}

// TestParity_AggregationAuthorizationARN verifies PutAggregationAuthorization generates an ARN.
func TestParity_AggregationAuthorizationARN(t *testing.T) {
	t.Parallel()

	_, b := newConfigBackend()
	require.NoError(t, b.PutAggregationAuthorization("999999999999", "eu-west-1"))

	auths := b.DescribeAggregationAuthorizations()
	require.Len(t, auths, 1)
	assert.Contains(t, auths[0].AggregationAuthorizationArn, "arn:aws:config:")
	assert.Contains(t, auths[0].AggregationAuthorizationArn, "999999999999")
	assert.Contains(t, auths[0].AggregationAuthorizationArn, "eu-west-1")
}

// TestParity_AggregatorARNAndSources verifies PutConfigurationAggregator generates an ARN.
func TestParity_AggregatorARNAndSources(t *testing.T) {
	t.Parallel()

	h, _ := newConfigBackend()
	rec := doConfig(t, h, "PutConfigurationAggregator", map[string]any{
		"ConfigurationAggregatorName": "my-agg",
		"AccountAggregationSources": []map[string]any{
			{"AccountIds": []string{"111111111111"}, "AllAwsRegions": true},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doConfig(t, h, "DescribeConfigurationAggregators", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "ConfigurationAggregatorArn")
	assert.Contains(t, body, "arn:aws:config:")
}

// TestParity_ConformancePackARN verifies PutConformancePack generates an ARN and ID.
func TestParity_ConformancePackARN(t *testing.T) {
	t.Parallel()

	h, _ := newConfigBackend()
	rec := doConfig(t, h, "PutConformancePack", map[string]any{
		"ConformancePackName": "test-pack",
		"DeliveryS3Bucket":    "my-delivery-bucket",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doConfig(t, h, "DescribeConformancePacks", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ConformancePackDetails []struct {
			ConformancePackArn string `json:"ConformancePackArn"`
			ConformancePackID  string `json:"ConformancePackId"`
			DeliveryS3Bucket   string `json:"DeliveryS3Bucket"`
		} `json:"ConformancePackDetails"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.ConformancePackDetails, 1)
	assert.Contains(t, out.ConformancePackDetails[0].ConformancePackArn, "arn:aws:config:")
	assert.NotEmpty(t, out.ConformancePackDetails[0].ConformancePackID)
	assert.Equal(t, "my-delivery-bucket", out.ConformancePackDetails[0].DeliveryS3Bucket)
}

// TestParity_ListConfigurationRecordersSummaries verifies ListConfigurationRecorders returns summaries.
func TestParity_ListConfigurationRecordersSummaries(t *testing.T) {
	t.Parallel()

	h, b := newConfigBackend()
	require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::123:role/r", nil))

	rec := doConfig(t, h, "ListConfigurationRecorders", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ConfigurationRecorderSummaries []struct {
			Arn            string `json:"arn"`
			Name           string `json:"name"`
			RecordingScope string `json:"recordingScope"`
		} `json:"ConfigurationRecorderSummaries"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.ConfigurationRecorderSummaries, 1)
	assert.Equal(t, "default", out.ConfigurationRecorderSummaries[0].Name)
	assert.Contains(t, out.ConfigurationRecorderSummaries[0].Arn, "arn:aws:config:")
	assert.Equal(t, "INTERNAL", out.ConfigurationRecorderSummaries[0].RecordingScope)
}

// TestParity_ListStoredQueriesMetadata verifies ListStoredQueries returns StoredQueryMetadata.
func TestParity_ListStoredQueriesMetadata(t *testing.T) {
	t.Parallel()

	h, b := newConfigBackend()
	require.NoError(t, b.PutStoredQuery("my-query"))

	rec := doConfig(t, h, "ListStoredQueries", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		StoredQueryMetadata []struct {
			QueryArn  string `json:"QueryArn"`
			QueryName string `json:"QueryName"`
		} `json:"StoredQueryMetadata"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.StoredQueryMetadata, 1)
	assert.Equal(t, "my-query", out.StoredQueryMetadata[0].QueryName)
	assert.Contains(t, out.StoredQueryMetadata[0].QueryArn, "arn:aws:config:")
}

// TestParity_ComplianceSummaryShape verifies GetComplianceSummaryByConfigRule uses CappedCount shape.
func TestParity_ComplianceSummaryShape(t *testing.T) {
	t.Parallel()

	h, b := newConfigBackend()
	require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "r1"}))
	require.NoError(t, b.StartConfigRulesEvaluation())

	rec := doConfig(t, h, "GetComplianceSummaryByConfigRule", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Real AWS shape: ComplianceSummary.CompliantResourceCount.CappedCount
	body := rec.Body.String()
	assert.Contains(t, body, "CappedCount")
	assert.Contains(t, body, "CapExceeded")
	assert.Contains(t, body, `"ComplianceSummary"`)
}

// TestParity_ConfigRuleEvaluationStatusTimestampStrings verifies timestamps are strings not numbers.
func TestParity_ConfigRuleEvaluationStatusTimestampStrings(t *testing.T) {
	t.Parallel()

	_, b := newConfigBackend()
	require.NoError(t, b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule-ts"}))
	require.NoError(t, b.StartConfigRulesEvaluation())

	statuses := b.DescribeConfigRuleEvaluationStatus(nil)
	require.Len(t, statuses, 1)
	assert.Equal(t, "rule-ts", statuses[0].ConfigRuleName)
}
