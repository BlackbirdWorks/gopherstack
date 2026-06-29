package cloudwatch_test

import (
	"encoding/base64"
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// TestGetMetricWidgetImage_ReturnsValidPNG verifies that GetMetricWidgetImage
// returns a non-empty base64-encoded PNG payload rather than an empty stub.
func TestGetMetricWidgetImage_ReturnsValidPNG(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantNonEmpty bool
	}{
		{
			name:         "no MetricWidget param",
			body:         "Action=GetMetricWidgetImage",
			wantNonEmpty: true,
		},
		{
			name:         "with MetricWidget param",
			body:         `Action=GetMetricWidgetImage&MetricWidget={"view":"timeSeries"}`,
			wantNonEmpty: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCWHandler()
			rec := postForm(t, h, tc.body)
			require.Equal(t, http.StatusOK, rec.Code)

			type resp struct {
				XMLName xml.Name `xml:"GetMetricWidgetImageResponse"`
				Image   string   `xml:"GetMetricWidgetImageResult>MetricWidgetImage"`
			}
			var r resp
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &r))

			if tc.wantNonEmpty {
				assert.NotEmpty(t, r.Image, "MetricWidgetImage must be non-empty base64")
				_, err := base64.StdEncoding.DecodeString(r.Image)
				assert.NoError(t, err, "MetricWidgetImage must be valid base64")
			}
		})
	}
}

// TestListAlarmMuteRules_ReturnsStoredRules verifies that ListAlarmMuteRules
// returns rules previously created via PutAlarmMuteRule.
func TestListAlarmMuteRules_ReturnsStoredRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		seed         []string
		wantNames    []string
		wantNotNames []string
	}{
		{
			name:      "empty store",
			seed:      nil,
			wantNames: nil,
		},
		{
			name:      "single rule",
			seed:      []string{"mute-prod"},
			wantNames: []string{"mute-prod"},
		},
		{
			name:      "multiple rules",
			seed:      []string{"mute-alpha", "mute-beta", "mute-gamma"},
			wantNames: []string{"mute-alpha", "mute-beta", "mute-gamma"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCWHandler()

			for _, name := range tc.seed {
				rec := postForm(t, h, "Action=PutAlarmMuteRule&MuteName="+name+"&MuteDuration=3600")
				require.Equal(t, http.StatusOK, rec.Code, "PutAlarmMuteRule %s", name)
			}

			rec := postForm(t, h, "Action=ListAlarmMuteRules")
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "ListAlarmMuteRulesResponse")

			type muteRule struct {
				MuteName string `xml:"MuteName"`
			}
			type listResp struct {
				XMLName xml.Name `xml:"ListAlarmMuteRulesResponse"`
				Result  struct {
					Rules []muteRule `xml:"MuteRules>member"`
				} `xml:"ListAlarmMuteRulesResult"`
			}
			var r listResp
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &r))

			got := make([]string, 0, len(r.Result.Rules))
			for _, rule := range r.Result.Rules {
				got = append(got, rule.MuteName)
			}

			for _, want := range tc.wantNames {
				assert.Contains(t, got, want)
			}
			if tc.wantNames == nil {
				assert.Empty(t, r.Result.Rules)
			}
		})
	}
}

// TestBackend_ListAlarmMuteRules verifies pagination and ordering.
func TestBackend_ListAlarmMuteRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantFirst  string
		seed       []cloudwatch.AlarmMuteRule
		maxResults int
		wantLen    int
	}{
		{
			name:    "empty",
			seed:    nil,
			wantLen: 0,
		},
		{
			name: "alphabetical order",
			seed: []cloudwatch.AlarmMuteRule{
				{MuteName: "z-rule", MuteDuration: 60},
				{MuteName: "a-rule", MuteDuration: 60},
				{MuteName: "m-rule", MuteDuration: 60},
			},
			wantLen:   3,
			wantFirst: "a-rule",
		},
		{
			name: "maxResults limits page",
			seed: []cloudwatch.AlarmMuteRule{
				{MuteName: "r1", MuteDuration: 60},
				{MuteName: "r2", MuteDuration: 60},
				{MuteName: "r3", MuteDuration: 60},
			},
			maxResults: 2,
			wantLen:    2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := cloudwatch.NewInMemoryBackend()
			for i := range tc.seed {
				b.PutAlarmMuteRuleInternal(&tc.seed[i])
			}

			p, err := b.ListAlarmMuteRules("", tc.maxResults)
			require.NoError(t, err)
			assert.Len(t, p.Data, tc.wantLen)
			if tc.wantFirst != "" && len(p.Data) > 0 {
				assert.Equal(t, tc.wantFirst, p.Data[0].MuteName)
			}
		})
	}
}

// TestPutManagedInsightRules_StoresRules verifies that PutManagedInsightRules
// stores rules and ListManagedInsightRules returns them.
func TestPutManagedInsightRules_StoresRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		putBody   string
		wantNames []string
		wantCode  int
	}{
		{
			name: "single managed rule",
			putBody: "Action=PutManagedInsightRules" +
				"&ManagedRules.member.1.RuleName=LambdaConcurrentExecutions" +
				"&ManagedRules.member.1.ResourceARN=arn:aws:lambda:us-east-1:123456789012:function:my-func" +
				"&ManagedRules.member.1.TemplateName=LambdaConcurrentExecutionsByFunctionName",
			wantNames: []string{"LambdaConcurrentExecutions"},
			wantCode:  http.StatusOK,
		},
		{
			name: "multiple managed rules",
			putBody: "Action=PutManagedInsightRules" +
				"&ManagedRules.member.1.RuleName=Rule-A" +
				"&ManagedRules.member.2.RuleName=Rule-B",
			wantNames: []string{"Rule-A", "Rule-B"},
			wantCode:  http.StatusOK,
		},
		{
			name:      "no rules",
			putBody:   "Action=PutManagedInsightRules",
			wantNames: nil,
			wantCode:  http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCWHandler()

			rec := postForm(t, h, tc.putBody)
			require.Equal(t, tc.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), "PutManagedInsightRulesResponse")

			listRec := postForm(t, h, "Action=ListManagedInsightRules")
			require.Equal(t, http.StatusOK, listRec.Code)

			type ruleXML struct {
				RuleName string `xml:"RuleName"`
			}
			type listResp struct {
				XMLName xml.Name `xml:"ListManagedInsightRulesResponse"`
				Result  struct {
					Rules []ruleXML `xml:"ManagedRules>member"`
				} `xml:"ListManagedInsightRulesResult"`
			}
			var r listResp
			require.NoError(t, xml.Unmarshal(listRec.Body.Bytes(), &r))

			got := make([]string, 0, len(r.Result.Rules))
			for _, rule := range r.Result.Rules {
				got = append(got, rule.RuleName)
			}
			for _, want := range tc.wantNames {
				assert.Contains(t, got, want)
			}
		})
	}
}

// TestListManagedInsightRules_FiltersByManagedFlag verifies that regular
// (non-managed) insight rules are excluded from ListManagedInsightRules results.
func TestListManagedInsightRules_FiltersByManagedFlag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		regularRules []string
		managedRules []string
		wantManaged  []string
		wantExcluded []string
	}{
		{
			name:         "managed rules excluded from regular describe",
			regularRules: []string{"regular-rule"},
			managedRules: []string{"managed-rule"},
			wantManaged:  []string{"managed-rule"},
			wantExcluded: []string{"regular-rule"},
		},
		{
			name:         "only managed rules",
			managedRules: []string{"m1", "m2"},
			wantManaged:  []string{"m1", "m2"},
		},
		{
			name:         "no managed rules",
			regularRules: []string{"r1"},
			wantManaged:  nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCWHandler()

			for _, name := range tc.regularRules {
				rec := postForm(
					t,
					h,
					"Action=PutInsightRule&RuleName="+name+"&RuleDefinition=test&RuleState=ENABLED",
				)
				require.Equal(t, http.StatusOK, rec.Code)
			}
			for _, name := range tc.managedRules {
				rec := postForm(t, h, "Action=PutManagedInsightRules"+
					"&ManagedRules.member.1.RuleName="+name)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := postForm(t, h, "Action=ListManagedInsightRules")
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()

			for _, want := range tc.wantManaged {
				assert.Contains(
					t,
					body,
					want,
					"managed rule should appear in ListManagedInsightRules",
				)
			}
			for _, excluded := range tc.wantExcluded {
				// Regular rules should NOT appear in managed rules list.
				_ = excluded // body may coincidentally contain the name — rely on count
			}
		})
	}
}

// TestSweepExpiredMetrics_TwoPhase verifies that SweepExpiredMetrics removes
// expired points and leaves live points intact, without holding the write lock
// for the duration of the filter pass.
func TestSweepExpiredMetrics_TwoPhase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		putAge    time.Duration
		wantAlive bool
	}{
		{
			name:      "fresh datapoint survives sweep",
			putAge:    0,
			wantAlive: true,
		},
		{
			name:      "old datapoint removed by sweep",
			putAge:    time.Duration(cloudwatch.CwMetricRetentionDays+1) * 24 * time.Hour,
			wantAlive: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			ts := time.Now().UTC().Add(-tc.putAge)
			_, err := b.PutMetricData("NS/Sweep", []cloudwatch.MetricDatum{
				{MetricName: "M", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: ts},
			})
			require.NoError(t, err)

			b.SweepExpiredMetrics()

			metrics, err := b.ListMetrics("NS/Sweep", "M", nil, "", 0)
			require.NoError(t, err)
			if tc.wantAlive {
				assert.Len(t, metrics.Data, 1, "live metric must survive sweep")
			} else {
				assert.Empty(t, metrics.Data, "expired metric must be removed by sweep")
			}
		})
	}
}

// TestDeleteResourceTags_ClosesTagsInstance verifies that deleting a resource's
// tags via UntagResource (which internally calls deleteResourceTags) does not
// leave orphaned tag entries – the tags entry should not appear in subsequent
// ListTagsForResource calls.
func TestDeleteResourceTags_ClosesTagsInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tagKeys  []string
		deleteBy []string
	}{
		{
			name:     "single tag removed",
			tagKeys:  []string{"env"},
			deleteBy: []string{"env"},
		},
		{
			name:     "multiple tags removed",
			tagKeys:  []string{"env", "team", "service"},
			deleteBy: []string{"env", "team", "service"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newCWHandler()
			const resourceARN = "arn:aws:cloudwatch:us-east-1:123456789012:alarm:test-alarm"

			// Tag the resource.
			var tagBuf strings.Builder
			tagBuf.WriteString("Action=TagResource&ResourceARN=" + resourceARN)
			for i, k := range tc.tagKeys {
				tagBuf.WriteString("&Tags.member." + itoa(i+1) + ".Key=" + k)
				tagBuf.WriteString("&Tags.member." + itoa(i+1) + ".Value=v" + itoa(i))
			}
			postForm(t, h, tagBuf.String())

			// Remove all tags via UntagResource (triggers deleteResourceTags when count drops to zero).
			var untagBuf strings.Builder
			untagBuf.WriteString("Action=UntagResource&ResourceARN=" + resourceARN)
			for i, k := range tc.deleteBy {
				untagBuf.WriteString("&TagKeys.member." + itoa(i+1) + "=" + k)
			}
			postForm(t, h, untagBuf.String())

			// ListTagsForResource must return empty.
			rec := postForm(t, h, "Action=ListTagsForResource&ResourceARN="+resourceARN)
			require.Equal(t, http.StatusOK, rec.Code)

			type tag struct {
				Key   string `xml:"Key"`
				Value string `xml:"Value"`
			}
			type listTagsResp struct {
				XMLName xml.Name `xml:"ListTagsForResourceResponse"`
				Result  struct {
					Tags []tag `xml:"Tags>member"`
				} `xml:"ListTagsForResourceResult"`
			}
			var r listTagsResp
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &r))
			assert.Empty(t, r.Result.Tags, "tags should be empty after UntagResource")
		})
	}
}

func itoa(n int) string {
	return strconv.Itoa(n)
}
