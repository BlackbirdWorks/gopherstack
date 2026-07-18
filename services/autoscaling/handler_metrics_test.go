package autoscaling_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAutoscalingHandler_MetricCollectionTypesGranularities verifies that
// DescribeMetricCollectionTypes returns a Granularities element with "1Minute".
func TestAutoscalingHandler_MetricCollectionTypesGranularities(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()

	code, body := doAS(t, h, "DescribeMetricCollectionTypes", url.Values{})
	require.Equal(t, 200, code)

	assert.Contains(t, body, "<Granularities>",
		"response must include Granularities element; got: %s", body)
	assert.Contains(t, body, "<Granularity>1Minute</Granularity>",
		"Granularities must include 1Minute")
}

func TestAutoscalingHandler_EnabledMetricsRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		enableMetrics  []string
		wantMetrics    []string
		disableMetrics []string
		wantAfterDis   []string
	}{
		{
			name:          "enable_specific_metrics_visible_in_describe",
			enableMetrics: []string{"GroupMinSize", "GroupMaxSize"},
			wantMetrics:   []string{"GroupMinSize", "GroupMaxSize"},
		},
		{
			name:           "disable_all_metrics_clears_list",
			enableMetrics:  []string{"GroupMinSize", "GroupMaxSize"},
			wantMetrics:    []string{"GroupMinSize", "GroupMaxSize"},
			disableMetrics: nil, // nil = disable all
			wantAfterDis:   []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()

			postAutoscalingForm(t, h,
				"Action=CreateAutoScalingGroup&Version=2011-01-01"+
					"&AutoScalingGroupName=metrics-asg&MinSize=0&MaxSize=5")

			// Enable metrics
			parts := []string{
				"Action=EnableMetricsCollection",
				"Version=2011-01-01",
				"AutoScalingGroupName=metrics-asg",
				"Granularity=1Minute",
			}
			for i, m := range tt.enableMetrics {
				parts = append(parts, fmt.Sprintf("Metrics.member.%d=%s", i+1, m))
			}
			enableBody := strings.Join(parts, "&")
			rec := postAutoscalingForm(t, h, enableBody)
			require.Equal(t, http.StatusOK, rec.Code)

			// Parse DescribeAutoScalingGroups response to get EnabledMetrics
			rec = postAutoscalingForm(t, h,
				"Action=DescribeAutoScalingGroups&Version=2011-01-01"+
					"&AutoScalingGroupNames.member.1=metrics-asg")
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				XMLName xml.Name `xml:"DescribeAutoScalingGroupsResponse"`
				Result  struct {
					AutoScalingGroups struct {
						Members []struct {
							Name           string `xml:"AutoScalingGroupName"`
							EnabledMetrics struct {
								Members []struct {
									Metric      string `xml:"Metric"`
									Granularity string `xml:"Granularity"`
								} `xml:"member"`
							} `xml:"EnabledMetrics"`
						} `xml:"member"`
					} `xml:"AutoScalingGroups"`
				} `xml:"DescribeAutoScalingGroupsResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Result.AutoScalingGroups.Members, 1)

			got := resp.Result.AutoScalingGroups.Members[0].EnabledMetrics.Members
			require.Len(t, got, len(tt.wantMetrics))

			gotNames := make([]string, len(got))
			for i, m := range got {
				gotNames[i] = m.Metric
				assert.Equal(t, "1Minute", m.Granularity)
			}
			assert.ElementsMatch(t, tt.wantMetrics, gotNames)

			if tt.wantAfterDis != nil {
				// Disable all metrics and re-check
				postAutoscalingForm(t, h,
					"Action=DisableMetricsCollection&Version=2011-01-01"+
						"&AutoScalingGroupName=metrics-asg")

				rec = postAutoscalingForm(t, h,
					"Action=DescribeAutoScalingGroups&Version=2011-01-01"+
						"&AutoScalingGroupNames.member.1=metrics-asg")
				require.Equal(t, http.StatusOK, rec.Code)

				var resp2 struct {
					XMLName xml.Name `xml:"DescribeAutoScalingGroupsResponse"`
					Result  struct {
						AutoScalingGroups struct {
							Members []struct {
								EnabledMetrics struct {
									Members []struct {
										Metric string `xml:"Metric"`
									} `xml:"member"`
								} `xml:"EnabledMetrics"`
							} `xml:"member"`
						} `xml:"AutoScalingGroups"`
					} `xml:"DescribeAutoScalingGroupsResult"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp2))
				require.Len(t, resp2.Result.AutoScalingGroups.Members, 1)
				assert.Empty(t, resp2.Result.AutoScalingGroups.Members[0].EnabledMetrics.Members)
			}
		})
	}
}
