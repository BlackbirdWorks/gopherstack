package rum_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrum "github.com/aws/aws-sdk-go-v2/service/rum"
	"github.com/aws/aws-sdk-go-v2/service/rum/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const evtLCP = "com.amazon.rum.largest_contentful_paint_event"

func TestRealClient_TelemetriesGateEvents(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		telemetries []types.Telemetry
		wantTypes   []string
		wantMetrics []string
	}{
		{
			name: "unset_collects_all", telemetries: nil,
			wantTypes: []string{evtJSError, evtNav, evtLCP, evtHTTP, evtSession},
			wantMetrics: []string{
				"JsErrorCount",
				"PerformanceNavigationDuration",
				"WebVitalsLargestContentfulPaint",
				"HttpErrorCount",
				"SessionCount",
			},
		},
		{
			name: "errors_only", telemetries: []types.Telemetry{types.TelemetryErrors},
			wantTypes:   []string{evtJSError, evtSession},
			wantMetrics: []string{"JsErrorCount", "SessionCount"},
		},
		{
			name: "performance_only", telemetries: []types.Telemetry{types.TelemetryPerformance},
			wantTypes:   []string{evtNav, evtLCP, evtSession},
			wantMetrics: []string{"PerformanceNavigationDuration", "WebVitalsLargestContentfulPaint", "SessionCount"},
		},
		{
			name: "http_and_errors", telemetries: []types.Telemetry{types.TelemetryHttp, types.TelemetryErrors},
			wantTypes:   []string{evtJSError, evtHTTP, evtSession},
			wantMetrics: []string{"JsErrorCount", "HttpErrorCount", "SessionCount"},
		},
	}

	allMetrics := []string{
		"JsErrorCount", "PerformanceNavigationDuration", "WebVitalsLargestContentfulPaint",
		"HttpErrorCount", "SessionCount",
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			id := f.createMonitor(t, "web", func(in *awsrum.CreateAppMonitorInput) {
				in.AppMonitorConfiguration = &types.AppMonitorConfiguration{Telemetries: tc.telemetries}
			})
			base := f.base()

			require.NoError(t, f.putEvents(t, id, "web",
				eventSpec{id: "e1", typ: evtJSError, details: `{}`, ts: base},
				eventSpec{id: "e2", typ: evtNav, details: `{"duration":1500}`, ts: base},
				eventSpec{id: "e3", typ: evtLCP, details: `{"value":900}`, ts: base},
				eventSpec{id: "e4", typ: evtHTTP, details: `{"response":{"status":500}}`, ts: base},
				eventSpec{id: "e5", typ: evtSession, details: `{}`, ts: base},
			))

			out, err := f.client.GetAppMonitorData(t.Context(), &awsrum.GetAppMonitorDataInput{
				Name: aws.String("web"), TimeRange: &types.TimeRange{After: int64(base) - 10},
			})
			require.NoError(t, err)

			gotTypes := make([]string, 0, len(out.Events))
			for _, e := range out.Events {
				gotTypes = append(gotTypes, mustJSON(t, e)["event_type"].(string))
			}

			assert.ElementsMatch(t, tc.wantTypes, gotTypes)
			assert.Equal(t, len(tc.wantTypes), f.backend.EventCount())

			for _, m := range allMetrics {
				want := 0
				for _, w := range tc.wantMetrics {
					if w == m {
						want = 1
					}
				}

				assert.Len(t, f.metrics.named("AWS/RUM", m), want, m)
			}
		})
	}
}
