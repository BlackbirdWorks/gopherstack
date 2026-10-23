package gcpmonitoring_test

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest/resttest"
)

func TestRESTFlow(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ts := epoch().Format(time.RFC3339)
	query := url.Values{
		"filter":             {metricFilter},
		"interval.endTime":   {epoch().Add(time.Second).Format(time.RFC3339)},
		"interval.startTime": {epoch().Add(-time.Minute).Format(time.RFC3339)},
	}.Encode()

	resttest.Run(t, e.handler.Addr(), []resttest.Step{
		{
			Name: "write series", Method: http.MethodPost, Path: "/v3/projects/p/timeSeries",
			Body: `{"timeSeries":[{"metric":{"type":"` + queueDepth + `","labels":{"tier":"gold"}},` +
				`"resource":{"type":"gce_instance","labels":{"instance_id":"i-1","zone":"z"}},` +
				`"points":[{"interval":{"endTime":"` + ts + `"},"value":{"doubleValue":4.5}}]}]}`,
			Status: 200, Contains: []string{`{}`},
		},
		{
			Name: "list series", Method: http.MethodGet, Path: "/v3/projects/p/timeSeries?" + query,
			Status: 200, Contains: []string{`"doubleValue":4.5`, `"valueType":"DOUBLE"`, `"metricKind":"GAUGE"`},
		},
		{
			Name: "bad write", Method: http.MethodPost, Path: "/v3/projects/p/timeSeries",
			Body:   `{"timeSeries":[{"metric":{"type":"compute.googleapis.com/x"},"resource":{"type":"global"}}]}`,
			Status: 400, Contains: []string{`INVALID_ARGUMENT`},
		},
		{
			Name: "get descriptor", Method: http.MethodGet,
			Path:   "/v3/projects/p/metricDescriptors/" + queueDepth,
			Status: 200, Contains: []string{`"valueType":"DOUBLE"`, `"tier"`},
		},
		{
			Name: "create channel", Method: http.MethodPost, Path: "/v3/projects/p/notificationChannels",
			Body:   `{"type":"pubsub","displayName":"ps","labels":{"topic":"projects/p/topics/t"}}`,
			Status: 200, Contains: []string{`"type":"pubsub"`, `"verificationStatus":"VERIFIED"`},
		},
		{
			Name: "list channels", Method: http.MethodGet, Path: "/v3/projects/p/notificationChannels",
			Status: 200, Contains: []string{`"displayName":"ps"`},
		},
		{
			Name: "create policy", Method: http.MethodPost, Path: "/v3/projects/p/alertPolicies",
			Body: `{"displayName":"p","combiner":"OR","conditions":[{"displayName":"c","conditionThreshold":{` +
				`"filter":"metric.type=\"` + queueDepth + `\"","comparison":"COMPARISON_GT","thresholdValue":1,` +
				`"duration":"60s","aggregations":[{"alignmentPeriod":"60s","perSeriesAligner":"ALIGN_MEAN"}]}}]}`,
			Status: 200, Contains: []string{`"displayName":"p"`, `"enabled":true`},
		},
		{
			Name: "list policies", Method: http.MethodGet, Path: "/v3/projects/p/alertPolicies",
			Status: 200, Contains: []string{`"alertPolicies"`},
		},
		{
			Name: "create uptime check", Method: http.MethodPost, Path: "/v3/projects/p/uptimeCheckConfigs",
			Body: `{"displayName":"u","monitoredResource":{"type":"uptime_url","labels":{"host":"example.com"}},` +
				`"httpCheck":{"path":"/"},"period":"300s","timeout":"10s"}`,
			Status: 200, Contains: []string{`"port":80`, `"period":"300s"`},
		},
		{
			Name: "create group", Method: http.MethodPost, Path: "/v3/projects/p/groups",
			Body:   `{"displayName":"g","filter":"resource.type=\"gce_instance\""}`,
			Status: 200, Contains: []string{`"displayName":"g"`},
		},
		{
			Name: "missing policy", Method: http.MethodGet, Path: "/v3/projects/p/alertPolicies/404",
			Status: 404, Contains: []string{`NOT_FOUND`},
		},
		{
			Name: "resource descriptors", Method: http.MethodGet, Path: "/v3/projects/p/monitoredResourceDescriptors",
			Status: 200, Contains: []string{`gce_instance`},
		},
	})
}
