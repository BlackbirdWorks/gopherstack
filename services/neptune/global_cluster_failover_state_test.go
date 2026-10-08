package neptune_test

import (
	"maps"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlobalCluster_FailoverState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		extra      url.Values
		name       string
		action     string
		wantStatus string
		wantLoss   string
	}{
		{
			name: "failover_allow_data_loss", action: "FailoverGlobalCluster",
			extra: url.Values{"AllowDataLoss": {"true"}}, wantStatus: "failing-over", wantLoss: "true",
		},
		{
			name: "failover_defaults_to_switchover", action: "FailoverGlobalCluster",
			wantStatus: "switching-over", wantLoss: "false",
		},
		{
			name: "switchover", action: "SwitchoverGlobalCluster",
			wantStatus: "switching-over", wantLoss: "false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createCluster(t, h, "fs-primary")
			doRequest(t, h, url.Values{
				"Action":                    {"CreateGlobalCluster"},
				"Version":                   {"2014-10-31"},
				"GlobalClusterIdentifier":   {"fs-gc"},
				"SourceDBClusterIdentifier": {"fs-primary"},
			})
			createClusterInGlobalCluster(t, h, "fs-secondary", "fs-gc")

			vals := url.Values{
				"Action":                    {tt.action},
				"Version":                   {"2014-10-31"},
				"GlobalClusterIdentifier":   {"fs-gc"},
				"TargetDbClusterIdentifier": {"fs-secondary"},
			}
			maps.Copy(vals, tt.extra)
			rr := doRequest(t, h, vals)
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			body := rr.Body.String()
			assert.Contains(t, body, "<FailoverState>")
			assert.Contains(t, body, "<Status>"+tt.wantStatus+"</Status>")
			assert.Contains(t, body, "<IsDataLossAllowed>"+tt.wantLoss+"</IsDataLossAllowed>")
			assert.Contains(t, body, "<FromDbClusterArn>")
			assert.Contains(t, body, "fs-primary</FromDbClusterArn>")
			assert.Contains(t, body, "fs-secondary</ToDbClusterArn>")

			rr = doRequest(t, h, url.Values{"Action": {"DescribeGlobalClusters"}, "Version": {"2014-10-31"}})
			assert.NotContains(t, rr.Body.String(), "FailoverState")
		})
	}
}
