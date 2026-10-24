package cloudwatch_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResourceMetricsConfiguration_QueryProtocol(t *testing.T) {
	t.Parallel()

	const arnParam = "&ResourceArn=arn:aws:ec2:us-east-1:000000000000:instance/i-0123456789abcdef0"

	create := "Action=CreateResourceMetricsConfiguration" + arnParam +
		"&MetricSelections.member.1.IncludeMetrics.member.1=CPUUtilization" +
		"&MetricSelections.member.1.IncludeMetrics.member.2=NetworkIn"

	cases := []struct {
		name         string
		steps        []string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "create then get",
			steps:        []string{create, "Action=GetResourceMetricsConfiguration" + arnParam},
			wantCode:     http.StatusOK,
			wantContains: []string{"GetResourceMetricsConfigurationResult", "<member>NetworkIn</member>"},
		},
		{
			name:         "duplicate create conflicts",
			steps:        []string{create, create},
			wantCode:     http.StatusConflict,
			wantContains: []string{"ConflictException"},
		},
		{
			name:         "delete missing",
			steps:        []string{"Action=DeleteResourceMetricsConfiguration" + arnParam},
			wantCode:     http.StatusNotFound,
			wantContains: []string{"ResourceNotFound"},
		},
		{
			name:         "update otel while stopped",
			steps:        []string{"Action=UpdateOTelEnrichment&IncludeFilters.member.1.Namespace=AWS/EC2"},
			wantCode:     http.StatusNotFound,
			wantContains: []string{"ResourceNotFound"},
		},
		{
			name: "update otel filters",
			steps: []string{
				"Action=StartOTelEnrichment",
				"Action=UpdateOTelEnrichment&IncludeFilters.member.1.Namespace=AWS/EC2" +
					"&IncludeFilters.member.1.MetricNames.member.1=CPUUtilization",
			},
			wantCode: http.StatusOK,
			wantContains: []string{
				"UpdateOTelEnrichmentResult",
				"<Namespace>AWS/EC2</Namespace>",
				"<member>CPUUtilization</member>",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newCWHandler()

			var last string
			var code int
			for _, step := range tc.steps {
				rec := postForm(t, h, step)
				last, code = rec.Body.String(), rec.Code
			}

			assert.Equal(t, tc.wantCode, code)
			for _, want := range tc.wantContains {
				assert.Contains(t, last, want)
			}
		})
	}
}
