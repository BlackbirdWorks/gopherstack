package ec2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDescribeInstanceTypes_Pagination verifies that DescribeInstanceTypes
// echoes multiple `InstanceType.N` requests, honors `MaxResults` bounds, and
// produces a NextToken for follow-up paging that matches AWS conventions.
func TestDescribeInstanceTypes_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantNotIn    []string
		wantCode     int
	}{
		{
			name: "echoes_multiple_instance_types",
			body: "Action=DescribeInstanceTypes&Version=2016-11-15" +
				"&InstanceType.1=t2.micro&InstanceType.2=t3.small&InstanceType.3=m5.large",
			wantCode:     http.StatusOK,
			wantContains: []string{"t2.micro", "t3.small", "m5.large"},
		},
		{
			name: "max_results_truncates_and_returns_next_token",
			body: "Action=DescribeInstanceTypes&Version=2016-11-15" +
				"&InstanceType.1=t2.micro&InstanceType.2=t3.small&InstanceType.3=m5.large&MaxResults=5",
			wantCode:     http.StatusOK,
			wantContains: []string{"t2.micro", "t3.small", "m5.large"},
			wantNotIn:    []string{"<nextToken>"},
		},
		{
			name: "next_token_resumes_from_offset",
			body: "Action=DescribeInstanceTypes&Version=2016-11-15" +
				"&InstanceType.1=t2.micro&InstanceType.2=t3.small&InstanceType.3=m5.large" +
				"&MaxResults=5&NextToken=2",
			wantCode:     http.StatusOK,
			wantContains: []string{"m5.large"},
			wantNotIn:    []string{"t2.micro", "t3.small"},
		},
		{
			name: "max_results_below_minimum_rejected",
			body: "Action=DescribeInstanceTypes&Version=2016-11-15" +
				"&InstanceType.1=t2.micro&MaxResults=1",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "max_results_above_maximum_rejected",
			body: "Action=DescribeInstanceTypes&Version=2016-11-15" +
				"&InstanceType.1=t2.micro&MaxResults=500",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "invalid_next_token_rejected",
			body: "Action=DescribeInstanceTypes&Version=2016-11-15" +
				"&InstanceType.1=t2.micro&NextToken=garbage",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			body := rec.Body.String()
			for _, s := range tt.wantContains {
				assert.Contains(t, body, s)
			}

			for _, s := range tt.wantNotIn {
				assert.NotContains(t, body, s)
			}
		})
	}
}
