package iam_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestExtractTargetIAMActionNamespaced(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{"bare", "CloudTrail_20131101.DescribeTrails", "cloudtrail:DescribeTrails"},
		{
			"botocore",
			"com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.DescribeTrails",
			"cloudtrail:DescribeTrails",
		},
		{"unknown", "Nope.Op", ""},
		{"no_op", "CloudTrail_20131101.", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodPost, "/", nil)
			r.Header.Set("X-Amz-Target", tt.target)
			assert.Equal(t, tt.want, iam.ExtractTargetOrFormIAMAction(r))
		})
	}
}
