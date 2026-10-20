package app

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

func runPartitionMiddleware(t *testing.T, region, resourceARN string) (int, bool) {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/tags/"+resourceARN, nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AK/20240101/"+region+"/sqs/aws4_request")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Service: "sqs"}))
	rec := httptest.NewRecorder()

	called := false
	h := partitionStrictMiddleware()(func(c *echo.Context) error {
		called = true

		return c.NoContent(http.StatusOK)
	})
	require.NoError(t, h(echo.New().NewContext(req, rec)))

	return rec.Code, called
}

func TestPartitionStrict_NoCommercialToleranceInForeignPartitions(t *testing.T) {
	tests := []struct {
		name       string
		region     string
		arn        string
		wantStatus int
		strictOff  bool
		wantNext   bool
	}{
		{"us_aws_ok", "us-east-1", "arn:aws:sqs:us-east-1:1:q", http.StatusOK, false, true},
		{"cn_cn_ok", "cn-north-1", "arn:aws-cn:sqs:cn-north-1:1:q", http.StatusOK, false, true},
		{"cn_aws_rejected", "cn-north-1", "arn:aws:sqs:cn-north-1:1:q", http.StatusBadRequest, false, false},
		{"gov_aws_rejected", "us-gov-west-1", "arn:aws:s3:::b", http.StatusBadRequest, false, false},
		{"gov_gov_ok", "us-gov-west-1", "arn:aws-us-gov:sqs:us-gov-west-1:1:q", http.StatusOK, false, true},
		{"cn_aws_non_strict", "cn-north-1", "arn:aws:sqs:cn-north-1:1:q", http.StatusOK, true, true},
		{"us_cn_non_strict", "us-east-1", "arn:aws-cn:sqs:cn-north-1:1:q", http.StatusOK, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.strictOff {
				t.Setenv(arn.EnvPartitionStrict, "false")
			} else {
				t.Setenv(arn.EnvPartitionStrict, "")
			}

			status, called := runPartitionMiddleware(t, tt.region, tt.arn)
			assert.Equal(t, tt.wantStatus, status)
			assert.Equal(t, tt.wantNext, called)
		})
	}
}
