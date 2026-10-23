package sts_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

func postFormRegion(t *testing.T, h *sts.Handler, region string, values url.Values) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(values.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = req.WithContext(awsmeta.WithRegion(req.Context(), region))
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

	return rec
}

func TestSTSPartitionARNs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		region    string
		partition string
	}{
		{"us_east_1", "us-east-1", "aws"},
		{"cn_north_1", "cn-north-1", "aws-cn"},
		{"us_gov_west_1", "us-gov-west-1", "aws-us-gov"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			p := "arn:" + tt.partition + ":"

			rec := postFormRegion(t, h, tt.region, url.Values{
				"Action":          {"AssumeRole"},
				"Version":         {"2011-06-15"},
				"RoleArn":         {p + "iam::123456789012:role/R"},
				"RoleSessionName": {"sess"},
			})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var assumed sts.AssumeRoleResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &assumed))
			assert.Equal(t, p+"sts::123456789012:assumed-role/R/sess", assumed.AssumeRoleResult.AssumedRoleUser.Arn)

			rec = postFormRegion(t, h, tt.region, url.Values{
				"Action": {"GetFederationToken"}, "Version": {"2011-06-15"}, "Name": {"fed"},
			})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), p+"sts::")

			rec = postFormRegion(t, h, tt.region, url.Values{
				"Action": {"GetCallerIdentity"}, "Version": {"2011-06-15"},
			})
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), p+"iam::")
		})
	}
}
