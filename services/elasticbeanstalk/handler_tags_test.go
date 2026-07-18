package elasticbeanstalk_test

import (
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := postEBForm(
		t,
		h,
		"Version=2010-12-01&Action=CreateApplication&ApplicationName=tag-app&Tags.member.1.Key=env&Tags.member.1.Value=prod",
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Parse application ARN from create response.
	var resp struct {
		CreateApplicationResult struct {
			Application struct {
				ApplicationArn string `xml:"ApplicationArn"`
			} `xml:"Application"`
		} `xml:"CreateApplicationResult"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	appARN := resp.CreateApplicationResult.Application.ApplicationArn

	tests := []struct {
		name        string
		resourceARN string
		wantTag     string
		wantStatus  int
	}{
		{
			name:        "list tags for existing",
			resourceARN: appARN,
			wantStatus:  http.StatusOK,
			wantTag:     "env",
		},
		{
			name:        "list tags for nonexistent",
			resourceARN: "arn:aws:elasticbeanstalk:us-east-1:123:nonexistent",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:       "missing resource arn",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := "Version=2010-12-01&Action=ListTagsForResource"
			if tt.resourceARN != "" {
				body += "&ResourceArn=" + tt.resourceARN
			}

			rec2 := postEBForm(t, h, body)
			assert.Equal(t, tt.wantStatus, rec2.Code)

			if tt.wantTag != "" {
				assert.Contains(t, rec2.Body.String(), tt.wantTag)
			}
		})
	}
}

// TestHandler_ListTagsForResource_SortedByKey verifies deterministic tag sort order.
func TestHandler_ListTagsForResource_SortedByKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := postEBForm(t, h,
		"Version=2010-12-01&Action=CreateApplication&ApplicationName=tagged-app"+
			"&Tags.member.1.Key=z-key&Tags.member.1.Value=z-val"+
			"&Tags.member.2.Key=a-key&Tags.member.2.Value=a-val"+
			"&Tags.member.3.Key=m-key&Tags.member.3.Value=m-val")
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		CreateApplicationResult struct {
			Application struct {
				ApplicationArn string `xml:"ApplicationArn"`
			} `xml:"Application"`
		} `xml:"CreateApplicationResult"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &createResp))

	arn := createResp.CreateApplicationResult.Application.ApplicationArn
	require.NotEmpty(t, arn)

	rec = postEBForm(t, h,
		"Version=2010-12-01&Action=ListTagsForResource&ResourceArn="+arn)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	posA := indexOfFirst(body, "a-key")
	posM := indexOfFirst(body, "m-key")
	posZ := indexOfFirst(body, "z-key")

	assert.Less(t, posA, posM, "a-key should come before m-key")
	assert.Less(t, posM, posZ, "m-key should come before z-key")
}

func TestHandler_UpdateTagsForResource(t *testing.T) {
	t.Parallel()

	// createAppAndGetARN creates a tagged application and returns its ARN.
	createAppAndGetARN := func(h *elasticbeanstalk.Handler) string {
		createBody := "Version=2010-12-01&Action=CreateApplication" +
			"&ApplicationName=tag-app&Tags.member.1.Key=k1&Tags.member.1.Value=v1"
		rec := postEBForm(t, h, createBody)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			CreateApplicationResult struct {
				Application struct {
					ApplicationArn string `xml:"ApplicationArn"`
				} `xml:"Application"`
			} `xml:"CreateApplicationResult"`
		}

		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

		return resp.CreateApplicationResult.Application.ApplicationArn
	}

	tests := []struct {
		setup      func(*elasticbeanstalk.Handler) string
		name       string
		body       string
		wantStatus int
	}{
		{
			name:  "add tags",
			setup: createAppAndGetARN,
			body: "Version=2010-12-01&Action=UpdateTagsForResource" +
				"&TagsToAdd.member.1.Key=k2&TagsToAdd.member.1.Value=v2",
			wantStatus: http.StatusOK,
		},
		{
			name:       "remove tags",
			setup:      createAppAndGetARN,
			body:       "Version=2010-12-01&Action=UpdateTagsForResource&TagsToRemove.member.1=k1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing resource arn",
			body:       "Version=2010-12-01&Action=UpdateTagsForResource",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			body := tt.body
			if tt.setup != nil {
				arn := tt.setup(h)
				body = body + "&ResourceArn=" + arn
			}

			rec := postEBForm(t, h, body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_TagsForResource_ArnNotFoundUsesResourceNotFoundException verifies that
// ListTagsForResource/UpdateTagsForResource surface the specific, documented
// ResourceNotFoundException wire code (not the generic InvalidParameterValue
// used by name-based lookups) when ResourceArn matches no known resource.
func TestHandler_TagsForResource_ArnNotFoundUsesResourceNotFoundException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "ListTagsForResource",
			body: "Version=2010-12-01&Action=ListTagsForResource" +
				"&ResourceArn=arn:aws:elasticbeanstalk:us-east-1:123456789012:application/ghost",
		},
		{
			name: "UpdateTagsForResource",
			body: "Version=2010-12-01&Action=UpdateTagsForResource" +
				"&ResourceArn=arn:aws:elasticbeanstalk:us-east-1:123456789012:application/ghost" +
				"&TagsToAdd.member.1.Key=k&TagsToAdd.member.1.Value=v",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := postEBForm(t, h, tt.body)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "<Code>ResourceNotFoundException</Code>")
		})
	}
}
