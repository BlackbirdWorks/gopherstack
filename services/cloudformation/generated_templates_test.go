package cloudformation_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCFN_GeneratedTemplates(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// CreateGeneratedTemplate
	rec := postForm(t, h, url.Values{
		"Action":                []string{"CreateGeneratedTemplate"},
		"GeneratedTemplateName": []string{"my-gen-template"},
	}.Encode())
	require.True(t, rec.Code >= 200 && rec.Code < 300, "CreateGeneratedTemplate: %d %s", rec.Code, rec.Body.String())

	// ListGeneratedTemplates
	rec = postForm(t, h, url.Values{
		"Action": []string{"ListGeneratedTemplates"},
	}.Encode())
	assert.True(t, rec.Code >= 200 && rec.Code < 300)

	// DescribeGeneratedTemplate -- gopherstack-7185: this and the three ops
	// below used to read the wrong wire key ("GeneratedTemplateId" instead
	// of "GeneratedTemplateName", the real request field), so every one of
	// them 400'd for any real client and this test's old `|| rec.Code ==
	// 400` masked it.
	rec = postForm(t, h, url.Values{
		"Action":                []string{"DescribeGeneratedTemplate"},
		"GeneratedTemplateName": []string{"my-gen-template"},
	}.Encode())
	require.True(t, rec.Code >= 200 && rec.Code < 300, "DescribeGeneratedTemplate: %d %s", rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "my-gen-template")

	// GetGeneratedTemplate
	rec = postForm(t, h, url.Values{
		"Action":                []string{"GetGeneratedTemplate"},
		"GeneratedTemplateName": []string{"my-gen-template"},
	}.Encode())
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "GetGeneratedTemplate: %d %s", rec.Code, rec.Body.String())

	// UpdateGeneratedTemplate
	rec = postForm(t, h, url.Values{
		"Action":                   []string{"UpdateGeneratedTemplate"},
		"GeneratedTemplateName":    []string{"my-gen-template"},
		"NewGeneratedTemplateName": []string{"my-gen-template-v2"},
	}.Encode())
	require.True(t, rec.Code >= 200 && rec.Code < 300, "UpdateGeneratedTemplate: %d %s", rec.Code, rec.Body.String())

	// DeleteGeneratedTemplate -- addressed by name, matching real AWS
	// (GeneratedTemplateName accepts "name or ARN"), not by the opaque ID
	// UpdateGeneratedTemplate's response carries.
	rec = postForm(t, h, url.Values{
		"Action":                []string{"DeleteGeneratedTemplate"},
		"GeneratedTemplateName": []string{"my-gen-template-v2"},
	}.Encode())
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "DeleteGeneratedTemplate: %d %s", rec.Code, rec.Body.String())
}

func TestCFN_ResourceScans(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// StartResourceScan
	rec := postForm(t, h, url.Values{
		"Action": []string{"StartResourceScan"},
	}.Encode())
	require.True(t, rec.Code >= 200 && rec.Code < 300, "StartResourceScan: %d %s", rec.Code, rec.Body.String())

	var startResp struct {
		ScanID string `xml:"StartResourceScanResult>ResourceScanId"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &startResp))
	require.NotEmpty(t, startResp.ScanID, "StartResourceScan must return a ResourceScanId")

	// ListResourceScans must include the scan just started.
	rec = postForm(t, h, url.Values{
		"Action": []string{"ListResourceScans"},
	}.Encode())
	require.True(t, rec.Code >= 200 && rec.Code < 300, "ListResourceScans: %d %s", rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), startResp.ScanID)
}

// TestUnsuffixedNotFoundCodes verifies that GeneratedTemplate and
// ResourceScan not-found errors use the exact (unsuffixed) wire code the SDK
// models — ErrorCode() on GeneratedTemplateNotFoundException/
// ResourceScanNotFoundException returns "GeneratedTemplateNotFound" /
// "ResourceScanNotFound" with no "Exception" suffix (same bug class as the
// already-fixed ChangeSetNotFound). Sending the "...Exception"-suffixed code
// this codebase previously emitted means aws-sdk-go-v2 clients never
// recognize it as the typed exception and fall back to a generic APIError.
func TestUnsuffixedNotFoundCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		idParam  string
		wantCode string
	}{
		{
			name:     "DescribeGeneratedTemplate",
			action:   "DescribeGeneratedTemplate",
			idParam:  "GeneratedTemplateName",
			wantCode: "GeneratedTemplateNotFound",
		},
		{
			name:     "GetGeneratedTemplate",
			action:   "GetGeneratedTemplate",
			idParam:  "GeneratedTemplateName",
			wantCode: "GeneratedTemplateNotFound",
		},
		{
			name:     "UpdateGeneratedTemplate",
			action:   "UpdateGeneratedTemplate",
			idParam:  "GeneratedTemplateName",
			wantCode: "GeneratedTemplateNotFound",
		},
		{
			name:     "DeleteGeneratedTemplate",
			action:   "DeleteGeneratedTemplate",
			idParam:  "GeneratedTemplateName",
			wantCode: "GeneratedTemplateNotFound",
		},
		{
			name:     "DescribeResourceScan",
			action:   "DescribeResourceScan",
			idParam:  "ResourceScanId",
			wantCode: "ResourceScanNotFound",
		},
		{
			name:     "ListResourceScanResources",
			action:   "ListResourceScanResources",
			idParam:  "ResourceScanId",
			wantCode: "ResourceScanNotFound",
		},
		{
			name:     "ListResourceScanRelatedResources",
			action:   "ListResourceScanRelatedResources",
			idParam:  "ResourceScanId",
			wantCode: "ResourceScanNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			rec := postForm(t, h, url.Values{
				"Action":   {tt.action},
				tt.idParam: {"does-not-exist"},
			}.Encode())

			assert.NotEqual(t, http.StatusOK, rec.Code, "%s on an unknown ID must fail", tt.action)
			assert.Contains(t, rec.Body.String(), "<Code>"+tt.wantCode+"</Code>",
				"%s must use the exact unsuffixed SDK-modeled error code", tt.action)
		})
	}
}

// TestResourceScanResources covers ListResourceScanResources and ListResourceScanRelatedResources.
func TestResourceScanResources(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Start a scan first
	rec := postForm(t, h, url.Values{
		"Action": []string{"StartResourceScan"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	scanID := extractField(rec.Body.String(), "ResourceScanId")
	require.NotEmpty(t, scanID, "ResourceScanId must be non-empty")

	// ListResourceScanResources — valid scan
	rec = postForm(t, h, url.Values{
		"Action":         []string{"ListResourceScanResources"},
		"ResourceScanId": []string{scanID},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AWS::S3::Bucket")

	// ListResourceScanResources — invalid scan should fail with the SDK-modeled
	// (unsuffixed) ResourceScanNotFound code, matching DescribeResourceScan.
	rec = postForm(t, h, url.Values{
		"Action":         []string{"ListResourceScanResources"},
		"ResourceScanId": []string{"nonexistent-scan"},
	}.Encode())
	assert.NotEqual(t, http.StatusOK, rec.Code,
		"ListResourceScanResources on an unknown scan must fail")
	assert.Contains(t, rec.Body.String(), "<Code>ResourceScanNotFound</Code>",
		"error code must be the SDK-modeled unsuffixed ResourceScanNotFound, not ...NotFoundException")

	// ListResourceScanRelatedResources — valid scan
	rec = postForm(t, h, url.Values{
		"Action":         []string{"ListResourceScanRelatedResources"},
		"ResourceScanId": []string{scanID},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
}
