package cloudformation_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
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

// TestListGeneratedTemplates_TiedNamePageWalk proves ListGeneratedTemplates
// sorts on GeneratedTemplateName alone -- a field CreateGeneratedTemplate
// never checks for uniqueness -- over b.generatedTemplates.All() (a
// store.Table map walk, unstable between calls). page.New then paginates
// that order with an offset-index scheme. Several templates sharing one
// Name can therefore land in a different relative order on each call, so a
// page boundary that fell between two tied templates on one call falls
// between two different tied templates on the next -- one gets dropped or
// duplicated across the page boundary with nothing else changed. Looped: a
// single walk can pass by luck since map iteration is randomized per-call.
func TestListGeneratedTemplates_TiedNamePageWalk(t *testing.T) {
	t.Parallel()

	b := newBackend()

	// ListGeneratedTemplates hardcodes cfnDefaultPageSize (100) as its page
	// size -- it takes no maxResults param -- so total must exceed 100 to
	// force a page boundary at all.
	const total = 110

	want := make(map[string]bool, total)

	for range total {
		gt, err := b.CreateGeneratedTemplate("shared-name", nil)
		require.NoError(t, err)
		want[gt.GeneratedTemplateID] = true
	}

	const pageSize = 100

	for iter := range 30 {
		got := make(map[string]int, total)

		token := ""
		for range total/pageSize + 2 {
			p, err := b.ListGeneratedTemplates(token)
			require.NoError(t, err)

			for _, gt := range p.Data {
				got[gt.GeneratedTemplateID]++
			}

			if p.Next == "" {
				break
			}

			token = p.Next
		}

		require.Lenf(
			t, got, total,
			"iteration %d: page walk produced %d distinct templates, want %d", iter, len(got), total,
		)

		for id := range want {
			require.Equalf(
				t, 1, got[id],
				"iteration %d: template %s appeared %d times across the page walk", iter, id, got[id],
			)
		}
	}
}

// TestDescribeEvents_AllStacksTiedTimestampPageWalk proves that, when
// StackName is omitted, DescribeEvents flattens b.events (a raw
// map[string][]StackEvent keyed by stack ID) by ranging it directly --
// unspecified Go map order -- before sorting by Timestamp. Two events on
// different stacks sharing an exact Timestamp can therefore land in a
// different relative order on each call, so a page boundary that fell
// between two tied events on one call falls between two different tied
// events on the next -- one gets dropped or duplicated across the page
// boundary with nothing else changed. Looped: a single walk can pass by
// luck since map iteration is randomized per-call.
func TestDescribeEvents_AllStacksTiedTimestampPageWalk(t *testing.T) {
	t.Parallel()

	b := newBackend()

	// DescribeEvents hardcodes cfnDefaultPageSize (100) as its page size --
	// it takes no maxResults param -- so total must exceed 100 to force a
	// page boundary at all.
	const stacks = 4
	const eventsPerStack = 30
	const total = stacks * eventsPerStack

	tied := time.Now()

	want := make(map[string]bool, total)

	for s := range stacks {
		stackID := "stack-" + strconv.Itoa(s)

		for e := range eventsPerStack {
			eventID := "evt-" + strconv.Itoa(s) + "-" + strconv.Itoa(e)
			b.AddStackEventInternal(stackID, cloudformation.StackEvent{
				EventID:   eventID,
				StackID:   stackID,
				Timestamp: tied,
			})
			want[eventID] = true
		}
	}

	const pageSize = 100

	for iter := range 30 {
		got := make(map[string]int, total)

		token := ""
		for range total/pageSize + 2 {
			p, err := b.DescribeEvents("", token, false)
			require.NoError(t, err)

			for _, evt := range p.Data {
				got[evt.EventID]++
			}

			if p.Next == "" {
				break
			}

			token = p.Next
		}

		require.Lenf(
			t, got, total,
			"iteration %d: page walk produced %d distinct events, want %d", iter, len(got), total,
		)

		for id := range want {
			require.Equalf(
				t, 1, got[id],
				"iteration %d: event %s appeared %d times across the page walk", iter, id, got[id],
			)
		}
	}
}

// TestListResourceScans_PageWalkReproducesFullSet proves ListResourceScans
// sorts nothing before paginating: it builds its list from
// b.resourceScans.All() (a store.Table map walk, unstable between calls)
// and hands it straight to page.New's offset-index scheme. Looped: a single
// walk can pass by luck.
func TestListResourceScans_PageWalkReproducesFullSet(t *testing.T) {
	t.Parallel()

	b := newBackend()

	// ListResourceScans hardcodes cfnDefaultPageSize (100) as its page size
	// -- it takes no maxResults param -- so total must exceed 100 to force a
	// page boundary at all.
	const total = 110

	want := make(map[string]bool, total)

	for range total {
		scanID, err := b.StartResourceScan()
		require.NoError(t, err)
		want[scanID] = true
	}

	const pageSize = 100

	for iter := range 30 {
		got := make(map[string]int, total)

		token := ""
		for range total/pageSize + 2 {
			p, err := b.ListResourceScans(token)
			require.NoError(t, err)

			for _, rs := range p.Data {
				got[rs.ResourceScanID]++
			}

			if p.Next == "" {
				break
			}

			token = p.Next
		}

		require.Lenf(
			t, got, total,
			"iteration %d: page walk produced %d distinct resource scans, want %d", iter, len(got), total,
		)

		for id := range want {
			require.Equalf(
				t, 1, got[id],
				"iteration %d: resource scan %s appeared %d times across the page walk", iter, id, got[id],
			)
		}
	}
}
