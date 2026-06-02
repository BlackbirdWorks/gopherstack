package cloudfront_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	echo "github.com/labstack/echo/v5"
)

// cfRequestWithHeader fires a request through the handler with optional extra headers.
func cfRequestWithHeader(
	t *testing.T,
	h interface{ Handler() echo.HandlerFunc },
	method, path, body string,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyReader *bytes.Reader
	if body != "" {
		bodyReader = bytes.NewReader([]byte(body))
	} else {
		bodyReader = bytes.NewReader(nil)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bodyReader)
	req.Header.Set("Content-Type", "application/xml")

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rr := httptest.NewRecorder()
	c := e.NewContext(req, rr)

	if err := h.Handler()(c); err != nil {
		t.Fatalf("handler error: %v", err)
	}

	return rr
}

// TestBatch2Accuracy_TagResource_Validation verifies tag key/value/count constraints.
func TestBatch2Accuracy_TagResource_Validation(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"

	tests := []struct {
		name     string
		tagBody  string
		wantErr  string
		wantCode int
	}{
		{
			name: "empty_key_rejected",
			tagBody: `<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
				`<Items><Tag><Key></Key><Value>v</Value></Tag></Items></Tags>`,
			wantCode: http.StatusBadRequest,
			wantErr:  "InvalidTagging",
		},
		{
			name: "key_too_long_rejected",
			tagBody: `<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/"><Items><Tag><Key>` + strings.Repeat(
				"k",
				129,
			) + `</Key><Value>v</Value></Tag></Items></Tags>`,
			wantCode: http.StatusBadRequest,
			wantErr:  "InvalidTagging",
		},
		{
			name: "value_too_long_rejected",
			tagBody: `<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/"><Items><Tag><Key>k</Key><Value>` + strings.Repeat(
				"v",
				257,
			) + `</Value></Tag></Items></Tags>`,
			wantCode: http.StatusBadRequest,
			wantErr:  "InvalidTagging",
		},
		{
			name: "aws_prefix_rejected",
			tagBody: `<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
				`<Items><Tag><Key>aws:reserved</Key><Value>v</Value></Tag></Items></Tags>`,
			wantCode: http.StatusBadRequest,
			wantErr:  "InvalidTagging",
		},
		{
			name: "valid_tag_accepted",
			tagBody: `<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
				`<Items><Tag><Key>env</Key><Value>prod</Value></Tag></Items></Tags>`,
			wantCode: http.StatusNoContent,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newCFHandler(t)
			// Create distribution.
			rr := cfRequest(
				t,
				h,
				http.MethodPost,
				prefix+"distribution",
				`<DistributionConfig><CallerReference>cr-`+tc.name+`</CallerReference><Enabled>true</Enabled></DistributionConfig>`,
			)
			if rr.Code != http.StatusCreated {
				t.Fatalf("create distribution got %d: %s", rr.Code, rr.Body.String())
			}
			distID := extractXMLID(t, rr.Body.String())
			arn := fmt.Sprintf("arn:aws:cloudfront::123456789012:distribution/%s", distID)

			tagRR := cfRequest(t, h, http.MethodPost, prefix+"tagging?Resource="+arn, tc.tagBody)
			if tagRR.Code != tc.wantCode {
				t.Errorf("got %d, want %d: %s", tagRR.Code, tc.wantCode, tagRR.Body.String())
			}
			if tc.wantErr != "" && !strings.Contains(tagRR.Body.String(), tc.wantErr) {
				t.Errorf("want error %q in body, got: %s", tc.wantErr, tagRR.Body.String())
			}
		})
	}
}

// TestBatch2Accuracy_TagResource_MaxTagsPerResource verifies the 50-tag limit is enforced.
func TestBatch2Accuracy_TagResource_MaxTagsPerResource(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"

	h := newCFHandler(t)
	rr := cfRequest(
		t,
		h,
		http.MethodPost,
		prefix+"distribution",
		`<DistributionConfig><CallerReference>cr-max-tags</CallerReference><Enabled>true</Enabled></DistributionConfig>`,
	)
	if rr.Code != http.StatusCreated {
		t.Fatalf("create distribution got %d: %s", rr.Code, rr.Body.String())
	}
	distID := extractXMLID(t, rr.Body.String())
	arn := fmt.Sprintf("arn:aws:cloudfront::123456789012:distribution/%s", distID)

	// Build a tag body with exactly 50 tags (should succeed).
	var sb strings.Builder
	sb.WriteString(`<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/"><Items>`)
	for i := range 50 {
		fmt.Fprintf(&sb, `<Tag><Key>key%d</Key><Value>val%d</Value></Tag>`, i, i)
	}
	sb.WriteString(`</Items></Tags>`)

	ok50 := cfRequest(t, h, http.MethodPost, prefix+"tagging?Resource="+arn, sb.String())
	if ok50.Code != http.StatusNoContent {
		t.Fatalf("adding 50 tags: got %d want 204: %s", ok50.Code, ok50.Body.String())
	}

	// Adding one more (key50) exceeds limit — should fail with InvalidTagging.
	over := cfRequest(
		t,
		h,
		http.MethodPost,
		prefix+"tagging?Resource="+arn,
		`<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/"><Items><Tag><Key>key50</Key><Value>v</Value></Tag></Items></Tags>`,
	)
	if over.Code != http.StatusBadRequest {
		t.Errorf("adding 51st tag: got %d want 400: %s", over.Code, over.Body.String())
	}
	if !strings.Contains(over.Body.String(), "InvalidTagging") {
		t.Errorf("expected InvalidTagging, got: %s", over.Body.String())
	}
}

// TestBatch2Accuracy_CreateDistributionTenant_RequiresDistributionId checks that
// an empty DistributionId is rejected with 400 InvalidArgument.
func TestBatch2Accuracy_CreateDistributionTenant_RequiresDistributionId(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"

	tests := []struct {
		name     string
		body     string
		wantErr  string
		wantCode int
	}{
		{
			name: "missing_distribution_id",
			body: `<CreateDistributionTenantRequest>
				<Domain>tenant-test.com</Domain>
			</CreateDistributionTenantRequest>`,
			wantCode: http.StatusBadRequest,
			wantErr:  "InvalidArgument",
		},
		{
			name: "empty_distribution_id",
			body: `<CreateDistributionTenantRequest>
				<DistributionId></DistributionId>
				<Domain>tenant-test2.com</Domain>
			</CreateDistributionTenantRequest>`,
			wantCode: http.StatusBadRequest,
			wantErr:  "InvalidArgument",
		},
		{
			name: "valid_with_distribution_id",
			body: `<CreateDistributionTenantRequest>
				<DistributionId>dist-xyz</DistributionId>
				<Domain>tenant-valid.com</Domain>
			</CreateDistributionTenantRequest>`,
			wantCode: http.StatusCreated,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newCFHandler(t)
			rr := cfRequest(t, h, http.MethodPost, prefix+"distribution-tenant", tc.body)
			if rr.Code != tc.wantCode {
				t.Errorf("got %d want %d: %s", rr.Code, tc.wantCode, rr.Body.String())
			}
			if tc.wantErr != "" && !strings.Contains(rr.Body.String(), tc.wantErr) {
				t.Errorf("want %q in body, got: %s", tc.wantErr, rr.Body.String())
			}
		})
	}
}

// TestBatch2Accuracy_UpdateDistributionTenant_RequiresIfMatch verifies that
// UpdateDistributionTenant returns 412 when If-Match is absent or stale.
func TestBatch2Accuracy_UpdateDistributionTenant_RequiresIfMatch(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"

	tests := []struct {
		name     string
		ifMatch  string
		wantErr  string
		wantCode int
	}{
		{
			name:     "missing_if_match",
			ifMatch:  "",
			wantCode: http.StatusPreconditionFailed,
			wantErr:  "PreconditionFailed",
		},
		{
			name:     "wrong_etag",
			ifMatch:  "wrong-etag",
			wantCode: http.StatusPreconditionFailed,
			wantErr:  "PreconditionFailed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newCFHandler(t)
			createRR := cfRequest(t, h, http.MethodPost, prefix+"distribution-tenant",
				`<CreateDistributionTenantRequest>`+
					`<DistributionId>dist-001</DistributionId>`+
					`<Domain>update-match-`+tc.name+`.com</Domain>`+
					`</CreateDistributionTenantRequest>`)
			if createRR.Code != http.StatusCreated {
				t.Fatalf("create got %d: %s", createRR.Code, createRR.Body.String())
			}
			tenantID := extractXMLID(t, createRR.Body.String())

			var headers map[string]string
			if tc.ifMatch != "" {
				headers = map[string]string{"If-Match": tc.ifMatch}
			}
			rr := cfRequestWithHeader(t, h, http.MethodPut, prefix+"distribution-tenant/"+tenantID, "", headers)
			if rr.Code != tc.wantCode {
				t.Errorf("got %d want %d: %s", rr.Code, tc.wantCode, rr.Body.String())
			}
			if tc.wantErr != "" && !strings.Contains(rr.Body.String(), tc.wantErr) {
				t.Errorf("want %q in body, got: %s", tc.wantErr, rr.Body.String())
			}
		})
	}
}

// TestBatch2Accuracy_DeleteDistributionTenant_RequiresIfMatch verifies that
// DeleteDistributionTenant returns 412 when If-Match is absent or stale.
func TestBatch2Accuracy_DeleteDistributionTenant_RequiresIfMatch(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"

	tests := []struct {
		name     string
		ifMatch  string
		wantErr  string
		wantCode int
	}{
		{
			name:     "missing_if_match",
			ifMatch:  "",
			wantCode: http.StatusPreconditionFailed,
			wantErr:  "PreconditionFailed",
		},
		{
			name:     "wrong_etag",
			ifMatch:  "bad-etag-value",
			wantCode: http.StatusPreconditionFailed,
			wantErr:  "PreconditionFailed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newCFHandler(t)
			createRR := cfRequest(t, h, http.MethodPost, prefix+"distribution-tenant",
				`<CreateDistributionTenantRequest>`+
					`<DistributionId>dist-001</DistributionId>`+
					`<Domain>delete-match-`+tc.name+`.com</Domain>`+
					`</CreateDistributionTenantRequest>`)
			if createRR.Code != http.StatusCreated {
				t.Fatalf("create got %d: %s", createRR.Code, createRR.Body.String())
			}
			tenantID := extractXMLID(t, createRR.Body.String())

			var headers map[string]string
			if tc.ifMatch != "" {
				headers = map[string]string{"If-Match": tc.ifMatch}
			}
			rr := cfRequestWithHeader(t, h, http.MethodDelete, prefix+"distribution-tenant/"+tenantID, "", headers)
			if rr.Code != tc.wantCode {
				t.Errorf("got %d want %d: %s", rr.Code, tc.wantCode, rr.Body.String())
			}
			if tc.wantErr != "" && !strings.Contains(rr.Body.String(), tc.wantErr) {
				t.Errorf("want %q in body, got: %s", tc.wantErr, rr.Body.String())
			}
		})
	}
}

// TestBatch2Accuracy_CreateDistributionWithTags_InvalidTagging verifies that
// invalid tags on CreateDistributionWithTags are rejected.
func TestBatch2Accuracy_CreateDistributionWithTags_InvalidTagging(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"

	tests := []struct {
		name     string
		body     string
		wantErr  string
		wantCode int
	}{
		{
			name: "aws_prefix_key",
			body: `<DistributionConfigWithTags>
				<DistributionConfig>
					<CallerReference>cr-aws-pfx</CallerReference>
					<Enabled>true</Enabled>
				</DistributionConfig>
				<Tags><Tag><Key>aws:reserved</Key><Value>v</Value></Tag></Tags>
			</DistributionConfigWithTags>`,
			wantCode: http.StatusBadRequest,
			wantErr:  "InvalidTagging",
		},
		{
			name: "key_too_long",
			body: `<DistributionConfigWithTags><DistributionConfig><CallerReference>cr-long-key</CallerReference><Enabled>true</Enabled></DistributionConfig><Tags><Tag><Key>` + strings.Repeat(
				"k",
				129,
			) + `</Key><Value>v</Value></Tag></Tags></DistributionConfigWithTags>`,
			wantCode: http.StatusBadRequest,
			wantErr:  "InvalidTagging",
		},
		{
			name: "valid_tags",
			body: `<DistributionConfigWithTags>
				<DistributionConfig>
					<CallerReference>cr-valid-tags</CallerReference>
					<Enabled>true</Enabled>
				</DistributionConfig>
				<Tags><Tag><Key>env</Key><Value>prod</Value></Tag></Tags>
			</DistributionConfigWithTags>`,
			wantCode: http.StatusCreated,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newCFHandler(t)
			rr := cfRequest(t, h, http.MethodPost, prefix+"distribution?Resource=WithTags", tc.body)
			if rr.Code != tc.wantCode {
				t.Errorf("got %d want %d: %s", rr.Code, tc.wantCode, rr.Body.String())
			}
			if tc.wantErr != "" && !strings.Contains(rr.Body.String(), tc.wantErr) {
				t.Errorf("want %q in body, got: %s", tc.wantErr, rr.Body.String())
			}
		})
	}
}
