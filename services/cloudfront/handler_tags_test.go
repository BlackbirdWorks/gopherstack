package cloudfront_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestTagResource_Validation verifies tag key/value/count constraints.
func TestTagResource_Validation(t *testing.T) {
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
			tagBody: `<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
				`<Items><Tag><Key>k</Key><Value>` + strings.Repeat("v", 257) +
				`</Value></Tag></Items></Tags>`,
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

// TestTagResource_MaxTagsPerResource verifies the 50-tag limit is enforced.
func TestTagResource_MaxTagsPerResource(t *testing.T) {
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
		`<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">`+
			`<Items><Tag><Key>key50</Key><Value>v</Value></Tag></Items></Tags>`,
	)
	if over.Code != http.StatusBadRequest {
		t.Errorf("adding 51st tag: got %d want 400: %s", over.Code, over.Body.String())
	}
	if !strings.Contains(over.Body.String(), "InvalidTagging") {
		t.Errorf("expected InvalidTagging, got: %s", over.Body.String())
	}
}

// TestTagging covers TagResource, ListTagsForResource, and UntagResource.
func TestTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) (distARN string)
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		method     string
		extraQuery string
		body       []byte
		wantStatus int
	}{
		{
			name: "tag_resource",
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-tag-001", "tag-dist", true,
					minimalDistConfig("ref-tag-001", "tag-dist", true))
				require.NoError(t, err)

				return d.ARN
			},
			method:     http.MethodPost,
			body:       []byte(`<Tags><Items><Tag><Key>Env</Key><Value>test</Value></Tag></Items></Tags>`),
			wantStatus: http.StatusNoContent,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder) { t.Helper() },
		},
		{
			name: "list_tags_for_resource",
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				d, err := h.Backend.CreateDistribution("ref-tag-002", "list-tag-dist", true,
					minimalDistConfig("ref-tag-002", "list-tag-dist", true))
				require.NoError(t, err)
				err = h.Backend.TagResource(d.ARN, map[string]string{"Env": "prod"})
				require.NoError(t, err)

				return d.ARN
			},
			method:     http.MethodGet,
			body:       nil,
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				// Real ListTagsForResourceOutput binds Tags as the sole httpPayload
				// member, so the wire root is <Tags> directly -- no
				// ListTagsForResourceResponse envelope (cloudfront@v1.67.4
				// deserializers.go: HandleDeserialize decodes straight off the
				// document root into awsRestxml_deserializeDocumentTags).
				assert.Contains(t, rec.Body.String(), "<Tags ")
				assert.NotContains(t, rec.Body.String(), "ListTagsForResourceResponse")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := tt.setup(t, h)
			path := "/2020-05-31/tagging?Resource=" + arn

			rec := doXML(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}

// TestSortedTags verifies tags are returned in sorted order.
func TestSortedTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a distribution via handler.
	rec := doXML(t, h, http.MethodPost, "/2020-05-31/distribution",
		minimalDistConfig("ref-sorted-tags", "tags-dist", true))
	require.Equal(t, http.StatusCreated, rec.Code)

	// Parse the distribution ARN from Location header.
	loc := rec.Header().Get("Location")
	distID := strings.TrimPrefix(loc, "/2020-05-31/distribution/")
	require.NotEmpty(t, distID)

	d, err := h.Backend.GetDistribution(distID)
	require.NoError(t, err)
	arn := d.ARN

	// Add tags.
	tagBody := `<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
		`<Items><Tag><Key>zebra</Key><Value>z</Value></Tag>` +
		`<Tag><Key>apple</Key><Value>a</Value></Tag>` +
		`<Tag><Key>mango</Key><Value>m</Value></Tag></Items></Tags>`
	rec2 := doXML(t, h, http.MethodPost, "/2020-05-31/tagging?Resource="+arn, []byte(tagBody))
	require.Equal(t, http.StatusNoContent, rec2.Code)

	// List tags and verify sorted order.
	rec3 := doXML(t, h, http.MethodGet, "/2020-05-31/tagging?Resource="+arn, nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	body := rec3.Body.String()
	applePos := strings.Index(body, "apple")
	mangoPos := strings.Index(body, "mango")
	zebraPos := strings.Index(body, "zebra")

	assert.Less(t, applePos, mangoPos, "apple should appear before mango")
	assert.Less(t, mangoPos, zebraPos, "mango should appear before zebra")
}

// TestUntagResource verifies the UntagResource handler.
func TestUntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create distribution.
	rec := doXML(t, h, http.MethodPost, "/2020-05-31/distribution",
		minimalDistConfig("ref-untag-001", "untag-dist", true))
	require.Equal(t, http.StatusCreated, rec.Code)

	loc := rec.Header().Get("Location")
	distID := strings.TrimPrefix(loc, "/2020-05-31/distribution/")
	d, err := h.Backend.GetDistribution(distID)
	require.NoError(t, err)

	arn := d.ARN

	// Add tags.
	tagBody := `<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
		`<Items><Tag><Key>env</Key><Value>prod</Value></Tag>` +
		`<Tag><Key>owner</Key><Value>team</Value></Tag></Items></Tags>`
	rec2 := doXML(t, h, http.MethodPost, "/2020-05-31/tagging?Resource="+arn, []byte(tagBody))
	require.Equal(t, http.StatusNoContent, rec2.Code)

	// Untag using body with correct AWS format. Real UntagResource is POST
	// /2020-05-31/tagging?Operation=Untag (cloudfront@v1.67.4 serializers.go:
	// awsRestxml_serializeOpUntagResource's SplitURI), never DELETE.
	untagBody := `<TagKeys><Items><Key>env</Key></Items></TagKeys>`
	rec3 := doXML(t, h, http.MethodPost, "/2020-05-31/tagging?Operation=Untag&Resource="+arn, []byte(untagBody))
	assert.Equal(t, http.StatusNoContent, rec3.Code)

	// Verify env tag was removed.
	rec4 := doXML(t, h, http.MethodGet, "/2020-05-31/tagging?Resource="+arn, nil)
	require.Equal(t, http.StatusOK, rec4.Code)
	assert.NotContains(t, rec4.Body.String(), "env")
	assert.Contains(t, rec4.Body.String(), "owner")
}

// TestTagUntagResource_RealClient drives the real aws-sdk-go-v2 client to
// prove TagResource and UntagResource are both reachable and distinguishable.
// Real TagResource and UntagResource are both POST /2020-05-31/tagging,
// disambiguated only by an "Operation=Tag"/"Operation=Untag" query value
// (cloudfront@v1.67.4 serializers.go:
// awsRestxml_serializeOp{Tag,Untag}Resource's SplitURI); UntagResource is
// never DELETE. gopherstack previously routed POST unconditionally to
// TagResource and DELETE to UntagResource, so every real UntagResource call
// (POST) landed on the TagResource handler instead, which then 400'd trying
// to unmarshal an UntagResource body (root element TagKeys) as Tags
// (gopherstack-o31x).
func TestTagUntagResource_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCloudFrontClient(t, h)

	d, err := h.Backend.CreateDistribution("ref-tag-real-client", "tag-dist-real-client", true,
		minimalDistConfig("ref-tag-real-client", "tag-dist-real-client", true))
	require.NoError(t, err)

	_, err = client.TagResource(t.Context(), &cfsdk.TagResourceInput{
		Resource: aws.String(d.ARN),
		Tags: &types.Tags{
			Items: []types.Tag{
				{Key: aws.String("env"), Value: aws.String("prod")},
				{Key: aws.String("owner"), Value: aws.String("team")},
			},
		},
	})
	require.NoError(t, err)

	tags, err := client.ListTagsForResource(t.Context(), &cfsdk.ListTagsForResourceInput{Resource: aws.String(d.ARN)})
	require.NoError(t, err)
	require.Len(t, tags.Tags.Items, 2)

	_, err = client.UntagResource(t.Context(), &cfsdk.UntagResourceInput{
		Resource: aws.String(d.ARN),
		TagKeys:  &types.TagKeys{Items: []string{"env"}},
	})
	require.NoError(t, err)

	tags, err = client.ListTagsForResource(t.Context(), &cfsdk.ListTagsForResourceInput{Resource: aws.String(d.ARN)})
	require.NoError(t, err)
	require.Len(t, tags.Tags.Items, 1)
	assert.Equal(t, "owner", aws.ToString(tags.Tags.Items[0].Key))
}
