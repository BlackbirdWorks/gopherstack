package omics_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	omicssdk "github.com/aws/aws-sdk-go-v2/service/omics"
	"github.com/aws/aws-sdk-go-v2/service/omics/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/omics"
)

func TestOmics_Share(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		check    func(t *testing.T, body []byte)
		body     any
		method   string
		path     string
		wantCode int
	}{
		{
			name:   "CreateShare returns 201",
			method: http.MethodPost,
			path:   "/share",
			body: map[string]any{
				"resourceArn":         "arn:aws:omics:us-east-1:000000000000:annotationStore/mystore",
				"principalSubscriber": "123456789012",
				"shareName":           "my-share",
			},
			wantCode: http.StatusCreated,
		},
		{
			name:     "GetShare unknown returns 404",
			method:   http.MethodGet,
			path:     "/share/doesnotexist",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteShare unknown returns 404",
			method:   http.MethodDelete,
			path:     "/share/doesnotexist",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// createShare creates a single share and returns its shareId.
func createShare(t *testing.T, h *omics.Handler) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/share", map[string]any{
		"resourceArn":         "arn:aws:omics:us-east-1:000000000000:annotationStore/mystore",
		"principalSubscriber": "123456789012",
		"shareName":           "my-share",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	id, _ := resp["shareId"].(string)

	return id
}

// TestOmics_Share_ResponseShape asserts the raw JSON body of each share
// operation against omics@v1.49.5's declared output members: AcceptShareOutput
// and DeleteShareOutput carry only Status (api_op_AcceptShare.go:39-48,
// api_op_DeleteShare.go:41-50), CreateShareOutput carries ShareId, ShareName,
// Status (api_op_CreateShare.go:58-73), and GetShareOutput carries the whole
// Share object under a "share" key (api_op_GetShare.go:39-49). An SDK-driven
// test cannot catch extra keys -- the deserializer silently drops anything it
// doesn't recognize -- so this asserts the raw body directly.
func TestOmics_Share_ResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		request  func(t *testing.T, h *omics.Handler) *httptest.ResponseRecorder
		name     string
		wantKeys []string
	}{
		{
			name: "create share emits shareId shareName status only",
			request: func(t *testing.T, h *omics.Handler) *httptest.ResponseRecorder {
				t.Helper()

				return doRequest(t, h, http.MethodPost, "/share", map[string]any{
					"resourceArn":         "arn:aws:omics:us-east-1:000000000000:annotationStore/mystore",
					"principalSubscriber": "123456789012",
					"shareName":           "my-share",
				})
			},
			wantKeys: []string{"shareId", "shareName", "status"},
		},
		{
			name: "accept share emits status only",
			request: func(t *testing.T, h *omics.Handler) *httptest.ResponseRecorder {
				t.Helper()

				id := createShare(t, h)

				return doRequest(t, h, http.MethodPost, "/share/"+id, nil)
			},
			wantKeys: []string{"status"},
		},
		{
			name: "delete share emits status only",
			request: func(t *testing.T, h *omics.Handler) *httptest.ResponseRecorder {
				t.Helper()

				id := createShare(t, h)

				return doRequest(t, h, http.MethodDelete, "/share/"+id, nil)
			},
			wantKeys: []string{"status"},
		},
		{
			name: "get share still emits the full object under share",
			request: func(t *testing.T, h *omics.Handler) *httptest.ResponseRecorder {
				t.Helper()

				id := createShare(t, h)

				return doRequest(t, h, http.MethodGet, "/share/"+id, nil)
			},
			wantKeys: []string{"share"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := tc.request(t, h)
			require.Less(t, rec.Code, http.StatusBadRequest)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			keys := make([]string, 0, len(resp))
			for k := range resp {
				keys = append(keys, k)
			}

			assert.ElementsMatch(t, tc.wantKeys, keys)
		})
	}
}

// TestOmics_GetShare_FullObjectFields is a regression guard: GetShareOutput
// legitimately carries the whole ShareDetails object (api_op_GetShare.go:39-49),
// unlike its three siblings, so it must keep emitting every share field.
func TestOmics_GetShare_FullObjectFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id := createShare(t, h)

	rec := doRequest(t, h, http.MethodGet, "/share/"+id, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	share, ok := resp["share"].(map[string]any)
	require.True(t, ok)

	assert.ElementsMatch(t, []string{
		"shareId", "resourceArn", "principalSubscriber", "shareName", "status", "creationTime",
	}, mapKeysExcept(share, "updateTime"))
}

// TestOmics_AcceptShare_ReachesActive is a real-SDK-client regression test
// for gopherstack-muzq: AcceptShare stamped Status ACTIVATING and nothing in
// this backend ever advanced it further, so a client polling GetShare for
// readiness never saw a terminal status. PENDING is correctly left alone --
// that wait is for AcceptShare itself, a client-driven call -- but ACTIVATING
// has no client op to advance it; GetShare must reap it to ACTIVE on read,
// mirroring GetWorkflow/GetAnnotationStore/GetVariantStore.
func TestOmics_AcceptShare_ReachesActive(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestOmicsClient(t, h)

	created, err := client.CreateShare(t.Context(), &omicssdk.CreateShareInput{
		ResourceArn:         aws.String("arn:aws:omics:us-east-1:000000000000:annotationStore/mystore"),
		PrincipalSubscriber: aws.String("123456789012"),
		ShareName:           aws.String("my-share"),
	})
	require.NoError(t, err)
	require.Equal(t, types.ShareStatusPending, created.Status)

	accepted, err := client.AcceptShare(t.Context(), &omicssdk.AcceptShareInput{
		ShareId: created.ShareId,
	})
	require.NoError(t, err)
	require.Equal(t, types.ShareStatusActivating, accepted.Status)

	got, err := client.GetShare(t.Context(), &omicssdk.GetShareInput{ShareId: created.ShareId})
	require.NoError(t, err)
	assert.Equal(t, types.ShareStatusActive, got.Share.Status, "GetShare must reap ACTIVATING to ACTIVE on poll")
}

// mapKeysExcept returns m's keys, dropping any in excl (used for keys that
// are only present sometimes, like an omitempty pointer field).
func mapKeysExcept(m map[string]any, excl ...string) []string {
	skip := make(map[string]bool, len(excl))
	for _, e := range excl {
		skip[e] = true
	}

	keys := make([]string, 0, len(m))

	for k := range m {
		if !skip[k] {
			keys = append(keys, k)
		}
	}

	return keys
}

// createSharePair creates one share on an annotation store resource and one
// on a variant store resource, returning their shareIds for the filter tests
// below (ListShares derives the real ShareResourceType from the ARN).
func createSharePair(t *testing.T, h *omics.Handler) map[string]string {
	t.Helper()

	resources := map[string]string{
		"annotation": "arn:aws:omics:us-east-1:000000000000:annotationStore/share-ann",
		"variant":    "arn:aws:omics:us-east-1:000000000000:variantStore/share-var",
	}
	ids := map[string]string{}

	for key, resourceARN := range resources {
		rec := doRequest(t, h, http.MethodPost, "/share", map[string]any{
			"resourceArn":         resourceARN,
			"principalSubscriber": "123456789012",
			"shareName":           key + "-share",
		})
		require.Equal(t, http.StatusCreated, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		ids[key], _ = resp["shareId"].(string)
	}

	return ids
}

func shareIDs(t *testing.T, raw any) []string {
	t.Helper()

	items, ok := raw.([]any)
	require.True(t, ok)

	ids := make([]string, 0, len(items))

	for _, item := range items {
		m, itemOK := item.(map[string]any)
		require.True(t, itemOK)
		ids = append(ids, m["shareId"].(string))
	}

	return ids
}

// TestListShares_Filters verifies ListShares applies its resourceArns/status/
// type body filter (real AWS ListSharesInput body "filter", types.Filter).
func TestListShares_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reqBody func(ids map[string]string) map[string]any
		name    string
		wantKey string
	}{
		{
			name: "type filter alone matches annotation store share",
			reqBody: func(map[string]string) map[string]any {
				return map[string]any{"filter": map[string]any{"type": []string{"ANNOTATION_STORE"}}}
			},
			wantKey: "annotation",
		},
		{
			name: "resourceArns filter alone matches variant store share",
			reqBody: func(map[string]string) map[string]any {
				return map[string]any{"filter": map[string]any{
					"resourceArns": []string{"arn:aws:omics:us-east-1:000000000000:variantStore/share-var"},
				}}
			},
			wantKey: "variant",
		},
		{
			name: "type and status combined match",
			reqBody: func(map[string]string) map[string]any {
				return map[string]any{"filter": map[string]any{
					"type":   []string{"ANNOTATION_STORE"},
					"status": []string{"PENDING"},
				}}
			},
			wantKey: "annotation",
		},
		{
			name: "type and status combined mismatch returns empty",
			reqBody: func(map[string]string) map[string]any {
				return map[string]any{"filter": map[string]any{
					"type":   []string{"ANNOTATION_STORE"},
					"status": []string{"ACTIVE"},
				}}
			},
			wantKey: "",
		},
		{
			name: "unmatched type returns empty",
			reqBody: func(map[string]string) map[string]any {
				return map[string]any{"filter": map[string]any{"type": []string{"WORKFLOW"}}}
			},
			wantKey: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			ids := createSharePair(t, h)

			rec := doRequest(t, h, http.MethodPost, "/shares", tc.reqBody(ids))
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			var want []string
			if tc.wantKey != "" {
				want = []string{ids[tc.wantKey]}
			}

			assert.ElementsMatch(t, want, shareIDs(t, resp["shares"]))
		})
	}
}
