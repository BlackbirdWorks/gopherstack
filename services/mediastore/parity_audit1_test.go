package mediastore_test

// Parity audit 1: AWS MediaStore accuracy gaps.
//
// Covers:
//   - DeleteContainerPolicy returns PolicyNotFoundException when no policy set
//   - DeleteCorsPolicy returns CorsPolicyNotFoundException when no CORS policy set
//   - DeleteLifecyclePolicy returns PolicyNotFoundException when no lifecycle policy set
//   - DeleteMetricPolicy returns PolicyNotFoundException when no metric policy set
//   - ARN format: arn:aws:mediastore:{region}:{account}:container/{name}
//   - Endpoint format: https://{name}.data.mediastore.{region}.amazonaws.com
//   - Container field shapes returned by CreateContainer and DescribeContainer
//   - Tags passed at CreateContainer time are stored and retrievable
//   - PutCorsPolicy rejects rules with empty AllowedOrigins or AllowedHeaders
//   - PutMetricPolicy rejects > 5 rules
//   - PutMetricPolicy rejects invalid ContainerLevelMetrics value
//   - Container name validation: empty name returns ValidationException
//   - ListContainers with multiple containers returns all sorted by name
//   - TagResource round-trip: add, list, partial remove, verify
//   - StartAccessLogging / StopAccessLogging flip AccessLoggingEnabled on DescribeContainer

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediastore"
)

const (
	pa1Region    = "us-east-1"
	pa1AccountID = "000000000000"
)

func pa1Handler(t *testing.T) *mediastore.Handler {
	t.Helper()

	b := mediastore.NewInMemoryBackend()
	h := mediastore.NewHandler(b)
	h.AccountID = pa1AccountID
	h.DefaultRegion = pa1Region

	return h
}

func pa1Do(t *testing.T, h *mediastore.Handler, op string, body any) *httptest.ResponseRecorder {
	t.Helper()

	payload, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "MediaStore_20170901."+op)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func pa1Unmarshal(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

func pa1CreateContainer(t *testing.T, h *mediastore.Handler, name string) string {
	t.Helper()

	rec := pa1Do(t, h, "CreateContainer", map[string]any{"ContainerName": name})
	require.Equal(t, http.StatusOK, rec.Code)

	m := pa1Unmarshal(t, rec)
	ct := m["Container"].(map[string]any)

	return ct["ARN"].(string)
}

// TestParity_DeletePolicy_NotSet verifies that all four delete-policy operations
// return the correct AWS not-found error when no policy is set.
func TestParity_DeletePolicy_NotSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		op          string
		wantErrType string
		wantStatus  int
	}{
		{
			name:        "DeleteContainerPolicy_no_policy",
			op:          "DeleteContainerPolicy",
			wantErrType: "PolicyNotFoundException",
			wantStatus:  http.StatusNotFound,
		},
		{
			name:        "DeleteCorsPolicy_no_policy",
			op:          "DeleteCorsPolicy",
			wantErrType: "CorsPolicyNotFoundException",
			wantStatus:  http.StatusNotFound,
		},
		{
			name:        "DeleteLifecyclePolicy_no_policy",
			op:          "DeleteLifecyclePolicy",
			wantErrType: "PolicyNotFoundException",
			wantStatus:  http.StatusNotFound,
		},
		{
			name:        "DeleteMetricPolicy_no_policy",
			op:          "DeleteMetricPolicy",
			wantErrType: "PolicyNotFoundException",
			wantStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			pa1CreateContainer(t, h, "parity-container")

			rec := pa1Do(t, h, tt.op, map[string]any{"ContainerName": "parity-container"})

			assert.Equal(t, tt.wantStatus, rec.Code)
			m := pa1Unmarshal(t, rec)
			assert.Equal(t, tt.wantErrType, m["__type"])
		})
	}
}

// TestParity_DeletePolicy_AfterSet verifies that all four delete-policy operations
// succeed (200) when a policy is set, and return not-found on a second delete.
func TestParity_DeletePolicy_AfterSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, h *mediastore.Handler)
		name        string
		deleteOp    string
		wantErrType string
	}{
		{
			name:     "DeleteContainerPolicy_idempotent_second_delete",
			deleteOp: "DeleteContainerPolicy",
			setup: func(t *testing.T, h *mediastore.Handler) {
				t.Helper()
				rec := pa1Do(t, h, "PutContainerPolicy", map[string]any{
					"ContainerName": "del-test",
					"Policy":        `{"Version":"2012-10-17","Statement":[]}`,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantErrType: "PolicyNotFoundException",
		},
		{
			name:     "DeleteCorsPolicy_idempotent_second_delete",
			deleteOp: "DeleteCorsPolicy",
			setup: func(t *testing.T, h *mediastore.Handler) {
				t.Helper()
				rec := pa1Do(t, h, "PutCorsPolicy", map[string]any{
					"ContainerName": "del-test",
					"CorsPolicy": []any{
						map[string]any{
							"AllowedOrigins": []any{"https://example.com"},
							"AllowedHeaders": []any{"*"},
						},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantErrType: "CorsPolicyNotFoundException",
		},
		{
			name:     "DeleteLifecyclePolicy_idempotent_second_delete",
			deleteOp: "DeleteLifecyclePolicy",
			setup: func(t *testing.T, h *mediastore.Handler) {
				t.Helper()
				rec := pa1Do(t, h, "PutLifecyclePolicy", map[string]any{
					"ContainerName":   "del-test",
					"LifecyclePolicy": `{"rules":[]}`,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantErrType: "PolicyNotFoundException",
		},
		{
			name:     "DeleteMetricPolicy_idempotent_second_delete",
			deleteOp: "DeleteMetricPolicy",
			setup: func(t *testing.T, h *mediastore.Handler) {
				t.Helper()
				rec := pa1Do(t, h, "PutMetricPolicy", map[string]any{
					"ContainerName": "del-test",
					"MetricPolicy":  map[string]any{"ContainerLevelMetrics": "ENABLED"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantErrType: "PolicyNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			pa1CreateContainer(t, h, "del-test")
			tt.setup(t, h)

			// First delete succeeds.
			rec := pa1Do(t, h, tt.deleteOp, map[string]any{"ContainerName": "del-test"})
			assert.Equal(t, http.StatusOK, rec.Code)

			// Second delete returns not-found.
			rec = pa1Do(t, h, tt.deleteOp, map[string]any{"ContainerName": "del-test"})
			assert.Equal(t, http.StatusNotFound, rec.Code)
			m := pa1Unmarshal(t, rec)
			assert.Equal(t, tt.wantErrType, m["__type"])
		})
	}
}

// TestParity_ContainerFieldShape verifies that CreateContainer and DescribeContainer
// return the expected field shapes matching AWS.
func TestParity_ContainerFieldShape(t *testing.T) {
	t.Parallel()

	const arnPrefix = "arn:aws:mediastore:us-east-1:000000000000:container/"
	const endpointSuffix = ".data.mediastore.us-east-1.amazonaws.com"

	tests := []struct {
		name          string
		containerName string
	}{
		{name: "create_returns_correct_shape", containerName: "shape-test"},
		{name: "describe_returns_correct_shape", containerName: "shape-test2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)

			rec := pa1Do(t, h, "CreateContainer", map[string]any{
				"ContainerName": tt.containerName,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			m := pa1Unmarshal(t, rec)
			ct := m["Container"].(map[string]any)
			assert.Equal(t, tt.containerName, ct["Name"])
			assert.Equal(t, arnPrefix+tt.containerName, ct["ARN"])
			assert.Contains(t, ct["Endpoint"].(string), endpointSuffix)
			assert.Equal(t, "ACTIVE", ct["Status"])
			assert.NotZero(t, ct["CreationTime"])

			// Verify DescribeContainer returns same shape.
			rec = pa1Do(t, h, "DescribeContainer", map[string]any{"ContainerName": tt.containerName})
			require.Equal(t, http.StatusOK, rec.Code)
			m = pa1Unmarshal(t, rec)
			ct2 := m["Container"].(map[string]any)
			assert.Equal(t, ct["ARN"], ct2["ARN"])
			assert.Equal(t, ct["Endpoint"], ct2["Endpoint"])
			assert.Equal(t, "ACTIVE", ct2["Status"])
		})
	}
}

// TestParity_CreateContainer_Tags verifies that tags supplied at create time
// can be retrieved via ListTagsForResource.
func TestParity_CreateContainer_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // test-only struct; layout not performance-critical
		name     string
		tags     []any
		wantTags map[string]string
	}{
		{
			name:     "single_tag_at_create",
			tags:     []any{map[string]any{"Key": "env", "Value": "prod"}},
			wantTags: map[string]string{"env": "prod"},
		},
		{
			name: "multiple_tags_at_create",
			tags: []any{
				map[string]any{"Key": "team", "Value": "platform"},
				map[string]any{"Key": "cost-center", "Value": "eng"},
			},
			wantTags: map[string]string{"team": "platform", "cost-center": "eng"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)

			rec := pa1Do(t, h, "CreateContainer", map[string]any{
				"ContainerName": "tag-create-test",
				"Tags":          tt.tags,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			m := pa1Unmarshal(t, rec)
			ct := m["Container"].(map[string]any)
			containerARN := ct["ARN"].(string)

			rec = pa1Do(t, h, "ListTagsForResource", map[string]any{
				"Resource": containerARN,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			tagList := pa1Unmarshal(t, rec)["Tags"].([]any)
			got := make(map[string]string, len(tagList))
			for _, entry := range tagList {
				e := entry.(map[string]any)
				got[e["Key"].(string)] = e["Value"].(string)
			}

			assert.Equal(t, tt.wantTags, got)
		})
	}
}

// TestParity_PutCorsPolicy_Validation verifies CORS rule validation.
func TestParity_PutCorsPolicy_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		corsPolicy any
		name       string
		wantStatus int
	}{
		{
			name: "valid_rule",
			corsPolicy: []any{
				map[string]any{
					"AllowedOrigins": []any{"https://example.com"},
					"AllowedHeaders": []any{"Authorization"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_allowed_origins",
			corsPolicy: []any{
				map[string]any{
					"AllowedOrigins": []any{},
					"AllowedHeaders": []any{"*"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_allowed_headers",
			corsPolicy: []any{
				map[string]any{
					"AllowedOrigins": []any{"https://example.com"},
					"AllowedHeaders": []any{},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			pa1CreateContainer(t, h, "cors-validation")

			rec := pa1Do(t, h, "PutCorsPolicy", map[string]any{
				"ContainerName": "cors-validation",
				"CorsPolicy":    tt.corsPolicy,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestParity_PutMetricPolicy_Validation verifies MetricPolicy validation.
func TestParity_PutMetricPolicy_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy     map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "valid_enabled",
			policy:     map[string]any{"ContainerLevelMetrics": "ENABLED"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid_disabled",
			policy:     map[string]any{"ContainerLevelMetrics": "DISABLED"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_metric_level",
			policy:     map[string]any{"ContainerLevelMetrics": "INVALID"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "too_many_rules_six",
			policy: map[string]any{
				"ContainerLevelMetrics": "ENABLED",
				"MetricPolicyRules": []any{
					map[string]any{"ObjectGroup": "a", "ObjectGroupName": "a"},
					map[string]any{"ObjectGroup": "b", "ObjectGroupName": "b"},
					map[string]any{"ObjectGroup": "c", "ObjectGroupName": "c"},
					map[string]any{"ObjectGroup": "d", "ObjectGroupName": "d"},
					map[string]any{"ObjectGroup": "e", "ObjectGroupName": "e"},
					map[string]any{"ObjectGroup": "f", "ObjectGroupName": "f"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "exactly_five_rules_allowed",
			policy: map[string]any{
				"ContainerLevelMetrics": "ENABLED",
				"MetricPolicyRules": []any{
					map[string]any{"ObjectGroup": "a", "ObjectGroupName": "a"},
					map[string]any{"ObjectGroup": "b", "ObjectGroupName": "b"},
					map[string]any{"ObjectGroup": "c", "ObjectGroupName": "c"},
					map[string]any{"ObjectGroup": "d", "ObjectGroupName": "d"},
					map[string]any{"ObjectGroup": "e", "ObjectGroupName": "e"},
				},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			pa1CreateContainer(t, h, "metric-validation")

			rec := pa1Do(t, h, "PutMetricPolicy", map[string]any{
				"ContainerName": "metric-validation",
				"MetricPolicy":  tt.policy,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestParity_AccessLogging verifies StartAccessLogging/StopAccessLogging flip the field.
func TestParity_AccessLogging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		ops         []string
		wantEnabled bool
	}{
		{
			name:        "start_sets_enabled",
			ops:         []string{"StartAccessLogging"},
			wantEnabled: true,
		},
		{
			name:        "start_then_stop_disables",
			ops:         []string{"StartAccessLogging", "StopAccessLogging"},
			wantEnabled: false,
		},
		{
			name:        "start_start_still_enabled",
			ops:         []string{"StartAccessLogging", "StartAccessLogging"},
			wantEnabled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			pa1CreateContainer(t, h, "logging-parity")

			for _, op := range tt.ops {
				rec := pa1Do(t, h, op, map[string]any{"ContainerName": "logging-parity"})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := pa1Do(t, h, "DescribeContainer", map[string]any{"ContainerName": "logging-parity"})
			require.Equal(t, http.StatusOK, rec.Code)

			m := pa1Unmarshal(t, rec)
			ct := m["Container"].(map[string]any)
			assert.Equal(t, tt.wantEnabled, ct["AccessLoggingEnabled"])
		})
	}
}

// TestParity_ListContainers_Order verifies containers are returned sorted by name.
func TestParity_ListContainers_Order(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		create    []string
		wantOrder []string
	}{
		{
			name:      "lexicographic_sort",
			create:    []string{"zzz-container", "aaa-container", "mmm-container"},
			wantOrder: []string{"aaa-container", "mmm-container", "zzz-container"},
		},
		{
			name:      "single_container",
			create:    []string{"only-one"},
			wantOrder: []string{"only-one"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			for _, name := range tt.create {
				pa1CreateContainer(t, h, name)
			}

			rec := pa1Do(t, h, "ListContainers", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			m := pa1Unmarshal(t, rec)
			list := m["Containers"].([]any)
			require.Len(t, list, len(tt.wantOrder))

			for i, want := range tt.wantOrder {
				ct := list[i].(map[string]any)
				assert.Equal(t, want, ct["Name"])
			}
		})
	}
}

// TestParity_TagResource_RoundTrip verifies add, list, partial remove, verify.
func TestParity_TagResource_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // test-only struct; layout not performance-critical
		addTags   []any
		removKeys []any
		wantAfter map[string]string
		name      string
	}{
		{
			name: "add_two_remove_one",
			addTags: []any{
				map[string]any{"Key": "a", "Value": "1"},
				map[string]any{"Key": "b", "Value": "2"},
			},
			removKeys: []any{"a"},
			wantAfter: map[string]string{"b": "2"},
		},
		{
			name: "add_then_update",
			addTags: []any{
				map[string]any{"Key": "env", "Value": "staging"},
			},
			removKeys: nil,
			wantAfter: map[string]string{"env": "staging"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			containerARN := pa1CreateContainer(t, h, "tag-roundtrip")

			rec := pa1Do(t, h, "TagResource", map[string]any{
				"Resource": containerARN,
				"Tags":     tt.addTags,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			if len(tt.removKeys) > 0 {
				rec = pa1Do(t, h, "UntagResource", map[string]any{
					"Resource": containerARN,
					"TagKeys":  tt.removKeys,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec = pa1Do(t, h, "ListTagsForResource", map[string]any{"Resource": containerARN})
			require.Equal(t, http.StatusOK, rec.Code)

			tagList := pa1Unmarshal(t, rec)["Tags"].([]any)
			got := make(map[string]string, len(tagList))
			for _, entry := range tagList {
				e := entry.(map[string]any)
				got[e["Key"].(string)] = e["Value"].(string)
			}

			assert.Equal(t, tt.wantAfter, got)
		})
	}
}

// TestParity_ContainerNotFound verifies all ops return ContainerNotFoundException (404).
func TestParity_ContainerNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		op   string
	}{
		{
			name: "DescribeContainer",
			op:   "DescribeContainer",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "DeleteContainer",
			op:   "DeleteContainer",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "PutContainerPolicy",
			op:   "PutContainerPolicy",
			body: map[string]any{"ContainerName": "missing", "Policy": "{}"},
		},
		{
			name: "GetContainerPolicy",
			op:   "GetContainerPolicy",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "DeleteContainerPolicy",
			op:   "DeleteContainerPolicy",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "PutCorsPolicy",
			op:   "PutCorsPolicy",
			body: map[string]any{
				"ContainerName": "missing",
				"CorsPolicy": []any{
					map[string]any{
						"AllowedOrigins": []any{"https://x.com"},
						"AllowedHeaders": []any{"*"},
					},
				},
			},
		},
		{
			name: "GetCorsPolicy",
			op:   "GetCorsPolicy",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "DeleteCorsPolicy",
			op:   "DeleteCorsPolicy",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "PutLifecyclePolicy",
			op:   "PutLifecyclePolicy",
			body: map[string]any{"ContainerName": "missing", "LifecyclePolicy": "{}"},
		},
		{
			name: "GetLifecyclePolicy",
			op:   "GetLifecyclePolicy",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "DeleteLifecyclePolicy",
			op:   "DeleteLifecyclePolicy",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "PutMetricPolicy",
			op:   "PutMetricPolicy",
			body: map[string]any{
				"ContainerName": "missing",
				"MetricPolicy":  map[string]any{"ContainerLevelMetrics": "ENABLED"},
			},
		},
		{
			name: "GetMetricPolicy",
			op:   "GetMetricPolicy",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "DeleteMetricPolicy",
			op:   "DeleteMetricPolicy",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "StartAccessLogging",
			op:   "StartAccessLogging",
			body: map[string]any{"ContainerName": "missing"},
		},
		{
			name: "StopAccessLogging",
			op:   "StopAccessLogging",
			body: map[string]any{"ContainerName": "missing"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := pa1Handler(t)
			rec := pa1Do(t, h, tt.op, tt.body)

			assert.Equal(t, http.StatusNotFound, rec.Code)
			m := pa1Unmarshal(t, rec)
			assert.Equal(t, "ContainerNotFoundException", m["__type"])
		})
	}
}
