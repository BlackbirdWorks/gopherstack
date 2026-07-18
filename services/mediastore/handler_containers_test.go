package mediastore_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateContainer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantField  string
		wantStatus int
	}{
		{
			name:       "creates container",
			body:       map[string]any{"ContainerName": "my-container"},
			wantStatus: http.StatusOK,
			wantField:  "Container",
		},
		{
			name:       "missing container name",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "duplicate returns conflict",
			body:       map[string]any{"ContainerName": "dup-container"},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantStatus == http.StatusConflict {
				rec := doRequest(t, h, "CreateContainer", tt.body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "CreateContainer", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField != "" {
				resp := unmarshalBody(t, rec)
				assert.Contains(t, resp, tt.wantField)
			}
		})
	}
}

func TestHandler_DeleteContainer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		container  string
		wantStatus int
	}{
		{
			name:       "deletes existing container",
			container:  "to-delete",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing container returns not found",
			container:  "missing",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing container name returns bad request",
			container:  "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantStatus == http.StatusOK {
				rec := doRequest(t, h, "CreateContainer", map[string]any{"ContainerName": tt.container})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "DeleteContainer", map[string]any{"ContainerName": tt.container})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DescribeContainer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		container  string
		wantStatus int
	}{
		{
			name:       "describes existing container",
			container:  "describe-me",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			container:  "missing",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantStatus == http.StatusOK {
				rec := doRequest(t, h, "CreateContainer", map[string]any{"ContainerName": tt.container})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "DescribeContainer", map[string]any{"ContainerName": tt.container})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := unmarshalBody(t, rec)
				assert.Contains(t, resp, "Container")
			}
		})
	}
}

func TestHandler_ListContainers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		createN   int
		wantCount int
	}{
		{
			name:      "empty list",
			createN:   0,
			wantCount: 0,
		},
		{
			name:      "lists created containers",
			createN:   2,
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tt.createN {
				rec := doRequest(t, h, "CreateContainer",
					map[string]any{"ContainerName": fmt.Sprintf("container-%d", i)})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "ListContainers", map[string]any{})

			require.Equal(t, http.StatusOK, rec.Code)

			resp := unmarshalBody(t, rec)
			containers, _ := resp["Containers"].([]any)
			assert.Len(t, containers, tt.wantCount)
		})
	}
}

// TestHandler_ContainerFieldShape verifies that CreateContainer and DescribeContainer
// return the expected field shapes matching AWS. Moved (and de-prefixed) from the
// former parity_audit1_test.go's TestParity_ContainerFieldShape.
func TestHandler_ContainerFieldShape(t *testing.T) {
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

			h := newTestHandler(t)

			rec := doRequest(t, h, "CreateContainer", map[string]any{
				"ContainerName": tt.containerName,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			m := unmarshalBody(t, rec)
			ct := m["Container"].(map[string]any)
			assert.Equal(t, tt.containerName, ct["Name"])
			assert.Equal(t, arnPrefix+tt.containerName, ct["ARN"])
			assert.Contains(t, ct["Endpoint"].(string), endpointSuffix)
			assert.Equal(t, "ACTIVE", ct["Status"])
			assert.NotZero(t, ct["CreationTime"])

			// Verify DescribeContainer returns same shape.
			rec = doRequest(t, h, "DescribeContainer", map[string]any{"ContainerName": tt.containerName})
			require.Equal(t, http.StatusOK, rec.Code)
			m = unmarshalBody(t, rec)
			ct2 := m["Container"].(map[string]any)
			assert.Equal(t, ct["ARN"], ct2["ARN"])
			assert.Equal(t, ct["Endpoint"], ct2["Endpoint"])
			assert.Equal(t, "ACTIVE", ct2["Status"])
		})
	}
}

// TestHandler_CreateContainer_Tags verifies that tags supplied at create time
// can be retrieved via ListTagsForResource. Moved (and de-prefixed) from the
// former parity_audit1_test.go's TestParity_CreateContainer_Tags.
func TestHandler_CreateContainer_Tags(t *testing.T) {
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

			h := newTestHandler(t)

			rec := doRequest(t, h, "CreateContainer", map[string]any{
				"ContainerName": "tag-create-test",
				"Tags":          tt.tags,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			m := unmarshalBody(t, rec)
			ct := m["Container"].(map[string]any)
			containerARN := ct["ARN"].(string)

			rec = doRequest(t, h, "ListTagsForResource", map[string]any{
				"Resource": containerARN,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			tagList := unmarshalBody(t, rec)["Tags"].([]any)
			got := make(map[string]string, len(tagList))
			for _, entry := range tagList {
				e := entry.(map[string]any)
				got[e["Key"].(string)] = e["Value"].(string)
			}

			assert.Equal(t, tt.wantTags, got)
		})
	}
}

// TestHandler_ListContainers_Order verifies containers are returned sorted by
// name. Moved (and de-prefixed) from the former parity_audit1_test.go's
// TestParity_ListContainers_Order.
func TestHandler_ListContainers_Order(t *testing.T) {
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

			h := newTestHandler(t)
			for _, name := range tt.create {
				createTestContainer(t, h, name)
			}

			rec := doRequest(t, h, "ListContainers", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			m := unmarshalBody(t, rec)
			list := m["Containers"].([]any)
			require.Len(t, list, len(tt.wantOrder))

			for i, want := range tt.wantOrder {
				ct := list[i].(map[string]any)
				assert.Equal(t, want, ct["Name"])
			}
		})
	}
}

func TestHandler_ContainerPolicy(t *testing.T) {
	t.Parallel()

	const policy = `{"Version":"2012-10-17","Statement":[]}`

	tests := []struct {
		name       string
		op         string
		wantStatus int
		withPolicy bool
		deleted    bool
	}{
		{
			name:       "put container policy",
			op:         "PutContainerPolicy",
			wantStatus: http.StatusOK,
		},
		{
			name:       "get container policy",
			op:         "GetContainerPolicy",
			withPolicy: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete container policy",
			op:         "DeleteContainerPolicy",
			withPolicy: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "get policy after delete returns not found",
			op:         "GetContainerPolicy",
			withPolicy: true,
			deleted:    true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupRec := doRequest(t, h, "CreateContainer", map[string]any{"ContainerName": "policy-test"})
			require.Equal(t, http.StatusOK, setupRec.Code)

			if tt.withPolicy {
				putRec := doRequest(t, h, "PutContainerPolicy",
					map[string]any{"ContainerName": "policy-test", "Policy": policy})
				require.Equal(t, http.StatusOK, putRec.Code)
			}

			if tt.deleted {
				delRec := doRequest(t, h, "DeleteContainerPolicy",
					map[string]any{"ContainerName": "policy-test"})
				require.Equal(t, http.StatusOK, delRec.Code)
			}

			body := map[string]any{"ContainerName": "policy-test"}
			if tt.op == "PutContainerPolicy" {
				body["Policy"] = policy
			}

			result := doRequest(t, h, tt.op, body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestHandler_AccessLogging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "start access logging",
			op:         "StartAccessLogging",
			wantStatus: http.StatusOK,
		},
		{
			name:       "stop access logging",
			op:         "StopAccessLogging",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupRec := doRequest(t, h, "CreateContainer", map[string]any{"ContainerName": "logging-test"})
			require.Equal(t, http.StatusOK, setupRec.Code)

			result := doRequest(t, h, tt.op, map[string]any{"ContainerName": "logging-test"})
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

// TestHandler_AccessLoggingSequence verifies StartAccessLogging/StopAccessLogging
// flip the AccessLoggingEnabled field across a sequence of calls. Moved (and
// de-prefixed) from the former parity_audit1_test.go's TestParity_AccessLogging
// (renamed to avoid colliding with TestHandler_AccessLogging above).
func TestHandler_AccessLoggingSequence(t *testing.T) {
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

			h := newTestHandler(t)
			createTestContainer(t, h, "logging-parity")

			for _, op := range tt.ops {
				rec := doRequest(t, h, op, map[string]any{"ContainerName": "logging-parity"})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "DescribeContainer", map[string]any{"ContainerName": "logging-parity"})
			require.Equal(t, http.StatusOK, rec.Code)

			m := unmarshalBody(t, rec)
			ct := m["Container"].(map[string]any)
			assert.Equal(t, tt.wantEnabled, ct["AccessLoggingEnabled"])
		})
	}
}
