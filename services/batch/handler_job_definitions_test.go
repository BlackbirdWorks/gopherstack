package batch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_JobDefinition_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		jdName     string
		wantStatus int
		wantRev    int32
	}{
		{name: "register_success", jdName: "test-jd", wantStatus: http.StatusOK, wantRev: 1},
		{name: "register_second_revision", jdName: "test-jd-rev", wantStatus: http.StatusOK, wantRev: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
				"jobDefinitionName": tt.jdName,
				"type":              "container",
			})

			require.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)
			assert.Contains(t, out["jobDefinitionArn"].(string), tt.jdName)
			assert.Equal(t, tt.jdName, out["jobDefinitionName"])
			assert.InEpsilon(t, float64(tt.wantRev), out["revision"].(float64), 0.001)
		})
	}
}

func TestHandler_RegisterJobDefinition_MultipleRevisions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
			"jobDefinitionName": "my-jd",
			"type":              "container",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		mustUnmarshal(t, rec, &out)
		assert.InEpsilon(t, float64(i+1), out["revision"].(float64), 0.001)
	}
}

func TestHandler_DescribeJobDefinitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filter     []string
		wantMin    int
		wantStatus int
	}{
		{name: "describe_all", filter: nil, wantMin: 2, wantStatus: http.StatusOK},
		{name: "describe_by_name", filter: []string{"jd-1"}, wantMin: 1, wantStatus: http.StatusOK},
		{name: "describe_missing", filter: []string{"nope"}, wantMin: 0, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, name := range []string{"jd-1", "jd-2"} {
				rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
					"jobDefinitionName": name,
					"type":              "container",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			body := map[string]any{}
			if tt.filter != nil {
				body["jobDefinitions"] = tt.filter
			}

			rec := post(t, h, "/v1/describejobdefinitions", body)

			require.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)

			list, ok := out["jobDefinitions"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(list), tt.wantMin)
		})
	}
}

func TestHandler_DeregisterJobDefinition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useARN     bool
		wantStatus int
	}{
		{name: "deregister_success", useARN: true, wantStatus: http.StatusOK},
		{name: "deregister_not_found", useARN: false, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
				"jobDefinitionName": "deregtest-jd",
				"type":              "container",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)
			jdARN := out["jobDefinitionArn"].(string)

			jd := jdARN
			if !tt.useARN {
				jd = "arn:aws:batch:us-east-1:000000000000:job-definition/nonexistent:1"
			}

			rec = post(t, h, "/v1/deregisterjobdefinition", map[string]any{
				"jobDefinition": jd,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Tags tests ---

func TestHandler_DeregisterJobDefinition_ByNameRevision(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
		"jobDefinitionName": "namerev-jd",
		"type":              "container",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Deregister by name:revision.
	rec = post(t, h, "/v1/deregisterjobdefinition", map[string]any{
		"jobDefinition": "namerev-jd:1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestDescribeJobDefinitions_TagsPresentNoTags verifies that
// DescribeJobDefinitions always includes "tags": {} when a job definition
// has no tags. AWS always returns tags:{} in this response.
func TestDescribeJobDefinitions_TagsPresentNoTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
		"jobDefinitionName": "jd-notags",
		"type":              "container",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/describejobdefinitions", map[string]any{
		"jobDefinitions": []string{"jd-notags"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	items := out["jobDefinitions"].([]any)
	require.Len(t, items, 1)

	itemBytes, err := json.Marshal(items[0])
	require.NoError(t, err)
	assertTagsPresent(t, itemBytes)
}

// TestDescribeJobDefinitions_EmptyList verifies that
// DescribeJobDefinitions returns "jobDefinitions": [] not null.
func TestDescribeJobDefinitions_EmptyList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := post(t, h, "/v1/describejobdefinitions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var rawMap map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rawMap))
	raw, ok := rawMap["jobDefinitions"]
	require.True(t, ok, "jobDefinitions key must be present")
	assert.Equal(t, "[]", string(raw), "jobDefinitions must be [] not null when empty")
}

// TestHandler_RegisterJobDefinition_RetryStrategy verifies that
// RegisterJobDefinition accepts and stores a job-definition-level
// RetryStrategy, and that it round-trips through DescribeJobDefinitions.
// Real AWS Batch supports a default RetryStrategy at the job-definition
// level in addition to the job-level RetryStrategy on SubmitJob.
func TestHandler_RegisterJobDefinition_RetryStrategy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
		"jobDefinitionName": "jd-retry",
		"type":              "container",
		"retryStrategy": map[string]any{
			"attempts": 3,
			"evaluateOnExit": []map[string]any{
				{"action": "RETRY", "onExitCode": "1"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/describejobdefinitions", map[string]any{
		"jobDefinitions": []string{"jd-retry"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	items := out["jobDefinitions"].([]any)
	require.Len(t, items, 1)

	jd := items[0].(map[string]any)
	rs, ok := jd["retryStrategy"].(map[string]any)
	require.True(t, ok, "retryStrategy should be present")
	assert.InEpsilon(t, float64(3), rs["attempts"].(float64), 0.001)

	exitRules := rs["evaluateOnExit"].([]any)
	require.Len(t, exitRules, 1)
	assert.Equal(t, "RETRY", exitRules[0].(map[string]any)["action"])
}

// TestHandler_RegisterJobDefinition_EksProperties verifies that
// RegisterJobDefinition wires the eksProperties parameter through to the
// backend and it round-trips via DescribeJobDefinitions. Previously this
// parameter was hardcoded to nil in the handler regardless of what the
// caller sent (see PARITY.md gaps).
func TestHandler_RegisterJobDefinition_EksProperties(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
		"jobDefinitionName": "jd-eks",
		"type":              "container",
		"eksProperties": map[string]any{
			"podProperties": map[string]any{
				"containers": []map[string]any{
					{"image": "busybox", "name": "main"},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = post(t, h, "/v1/describejobdefinitions", map[string]any{
		"jobDefinitions": []string{"jd-eks"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	items := out["jobDefinitions"].([]any)
	require.Len(t, items, 1)

	jd := items[0].(map[string]any)
	eksProps, ok := jd["eksProperties"].(map[string]any)
	require.True(t, ok, "eksProperties should be present, not silently dropped")

	podProps := eksProps["podProperties"].(map[string]any)
	containers := podProps["containers"].([]any)
	require.Len(t, containers, 1)
	assert.Equal(t, "busybox", containers[0].(map[string]any)["image"])
}

func TestHandler_RegisterJobDefinition_EksProperties_ImagePullFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		imagePullPolicy string
		secretName      string
	}{
		{
			name:            "with_image_pull_policy_and_secrets",
			imagePullPolicy: "Always",
			secretName:      "my-reg-secret",
		},
		{
			name:            "with_if_not_present_policy",
			imagePullPolicy: "IfNotPresent",
			secretName:      "registry-cred",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			jdName := "jd-" + tt.name

			rec := post(t, h, "/v1/registerjobdefinition", map[string]any{
				"jobDefinitionName": jdName,
				"type":              "container",
				"eksProperties": map[string]any{
					"podProperties": map[string]any{
						"containers": []map[string]any{
							{
								"name":            "main",
								"image":           "alpine:latest",
								"imagePullPolicy": tt.imagePullPolicy,
							},
						},
						"imagePullSecrets": []map[string]any{
							{
								"name": tt.secretName,
							},
						},
					},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = post(t, h, "/v1/describejobdefinitions", map[string]any{
				"jobDefinitions": []string{jdName},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)
			items := out["jobDefinitions"].([]any)
			require.Len(t, items, 1)

			jd := items[0].(map[string]any)
			eksProps, ok := jd["eksProperties"].(map[string]any)
			require.True(t, ok)

			podProps := eksProps["podProperties"].(map[string]any)
			containers := podProps["containers"].([]any)
			require.Len(t, containers, 1)
			container := containers[0].(map[string]any)
			assert.Equal(t, tt.imagePullPolicy, container["imagePullPolicy"])

			secrets := podProps["imagePullSecrets"].([]any)
			require.Len(t, secrets, 1)
			secret := secrets[0].(map[string]any)
			assert.Equal(t, tt.secretName, secret["name"])
		})
	}
}
