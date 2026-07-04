package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// registerDaemonTaskDef is a test helper that registers a daemon task
// definition and returns its ARN.
func registerDaemonTaskDef(t *testing.T, h *ecs.Handler, family string) string {
	t.Helper()

	rec := doECSRequest(t, h, "RegisterDaemonTaskDefinition", map[string]any{
		"family": family,
		"containerDefinitions": []map[string]any{
			{"name": "agent", "image": "example/agent:latest", "essential": true},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	arn, _ := resp["daemonTaskDefinitionArn"].(string)
	require.NotEmpty(t, arn)

	return arn
}

// createCapacityProviderForDaemon registers a capacity provider and returns its ARN.
func createCapacityProviderForDaemon(t *testing.T, h *ecs.Handler, name string) string {
	t.Helper()

	rec := doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": name})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	cp, ok := resp["capacityProvider"].(map[string]any)
	require.True(t, ok)

	arn, _ := cp["capacityProviderArn"].(string)
	require.NotEmpty(t, arn)

	return arn
}

// ----- RegisterDaemonTaskDefinition / DescribeDaemonTaskDefinition -----
// ----- ListDaemonTaskDefinitions / DeleteDaemonTaskDefinition -----

func TestECS_RegisterDaemonTaskDefinition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{
			name: "registers daemon task definition",
			input: map[string]any{
				"family": "agent-family",
				"containerDefinitions": []map[string]any{
					{"name": "agent", "image": "example/agent:latest", "essential": true},
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing family returns 400",
			input:    map[string]any{"containerDefinitions": []map[string]any{{"name": "agent", "image": "x"}}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing container definitions returns 400",
			input:    map[string]any{"family": "agent-family"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, "RegisterDaemonTaskDefinition", tt.input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			arn, _ := resp["daemonTaskDefinitionArn"].(string)
			assert.Contains(t, arn, "daemon-task-definition/agent-family:1")
		})
	}
}

func TestECS_RegisterDaemonTaskDefinition_RevisionIncrements(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	arn1 := registerDaemonTaskDef(t, h, "incrementing")
	arn2 := registerDaemonTaskDef(t, h, "incrementing")

	assert.Contains(t, arn1, ":1")
	assert.Contains(t, arn2, ":2")
	assert.NotEqual(t, arn1, arn2)
}

func TestECS_DescribeDaemonTaskDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	arn := registerDaemonTaskDef(t, h, "describe-me")

	tests := []struct {
		name       string
		daemonTask string
		wantCode   int
	}{
		{name: "by arn", daemonTask: arn, wantCode: http.StatusOK},
		{name: "by family", daemonTask: "describe-me", wantCode: http.StatusOK},
		{name: "by family:revision", daemonTask: "describe-me:1", wantCode: http.StatusOK},
		{name: "unknown family returns 400", daemonTask: "does-not-exist", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doECSRequest(t, h, "DescribeDaemonTaskDefinition", map[string]any{
				"daemonTaskDefinition": tt.daemonTask,
			})
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			td, ok := resp["daemonTaskDefinition"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "describe-me", td["family"])
			assert.InDelta(t, float64(1), td["revision"], 0)
			assert.Equal(t, "ACTIVE", td["status"])
		})
	}
}

func TestECS_DeleteDaemonTaskDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	arn := registerDaemonTaskDef(t, h, "delete-me")

	rec := doECSRequest(t, h, "DeleteDaemonTaskDefinition", map[string]any{"daemonTaskDefinition": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, arn, resp["daemonTaskDefinitionArn"])

	// Deleted revisions are excluded from the default ACTIVE-only describe lookup by family.
	rec = doECSRequest(t, h, "DescribeDaemonTaskDefinition", map[string]any{"daemonTaskDefinition": "delete-me"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// But the ARN itself still resolves, now showing DELETED status (matches
	// real AWS behavior where deleted revisions remain describable by ARN).
	rec = doECSRequest(t, h, "DescribeDaemonTaskDefinition", map[string]any{"daemonTaskDefinition": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	td, ok := resp["daemonTaskDefinition"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "DELETED", td["status"])

	rec = doECSRequest(t, h, "DeleteDaemonTaskDefinition", map[string]any{"daemonTaskDefinition": "nonexistent-family"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestECS_ListDaemonTaskDefinitions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	registerDaemonTaskDef(t, h, "list-family-a")
	registerDaemonTaskDef(t, h, "list-family-b")

	rec := doECSRequest(t, h, "ListDaemonTaskDefinitions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tds, ok := resp["daemonTaskDefinitions"].([]any)
	require.True(t, ok)
	assert.Len(t, tds, 2)

	rec = doECSRequest(t, h, "ListDaemonTaskDefinitions", map[string]any{"familyPrefix": "list-family-a"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	tds, ok = resp["daemonTaskDefinitions"].([]any)
	require.True(t, ok)
	assert.Len(t, tds, 1)
}

// ----- CreateDaemon / DescribeDaemon / UpdateDaemon / DeleteDaemon / ListDaemons -----

func TestECS_CreateDaemon(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerDaemonTaskDef(t, h, "create-daemon-family")
	cpArn := createCapacityProviderForDaemon(t, h, "create-daemon-cp")

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{
			name: "creates daemon",
			input: map[string]any{
				"daemonName":              "my-daemon",
				"daemonTaskDefinitionArn": tdArn,
				"capacityProviderArns":    []string{cpArn},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "missing daemonName returns 400",
			input: map[string]any{
				"daemonTaskDefinitionArn": tdArn,
				"capacityProviderArns":    []string{cpArn},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing capacityProviderArns returns 400",
			input: map[string]any{
				"daemonName":              "no-cp-daemon",
				"daemonTaskDefinitionArn": tdArn,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "unknown daemon task definition returns 400",
			input: map[string]any{
				"daemonName":              "bad-td-daemon",
				"daemonTaskDefinitionArn": "arn:aws:ecs:us-east-1:000000000000:daemon-task-definition/nope:1",
				"capacityProviderArns":    []string{cpArn},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doECSRequest(t, h, "CreateDaemon", tt.input)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "ACTIVE", resp["status"])
			assert.NotEmpty(t, resp["daemonArn"])
			assert.NotEmpty(t, resp["deploymentArn"])
		})
	}
}

func TestECS_CreateDaemon_DuplicateNameConflicts(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerDaemonTaskDef(t, h, "dup-family")
	cpArn := createCapacityProviderForDaemon(t, h, "dup-cp")

	input := map[string]any{
		"daemonName":              "dup-daemon",
		"daemonTaskDefinitionArn": tdArn,
		"capacityProviderArns":    []string{cpArn},
	}

	rec := doECSRequest(t, h, "CreateDaemon", input)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doECSRequest(t, h, "CreateDaemon", input)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestECS_DescribeDaemon(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerDaemonTaskDef(t, h, "describe-daemon-family")
	cpArn := createCapacityProviderForDaemon(t, h, "describe-daemon-cp")

	rec := doECSRequest(t, h, "CreateDaemon", map[string]any{
		"daemonName":              "describe-target",
		"daemonTaskDefinitionArn": tdArn,
		"capacityProviderArns":    []string{cpArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	daemonArn, _ := created["daemonArn"].(string)
	require.NotEmpty(t, daemonArn)

	rec = doECSRequest(t, h, "DescribeDaemon", map[string]any{"daemonArn": daemonArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	d, ok := resp["daemon"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, daemonArn, d["daemonArn"])
	assert.Equal(t, "ACTIVE", d["status"])
	assert.NotEmpty(t, d["clusterArn"])
	revs, ok := d["currentRevisions"].([]any)
	require.True(t, ok)
	assert.Len(t, revs, 1)

	rec = doECSRequest(
		t, h, "DescribeDaemon",
		map[string]any{"daemonArn": "arn:aws:ecs:us-east-1:000000000000:daemon/default/nope"},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestECS_UpdateDaemon(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerDaemonTaskDef(t, h, "update-daemon-family")
	td2Arn := registerDaemonTaskDef(t, h, "update-daemon-family-v2")
	cpArn := createCapacityProviderForDaemon(t, h, "update-daemon-cp")

	rec := doECSRequest(t, h, "CreateDaemon", map[string]any{
		"daemonName":              "update-target",
		"daemonTaskDefinitionArn": tdArn,
		"capacityProviderArns":    []string{cpArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	daemonArn, _ := created["daemonArn"].(string)
	firstDeploymentArn, _ := created["deploymentArn"].(string)

	rec = doECSRequest(t, h, "UpdateDaemon", map[string]any{
		"daemonArn":               daemonArn,
		"daemonTaskDefinitionArn": td2Arn,
		"capacityProviderArns":    []string{cpArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ACTIVE", resp["status"])
	assert.NotEqual(t, firstDeploymentArn, resp["deploymentArn"])

	rec = doECSRequest(t, h, "UpdateDaemon", map[string]any{
		"daemonArn":               "arn:aws:ecs:us-east-1:000000000000:daemon/default/nope",
		"daemonTaskDefinitionArn": td2Arn,
		"capacityProviderArns":    []string{cpArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestECS_DeleteDaemon(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerDaemonTaskDef(t, h, "delete-daemon-family")
	cpArn := createCapacityProviderForDaemon(t, h, "delete-daemon-cp")

	rec := doECSRequest(t, h, "CreateDaemon", map[string]any{
		"daemonName":              "delete-target",
		"daemonTaskDefinitionArn": tdArn,
		"capacityProviderArns":    []string{cpArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	daemonArn, _ := created["daemonArn"].(string)

	rec = doECSRequest(t, h, "DeleteDaemon", map[string]any{"daemonArn": daemonArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "DELETE_IN_PROGRESS", resp["status"])

	// Deleted daemons are gone from state; a subsequent Describe 404s.
	rec = doECSRequest(t, h, "DescribeDaemon", map[string]any{"daemonArn": daemonArn})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = doECSRequest(t, h, "DeleteDaemon", map[string]any{"daemonArn": daemonArn})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestECS_ListDaemons(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerDaemonTaskDef(t, h, "list-daemons-family")
	cpArn := createCapacityProviderForDaemon(t, h, "list-daemons-cp")

	for _, name := range []string{"list-daemon-a", "list-daemon-b"} {
		rec := doECSRequest(t, h, "CreateDaemon", map[string]any{
			"daemonName":              name,
			"daemonTaskDefinitionArn": tdArn,
			"capacityProviderArns":    []string{cpArn},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doECSRequest(t, h, "ListDaemons", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	daemons, ok := resp["daemonSummariesList"].([]any)
	require.True(t, ok)
	assert.Len(t, daemons, 2)

	rec = doECSRequest(t, h, "ListDaemons", map[string]any{"capacityProviderArns": []string{"arn:none"}})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	daemons, ok = resp["daemonSummariesList"].([]any)
	require.True(t, ok)
	assert.Empty(t, daemons)
}

// ----- DescribeDaemonDeployments / ListDaemonDeployments / DescribeDaemonRevisions -----

func TestECS_DescribeDaemonDeployments(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerDaemonTaskDef(t, h, "deploy-family")
	cpArn := createCapacityProviderForDaemon(t, h, "deploy-cp")

	rec := doECSRequest(t, h, "CreateDaemon", map[string]any{
		"daemonName":              "deploy-target",
		"daemonTaskDefinitionArn": tdArn,
		"capacityProviderArns":    []string{cpArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	deploymentArn, _ := created["deploymentArn"].(string)
	require.NotEmpty(t, deploymentArn)

	rec = doECSRequest(t, h, "DescribeDaemonDeployments", map[string]any{
		"daemonDeploymentArns": []string{deploymentArn, "arn:aws:ecs:us-east-1:000000000000:daemon-deployment/missing"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	deps, ok := resp["daemonDeployments"].([]any)
	require.True(t, ok)
	require.Len(t, deps, 1)

	dep, ok := deps[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "SUCCESSFUL", dep["status"])
	assert.Equal(t, deploymentArn, dep["daemonDeploymentArn"])

	failures, ok := resp["failures"].([]any)
	require.True(t, ok)
	require.Len(t, failures, 1)
	failure, ok := failures[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "MISSING", failure["reason"])
}

func TestECS_ListDaemonDeployments(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerDaemonTaskDef(t, h, "list-deploy-family")
	cpArn := createCapacityProviderForDaemon(t, h, "list-deploy-cp")

	rec := doECSRequest(t, h, "CreateDaemon", map[string]any{
		"daemonName":              "list-deploy-target",
		"daemonTaskDefinitionArn": tdArn,
		"capacityProviderArns":    []string{cpArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	daemonArn, _ := created["daemonArn"].(string)

	rec = doECSRequest(t, h, "ListDaemonDeployments", map[string]any{"daemonArn": daemonArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	deps, ok := resp["daemonDeployments"].([]any)
	require.True(t, ok)
	assert.Len(t, deps, 1)
}

func TestECS_DescribeDaemonRevisions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerDaemonTaskDef(t, h, "revision-family")
	cpArn := createCapacityProviderForDaemon(t, h, "revision-cp")

	rec := doECSRequest(t, h, "CreateDaemon", map[string]any{
		"daemonName":              "revision-target",
		"daemonTaskDefinitionArn": tdArn,
		"capacityProviderArns":    []string{cpArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	daemonArn, _ := created["daemonArn"].(string)

	rec = doECSRequest(t, h, "DescribeDaemon", map[string]any{"daemonArn": daemonArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var described map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &described))
	d, ok := described["daemon"].(map[string]any)
	require.True(t, ok)
	revs, ok := d["currentRevisions"].([]any)
	require.True(t, ok)
	require.Len(t, revs, 1)
	revDetail, ok := revs[0].(map[string]any)
	require.True(t, ok)
	revArn, _ := revDetail["arn"].(string)
	require.NotEmpty(t, revArn)

	rec = doECSRequest(t, h, "DescribeDaemonRevisions", map[string]any{
		"daemonRevisionArns": []string{revArn, "arn:aws:ecs:us-east-1:000000000000:daemon-revision/missing"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	revisions, ok := resp["daemonRevisions"].([]any)
	require.True(t, ok)
	require.Len(t, revisions, 1)

	revision, ok := revisions[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, daemonArn, revision["daemonArn"])
	assert.Equal(t, tdArn, revision["daemonTaskDefinitionArn"])
	images, ok := revision["containerImages"].([]any)
	require.True(t, ok)
	require.Len(t, images, 1)
	image, ok := images[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "agent", image["containerName"])
	assert.Equal(t, "example/agent:latest", image["image"])

	failures, ok := resp["failures"].([]any)
	require.True(t, ok)
	require.Len(t, failures, 1)
}
