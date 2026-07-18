package codecommit_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_PutGetRepositoryTriggers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "trigger-repo"})

	rec := doRequest(t, h, "PutRepositoryTriggers", map[string]any{
		"repositoryName": "trigger-repo",
		"triggers": []map[string]any{
			{
				"name":           "my-trigger",
				"destinationArn": "arn:aws:sns:us-east-1:123456789012:my-topic",
				"events":         []string{"all"},
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetRepositoryTriggers", map[string]any{
		"repositoryName": "trigger-repo",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	triggers := resp["triggers"].([]any)
	assert.Len(t, triggers, 1)
}

func TestHandler_TestRepositoryTriggers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "test-trigger-repo"})
	doRequest(t, h, "PutRepositoryTriggers", map[string]any{
		"repositoryName": "test-trigger-repo",
		"triggers": []map[string]any{
			{
				"name":           "trigger1",
				"destinationArn": "arn:aws:sns:us-east-1:123456789012:topic1",
				"events":         []string{"all"},
			},
		},
	})

	rec := doRequest(t, h, "TestRepositoryTriggers", map[string]any{
		"repositoryName": "test-trigger-repo",
		"triggers":       []map[string]any{},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	succeeded := resp["successfulExecutions"].([]any)
	assert.Len(t, succeeded, 1)
}

func TestHandler_TriggerLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})

	triggers := []map[string]any{
		{
			"name":           "trigger1",
			"destinationArn": "arn:aws:sns:us-east-1:123456789012:topic1",
			"events":         []string{"all"},
		},
		{
			"name":           "trigger2",
			"destinationArn": "arn:aws:sns:us-east-1:123456789012:topic2",
			"events":         []string{"updateReference"},
		},
	}

	rec := doRequest(t, h, "PutRepositoryTriggers", map[string]any{
		"repositoryName": "repo",
		"triggers":       triggers,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetRepositoryTriggers", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	got := resp["triggers"].([]any)
	assert.Len(t, got, 2)

	// Test triggers.
	rec = doRequest(t, h, "TestRepositoryTriggers", map[string]any{
		"repositoryName": "repo",
		"triggers":       triggers,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	succeeded := resp["successfulExecutions"].([]any)
	assert.Len(t, succeeded, 2)

	// Replace with empty triggers.
	rec = doRequest(t, h, "PutRepositoryTriggers", map[string]any{
		"repositoryName": "repo",
		"triggers":       []map[string]any{},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetRepositoryTriggers", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	got = resp["triggers"].([]any)
	assert.Empty(t, got)
}
