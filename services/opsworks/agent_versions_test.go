package opsworks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeStaticLists verifies the always-non-empty static-list
// describe-only operations (DescribeAgentVersions, DescribeOperatingSystems).
func TestDescribeStaticLists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string
		body      map[string]any
		checkKey  string
	}{
		{
			name:      "DescribeAgentVersions returns versions",
			operation: "DescribeAgentVersions",
			body:      map[string]any{},
			checkKey:  "AgentVersions",
		},
		{
			name:      "DescribeOperatingSystems returns OS list",
			operation: "DescribeOperatingSystems",
			body:      map[string]any{},
			checkKey:  "OperatingSystems",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doTarget(t, h, tt.operation, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseJSON(t, rec.Body.Bytes())
			_, ok := resp[tt.checkKey]
			assert.True(t, ok, "response should contain key %q", tt.checkKey)
		})
	}
}

// TestDescribeAgentVersions verifies non-empty static list.
func TestDescribeAgentVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTarget(t, h, "DescribeAgentVersions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseJSON(t, rec.Body.Bytes())
	versions := resp["AgentVersions"].([]any)
	assert.NotEmpty(t, versions)
	v := versions[0].(map[string]any)
	assert.NotEmpty(t, v["Version"])
}

// TestDescribeOperatingSystems verifies non-empty static list.
func TestDescribeOperatingSystems(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTarget(t, h, "DescribeOperatingSystems", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseJSON(t, rec.Body.Bytes())
	oses := resp["OperatingSystems"].([]any)
	assert.NotEmpty(t, oses)
	os := oses[0].(map[string]any)
	assert.NotEmpty(t, os["Id"])
	assert.NotEmpty(t, os["Name"])
}
