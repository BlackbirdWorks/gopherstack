package swf_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

func TestSWF_DescribeWorkflowType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantType string
		wantName string
		setup    []setupAction
		wantCode int
	}{
		{
			name: "found_registered",
			setup: []setupAction{
				{action: "RegisterWorkflowType", body: map[string]any{
					"domain": "d1", "name": "wf1", "version": "1.0",
				}},
			},
			body:     map[string]any{"domain": "d1", "workflowType": map[string]any{"name": "wf1", "version": "1.0"}},
			wantCode: http.StatusOK,
			wantType: "REGISTERED",
			wantName: "wf1",
		},
		{
			name: "not_found",
			body: map[string]any{
				"domain":       "d1",
				"workflowType": map[string]any{"name": "missing", "version": "1.0"},
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			for _, s := range tt.setup {
				doSWFRequest(t, h, s.action, s.body)
			}

			rec := doSWFRequest(t, h, "DescribeWorkflowType", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				typeInfo, ok := resp["typeInfo"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantType, typeInfo["status"])
				wt := typeInfo["workflowType"].(map[string]any)
				assert.Equal(t, tt.wantName, wt["name"])
			}
		})
	}
}

func TestSWF_DeprecateWorkflowType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantType string
		setup    []setupAction
		wantCode int
	}{
		{
			name: "success",
			setup: []setupAction{
				{action: "RegisterWorkflowType", body: map[string]any{
					"domain": "d1", "name": "wf1", "version": "1.0",
				}},
			},
			body:     map[string]any{"domain": "d1", "workflowType": map[string]any{"name": "wf1", "version": "1.0"}},
			wantCode: http.StatusOK,
		},
		{
			name: "not_found",
			body: map[string]any{
				"domain":       "d1",
				"workflowType": map[string]any{"name": "missing", "version": "1.0"},
			},
			wantCode: http.StatusNotFound,
			wantType: "UnknownResourceFault",
		},
		{
			name: "already_deprecated",
			setup: []setupAction{
				{action: "RegisterWorkflowType", body: map[string]any{
					"domain": "d1", "name": "wf-dep", "version": "1.0",
				}},
				{action: "DeprecateWorkflowType", body: map[string]any{
					"domain": "d1", "workflowType": map[string]any{"name": "wf-dep", "version": "1.0"},
				}},
			},
			body: map[string]any{
				"domain":       "d1",
				"workflowType": map[string]any{"name": "wf-dep", "version": "1.0"},
			},
			wantCode: http.StatusBadRequest,
			wantType: "TypeDeprecatedFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			for _, s := range tt.setup {
				doSWFRequest(t, h, s.action, s.body)
			}

			rec := doSWFRequest(t, h, "DeprecateWorkflowType", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantType != "" {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantType, resp["__type"])
			}
		})
	}
}

func TestSWF_DeleteWorkflowType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		setup    []setupAction
		wantCode int
	}{
		{
			name: "success",
			setup: []setupAction{
				{action: "RegisterWorkflowType", body: map[string]any{
					"domain": "d1", "name": "wf1", "version": "1.0",
				}},
			},
			body:     map[string]any{"domain": "d1", "workflowType": map[string]any{"name": "wf1", "version": "1.0"}},
			wantCode: http.StatusOK,
		},
		{
			name: "not_found",
			body: map[string]any{
				"domain":       "d1",
				"workflowType": map[string]any{"name": "missing", "version": "1.0"},
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			for _, s := range tt.setup {
				doSWFRequest(t, h, s.action, s.body)
			}

			rec := doSWFRequest(t, h, "DeleteWorkflowType", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestSWF_DescribeActivityType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setupFn  func(*swf.InMemoryBackend)
		name     string
		wantType string
		wantCode int
	}{
		{
			name: "found",
			setupFn: func(b *swf.InMemoryBackend) {
				b.AddActivityTypeInternal("d1", "act1", "1.0", "REGISTERED")
			},
			body:     map[string]any{"domain": "d1", "activityType": map[string]any{"name": "act1", "version": "1.0"}},
			wantCode: http.StatusOK,
			wantType: "REGISTERED",
		},
		{
			name: "not_found",
			body: map[string]any{
				"domain":       "d1",
				"activityType": map[string]any{"name": "missing", "version": "1.0"},
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			if tt.setupFn != nil {
				tt.setupFn(b)
			}

			h := swf.NewHandler(b)
			rec := doSWFRequest(t, h, "DescribeActivityType", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantType != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				typeInfo, ok := resp["typeInfo"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantType, typeInfo["status"])
			}
		})
	}
}

func TestSWF_DeprecateActivityType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setupFn  func(*swf.InMemoryBackend)
		name     string
		wantType string
		wantCode int
	}{
		{
			name: "success",
			setupFn: func(b *swf.InMemoryBackend) {
				b.AddActivityTypeInternal("d1", "act1", "1.0", "REGISTERED")
			},
			body:     map[string]any{"domain": "d1", "activityType": map[string]any{"name": "act1", "version": "1.0"}},
			wantCode: http.StatusOK,
		},
		{
			name: "not_found",
			body: map[string]any{
				"domain":       "d1",
				"activityType": map[string]any{"name": "missing", "version": "1.0"},
			},
			wantCode: http.StatusNotFound,
			wantType: "UnknownResourceFault",
		},
		{
			name: "already_deprecated",
			setupFn: func(b *swf.InMemoryBackend) {
				b.AddActivityTypeInternal("d1", "act-dep", "1.0", "DEPRECATED")
			},
			body: map[string]any{
				"domain":       "d1",
				"activityType": map[string]any{"name": "act-dep", "version": "1.0"},
			},
			wantCode: http.StatusBadRequest,
			wantType: "TypeDeprecatedFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			if tt.setupFn != nil {
				tt.setupFn(b)
			}

			h := swf.NewHandler(b)
			rec := doSWFRequest(t, h, "DeprecateActivityType", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantType != "" {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantType, resp["__type"])
			}
		})
	}
}

func TestSWF_DeleteActivityType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setupFn  func(*swf.InMemoryBackend)
		name     string
		wantCode int
	}{
		{
			name: "success",
			setupFn: func(b *swf.InMemoryBackend) {
				b.AddActivityTypeInternal("d1", "act1", "1.0", "REGISTERED")
			},
			body:     map[string]any{"domain": "d1", "activityType": map[string]any{"name": "act1", "version": "1.0"}},
			wantCode: http.StatusOK,
		},
		{
			name: "not_found",
			body: map[string]any{
				"domain":       "d1",
				"activityType": map[string]any{"name": "missing", "version": "1.0"},
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			if tt.setupFn != nil {
				tt.setupFn(b)
			}

			h := swf.NewHandler(b)
			rec := doSWFRequest(t, h, "DeleteActivityType", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestSWF_CountOpenWorkflowExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      any
		name      string
		setup     []setupAction
		wantCode  int
		wantCount int
	}{
		{
			name: "no_executions",
			body: map[string]any{"domain": "d1"},
			setup: []setupAction{
				{action: "RegisterDomain", body: map[string]any{"name": "d1"}},
			},
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
		{
			name: "with_running_executions",
			setup: []setupAction{
				{action: "RegisterDomain", body: map[string]any{"name": "d1"}},
				{action: "StartWorkflowExecution", body: map[string]any{"domain": "d1", "workflowId": "wf-1"}},
				{action: "StartWorkflowExecution", body: map[string]any{"domain": "d1", "workflowId": "wf-2"}},
			},
			body:      map[string]any{"domain": "d1"},
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			for _, s := range tt.setup {
				doSWFRequest(t, h, s.action, s.body)
			}

			rec := doSWFRequest(t, h, "CountOpenWorkflowExecutions", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.EqualValues(t, tt.wantCount, resp["count"])
		})
	}
}

func TestSWF_CountClosedWorkflowExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      any
		name      string
		setup     []setupAction
		wantCode  int
		wantCount int
	}{
		{
			name: "no_closed",
			setup: []setupAction{
				{action: "RegisterDomain", body: map[string]any{"name": "d1"}},
				{action: "StartWorkflowExecution", body: map[string]any{"domain": "d1", "workflowId": "wf-1"}},
			},
			body:      map[string]any{"domain": "d1"},
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			for _, s := range tt.setup {
				doSWFRequest(t, h, s.action, s.body)
			}

			rec := doSWFRequest(t, h, "CountClosedWorkflowExecutions", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.EqualValues(t, tt.wantCount, resp["count"])
		})
	}
}

func TestSWF_CountPendingActivityTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      any
		name      string
		wantCode  int
		wantCount int
	}{
		{
			name:      "returns_zero",
			body:      map[string]any{"domain": "d1", "taskList": map[string]any{"name": "my-list"}},
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			rec := doSWFRequest(t, h, "CountPendingActivityTasks", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.EqualValues(t, tt.wantCount, resp["count"])
		})
	}
}

func TestSWF_CountPendingDecisionTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      any
		name      string
		wantCode  int
		wantCount int
	}{
		{
			name:      "returns_zero",
			body:      map[string]any{"domain": "d1", "taskList": map[string]any{"name": "decision-list"}},
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			rec := doSWFRequest(t, h, "CountPendingDecisionTasks", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.EqualValues(t, tt.wantCount, resp["count"])
		})
	}
}

func TestSWF_DeleteWorkflowType_ThenNotFound(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)

	doSWFRequest(t, h, "RegisterWorkflowType", map[string]any{
		"domain": "d1", "name": "wf1", "version": "1.0",
	})

	rec := doSWFRequest(t, h, "DeleteWorkflowType", map[string]any{
		"domain": "d1", "workflowType": map[string]any{"name": "wf1", "version": "1.0"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSWFRequest(t, h, "DescribeWorkflowType", map[string]any{
		"domain": "d1", "workflowType": map[string]any{"name": "wf1", "version": "1.0"},
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestSWF_DeleteActivityType_ThenNotFound(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	b.AddActivityTypeInternal("d1", "act1", "1.0", "REGISTERED")
	h := swf.NewHandler(b)

	rec := doSWFRequest(t, h, "DeleteActivityType", map[string]any{
		"domain": "d1", "activityType": map[string]any{"name": "act1", "version": "1.0"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSWFRequest(t, h, "DescribeActivityType", map[string]any{
		"domain": "d1", "activityType": map[string]any{"name": "act1", "version": "1.0"},
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestSWF_DeprecateWorkflowType_ThenDescribeShowsDeprecated(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)

	doSWFRequest(t, h, "RegisterWorkflowType", map[string]any{
		"domain": "d1", "name": "wf1", "version": "1.0",
	})

	rec := doSWFRequest(t, h, "DeprecateWorkflowType", map[string]any{
		"domain": "d1", "workflowType": map[string]any{"name": "wf1", "version": "1.0"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSWFRequest(t, h, "DescribeWorkflowType", map[string]any{
		"domain": "d1", "workflowType": map[string]any{"name": "wf1", "version": "1.0"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	typeInfo := resp["typeInfo"].(map[string]any)
	assert.Equal(t, "DEPRECATED", typeInfo["status"])
}

func TestSWF_DeprecateActivityType_ThenDescribeShowsDeprecated(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	b.AddActivityTypeInternal("d1", "act1", "1.0", "REGISTERED")
	h := swf.NewHandler(b)

	rec := doSWFRequest(t, h, "DeprecateActivityType", map[string]any{
		"domain": "d1", "activityType": map[string]any{"name": "act1", "version": "1.0"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSWFRequest(t, h, "DescribeActivityType", map[string]any{
		"domain": "d1", "activityType": map[string]any{"name": "act1", "version": "1.0"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	typeInfo := resp["typeInfo"].(map[string]any)
	assert.Equal(t, "DEPRECATED", typeInfo["status"])
}
