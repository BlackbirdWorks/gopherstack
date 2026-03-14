package xray_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/xray"
)

func newTestBackend(t *testing.T) *xray.InMemoryBackend {
	t.Helper()

	return xray.NewInMemoryBackend()
}

func TestInMemoryBackend_CreateGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		groupName    string
		filterExpr   string
		createFirst  bool
		wantErr      bool
		wantErrIs    error
	}{
		{
			name:      "creates group",
			groupName: "my-group",
		},
		{
			name:       "creates group with filter",
			groupName:  "filtered-group",
			filterExpr: `service("my-service")`,
		},
		{
			name:        "duplicate group returns conflict",
			groupName:   "dup-group",
			createFirst: true,
			wantErr:     true,
			wantErrIs:   awserr.ErrConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			if tt.createFirst {
				_, err := b.CreateGroup(tt.groupName, "")
				require.NoError(t, err)
			}

			g, err := b.CreateGroup(tt.groupName, tt.filterExpr)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					assert.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.groupName, g.GroupName)
			assert.Equal(t, tt.filterExpr, g.FilterExpression)
			assert.NotEmpty(t, g.GroupARN)
		})
	}
}

func TestInMemoryBackend_GetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		groupName string
		create    bool
		wantErr   bool
		wantErrIs error
	}{
		{
			name:      "gets existing group",
			groupName: "my-group",
			create:    true,
		},
		{
			name:      "not found",
			groupName: "missing-group",
			wantErr:   true,
			wantErrIs: awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			if tt.create {
				_, err := b.CreateGroup(tt.groupName, "")
				require.NoError(t, err)
			}

			g, err := b.GetGroup(tt.groupName)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					assert.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.groupName, g.GroupName)
		})
	}
}

func TestInMemoryBackend_GetGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		groupNames []string
		wantCount  int
	}{
		{
			name:      "empty",
			wantCount: 0,
		},
		{
			name:       "multiple groups sorted by name",
			groupNames: []string{"beta-group", "alpha-group", "gamma-group"},
			wantCount:  3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			for _, name := range tt.groupNames {
				_, err := b.CreateGroup(name, "")
				require.NoError(t, err)
			}

			groups := b.GetGroups()
			assert.Len(t, groups, tt.wantCount)

			if len(groups) > 1 {
				assert.Less(t, groups[0].GroupName, groups[1].GroupName)
			}
		})
	}
}

func TestInMemoryBackend_UpdateGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		groupName    string
		newFilter    string
		create       bool
		wantErr      bool
		wantErrIs    error
	}{
		{
			name:      "updates filter expression",
			groupName: "my-group",
			newFilter: `service("updated-svc")`,
			create:    true,
		},
		{
			name:      "not found",
			groupName: "missing-group",
			wantErr:   true,
			wantErrIs: awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			if tt.create {
				_, err := b.CreateGroup(tt.groupName, "old-filter")
				require.NoError(t, err)
			}

			g, err := b.UpdateGroup(tt.groupName, tt.newFilter)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					assert.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.newFilter, g.FilterExpression)
		})
	}
}

func TestInMemoryBackend_DeleteGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		groupName string
		create    bool
		wantErr   bool
		wantErrIs error
	}{
		{
			name:      "deletes existing group",
			groupName: "my-group",
			create:    true,
		},
		{
			name:      "not found",
			groupName: "missing-group",
			wantErr:   true,
			wantErrIs: awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			if tt.create {
				_, err := b.CreateGroup(tt.groupName, "")
				require.NoError(t, err)
			}

			err := b.DeleteGroup(tt.groupName)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					assert.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)

			_, getErr := b.GetGroup(tt.groupName)
			require.Error(t, getErr)
		})
	}
}

func TestInMemoryBackend_CreateSamplingRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		rule        xray.SamplingRule
		createFirst bool
		wantErr     bool
		wantErrIs   error
	}{
		{
			name: "creates rule",
			rule: xray.SamplingRule{RuleName: "my-rule", FixedRate: 0.05, Priority: 1},
		},
		{
			name:        "duplicate returns conflict",
			rule:        xray.SamplingRule{RuleName: "dup-rule"},
			createFirst: true,
			wantErr:     true,
			wantErrIs:   awserr.ErrConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			if tt.createFirst {
				_, err := b.CreateSamplingRule(tt.rule)
				require.NoError(t, err)
			}

			r, err := b.CreateSamplingRule(tt.rule)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					assert.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.rule.RuleName, r.RuleName)
			assert.NotEmpty(t, r.RuleARN)
		})
	}
}

func TestInMemoryBackend_GetSamplingRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		ruleNames []string
		wantCount int
	}{
		{
			name:      "empty",
			wantCount: 0,
		},
		{
			name:      "multiple rules sorted by name",
			ruleNames: []string{"rule-b", "rule-a", "rule-c"},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			for _, name := range tt.ruleNames {
				_, err := b.CreateSamplingRule(xray.SamplingRule{RuleName: name})
				require.NoError(t, err)
			}

			rules := b.GetSamplingRules()
			assert.Len(t, rules, tt.wantCount)

			if len(rules) > 1 {
				assert.Less(t, rules[0].RuleName, rules[1].RuleName)
			}
		})
	}
}

func TestInMemoryBackend_UpdateSamplingRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updates   xray.SamplingRule
		name      string
		ruleName  string
		create    bool
		wantErr   bool
		wantErrIs error
	}{
		{
			name:     "updates service name",
			ruleName: "my-rule",
			create:   true,
			updates:  xray.SamplingRule{ServiceName: "updated-svc"},
		},
		{
			name:      "not found",
			ruleName:  "missing-rule",
			wantErr:   true,
			wantErrIs: awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			if tt.create {
				_, err := b.CreateSamplingRule(xray.SamplingRule{RuleName: tt.ruleName})
				require.NoError(t, err)
			}

			r, err := b.UpdateSamplingRule(tt.ruleName, tt.updates)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					assert.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.updates.ServiceName, r.ServiceName)
		})
	}
}

func TestInMemoryBackend_DeleteSamplingRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		ruleName  string
		create    bool
		wantErr   bool
		wantErrIs error
	}{
		{
			name:     "deletes existing rule",
			ruleName: "my-rule",
			create:   true,
		},
		{
			name:      "not found",
			ruleName:  "missing-rule",
			wantErr:   true,
			wantErrIs: awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			if tt.create {
				_, err := b.CreateSamplingRule(xray.SamplingRule{RuleName: tt.ruleName})
				require.NoError(t, err)
			}

			r, err := b.DeleteSamplingRule(tt.ruleName)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					assert.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.ruleName, r.RuleName)

			rules := b.GetSamplingRules()
			assert.Empty(t, rules)
		})
	}
}

func TestInMemoryBackend_PutTraceSegments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		segments         []string
		wantUnprocessed  int
	}{
		{
			name:            "valid segment stored",
			segments:        []string{`{"trace_id":"1-abc123","id":"seg1","name":"test"}`},
			wantUnprocessed: 0,
		},
		{
			name:            "invalid JSON becomes unprocessed",
			segments:        []string{"not-json"},
			wantUnprocessed: 1,
		},
		{
			name:            "missing trace_id becomes unprocessed",
			segments:        []string{`{"id":"seg1","name":"test"}`},
			wantUnprocessed: 1,
		},
		{
			name:            "empty segments",
			segments:        []string{},
			wantUnprocessed: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			unprocessed := b.PutTraceSegments(tt.segments)
			assert.Len(t, unprocessed, tt.wantUnprocessed)
		})
	}
}

func TestInMemoryBackend_GetTraceSummaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		segments  []string
		wantCount int
	}{
		{
			name:      "empty",
			wantCount: 0,
		},
		{
			name: "returns stored traces",
			segments: []string{
				`{"trace_id":"1-abc","id":"seg1","name":"test"}`,
				`{"trace_id":"1-def","id":"seg2","name":"test2"}`,
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			if len(tt.segments) > 0 {
				_ = b.PutTraceSegments(tt.segments)
			}

			traces := b.GetTraceSummaries()
			assert.Len(t, traces, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_GetTrace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		traceID string
		store   bool
		wantNil bool
	}{
		{
			name:    "returns existing trace",
			traceID: "1-abc123",
			store:   true,
		},
		{
			name:    "returns nil for missing trace",
			traceID: "1-missing",
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			if tt.store {
				seg := `{"trace_id":"` + tt.traceID + `","id":"seg1","name":"test"}`
				_ = b.PutTraceSegments([]string{seg})
			}

			trace := b.GetTrace(tt.traceID)

			if tt.wantNil {
				assert.Nil(t, trace)

				return
			}

			assert.NotNil(t, trace)
			assert.Equal(t, tt.traceID, trace.TraceID)
		})
	}
}
