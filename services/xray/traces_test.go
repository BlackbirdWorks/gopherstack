package xray_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

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

func TestEvaluateFilter_AllExpressionTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expr    string
		summary xray.TraceSummaryData
		want    bool
	}{
		{
			name:    "empty expr always true",
			expr:    "",
			summary: xray.TraceSummaryData{},
			want:    true,
		},
		{
			name:    "fault matches HasFault=true",
			expr:    "fault",
			summary: xray.TraceSummaryData{HasFault: true},
			want:    true,
		},
		{
			name:    "fault does not match HasFault=false",
			expr:    "fault",
			summary: xray.TraceSummaryData{HasFault: false},
			want:    false,
		},
		{
			name:    "error matches HasError=true",
			expr:    "error",
			summary: xray.TraceSummaryData{HasError: true},
			want:    true,
		},
		{
			name:    "throttle matches HasThrottle=true",
			expr:    "throttle",
			summary: xray.TraceSummaryData{HasThrottle: true},
			want:    true,
		},
		{
			name:    "responsetime > 1 matches 2s response",
			expr:    "responsetime > 1",
			summary: xray.TraceSummaryData{ResponseTime: 2.0},
			want:    true,
		},
		{
			name:    "responsetime > 1 does not match 0.5s response",
			expr:    "responsetime > 1",
			summary: xray.TraceSummaryData{ResponseTime: 0.5},
			want:    false,
		},
		{
			name:    "responsetime >= 1 matches exactly 1s",
			expr:    "responsetime >= 1",
			summary: xray.TraceSummaryData{ResponseTime: 1.0},
			want:    true,
		},
		{
			name:    "responsetime < 2 matches 1s",
			expr:    "responsetime < 2",
			summary: xray.TraceSummaryData{ResponseTime: 1.0},
			want:    true,
		},
		{
			name: "http.status = 200 matches",
			expr: "http.status = 200",
			summary: xray.TraceSummaryData{
				HTTP: &xray.TraceSummaryHTTP{HTTPStatus: 200},
			},
			want: true,
		},
		{
			name: "http.status = 500 does not match 200",
			expr: "http.status = 500",
			summary: xray.TraceSummaryData{
				HTTP: &xray.TraceSummaryHTTP{HTTPStatus: 200},
			},
			want: false,
		},
		{
			name: "annotation filter matches",
			expr: `annotation.env = "prod"`,
			summary: xray.TraceSummaryData{
				Annotations: map[string][]xray.AnnotationOccurrence{"env": {{Value: "prod"}}},
			},
			want: true,
		},
		{
			name: "annotation filter no match",
			expr: `annotation.env = "prod"`,
			summary: xray.TraceSummaryData{
				Annotations: map[string][]xray.AnnotationOccurrence{"env": {{Value: "staging"}}},
			},
			want: false,
		},
		{
			name:    "unknown filter returns false",
			expr:    "unknown_filter_expression",
			summary: xray.TraceSummaryData{},
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := xray.EvaluateFilter(tt.expr, tt.summary)
			assert.Equal(t, tt.want, got)
		})
	}
}
