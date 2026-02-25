package eventbridge_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/eventbridge"
	"github.com/stretchr/testify/assert"
)

// matchPatternForTest exposes the internal matchPattern via a test helper.
// We test it through the backend's PutEvents fan-out behavior.
// Direct unit tests use a table-driven approach via the exported TestMatchPattern.

func TestPattern_ExactMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		event   string
		want    bool
	}{
		{
			name:    "source exact match - positive",
			pattern: `{"source": ["my.service"]}`,
			event:   `{"source": "my.service"}`,
			want:    true,
		},
		{
			name:    "source exact match - negative",
			pattern: `{"source": ["my.service"]}`,
			event:   `{"source": "other.service"}`,
			want:    false,
		},
		{
			name:    "source multi-value - match first",
			pattern: `{"source": ["a", "b"]}`,
			event:   `{"source": "a"}`,
			want:    true,
		},
		{
			name:    "source multi-value - match second",
			pattern: `{"source": ["a", "b"]}`,
			event:   `{"source": "b"}`,
			want:    true,
		},
		{
			name:    "source multi-value - no match",
			pattern: `{"source": ["a", "b"]}`,
			event:   `{"source": "c"}`,
			want:    false,
		},
		{
			name:    "multiple fields - both match",
			pattern: `{"source": ["svc"], "detail-type": ["MyEvent"]}`,
			event:   `{"source": "svc", "detail-type": "MyEvent"}`,
			want:    true,
		},
		{
			name:    "multiple fields - one mismatch",
			pattern: `{"source": ["svc"], "detail-type": ["MyEvent"]}`,
			event:   `{"source": "svc", "detail-type": "Other"}`,
			want:    false,
		},
		{
			name:    "nested detail match",
			pattern: `{"detail": {"status": ["ok"]}}`,
			event:   `{"detail": {"status": "ok"}}`,
			want:    true,
		},
		{
			name:    "nested detail mismatch",
			pattern: `{"detail": {"status": ["ok"]}}`,
			event:   `{"detail": {"status": "fail"}}`,
			want:    false,
		},
		{
			name:    "empty pattern matches everything",
			pattern: `{}`,
			event:   `{"source": "anything"}`,
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := eventbridge.MatchPatternForTest(tt.pattern, tt.event)
			assert.Equal(t, tt.want, got, "pattern=%s event=%s", tt.pattern, tt.event)
		})
	}
}

func TestPattern_PrefixMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		event   string
		want    bool
	}{
		{
			name:    "prefix match positive",
			pattern: `{"source": [{"prefix": "com.example"}]}`,
			event:   `{"source": "com.example.service"}`,
			want:    true,
		},
		{
			name:    "prefix match negative",
			pattern: `{"source": [{"prefix": "com.example"}]}`,
			event:   `{"source": "org.other.service"}`,
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := eventbridge.MatchPatternForTest(tt.pattern, tt.event)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPattern_ExistsMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		event   string
		want    bool
	}{
		{
			name:    "exists:true - field present",
			pattern: `{"source": [{"exists": true}]}`,
			event:   `{"source": "svc"}`,
			want:    true,
		},
		{
			name:    "exists:true - field absent",
			pattern: `{"source": [{"exists": true}]}`,
			event:   `{"other": "svc"}`,
			want:    false,
		},
		{
			name:    "exists:false - field absent",
			pattern: `{"source": [{"exists": false}]}`,
			event:   `{"other": "svc"}`,
			want:    true,
		},
		{
			name:    "exists:false - field present",
			pattern: `{"source": [{"exists": false}]}`,
			event:   `{"source": "svc"}`,
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := eventbridge.MatchPatternForTest(tt.pattern, tt.event)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPattern_NumericMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		event   string
		want    bool
	}{
		{
			name:    "numeric > positive",
			pattern: `{"detail": {"count": [{"numeric": [">", 5]}]}}`,
			event:   `{"detail": {"count": 10}}`,
			want:    true,
		},
		{
			name:    "numeric > negative",
			pattern: `{"detail": {"count": [{"numeric": [">", 5]}]}}`,
			event:   `{"detail": {"count": 3}}`,
			want:    false,
		},
		{
			name:    "numeric range",
			pattern: `{"detail": {"count": [{"numeric": [">=", 1, "<=", 10]}]}}`,
			event:   `{"detail": {"count": 5}}`,
			want:    true,
		},
		{
			name:    "numeric range - outside",
			pattern: `{"detail": {"count": [{"numeric": [">=", 1, "<=", 10]}]}}`,
			event:   `{"detail": {"count": 15}}`,
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := eventbridge.MatchPatternForTest(tt.pattern, tt.event)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPattern_AnythingButMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		event   string
		want    bool
	}{
		{
			name:    "anything-but list - not in list",
			pattern: `{"source": [{"anything-but": ["bad", "ugly"]}]}`,
			event:   `{"source": "good"}`,
			want:    true,
		},
		{
			name:    "anything-but list - in list",
			pattern: `{"source": [{"anything-but": ["bad", "ugly"]}]}`,
			event:   `{"source": "bad"}`,
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := eventbridge.MatchPatternForTest(tt.pattern, tt.event)
			assert.Equal(t, tt.want, got)
		})
	}
}
