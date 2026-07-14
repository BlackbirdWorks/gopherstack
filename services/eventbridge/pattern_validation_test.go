package eventbridge_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// TestMalformedEventPattern_PutRule verifies that PutRule returns an
// error when the EventPattern is structurally invalid (e.g. a scalar value
// where an array is required, or an unknown matcher key), rather than silently
// accepting the pattern and having the rule never match.
func TestMalformedEventPattern_PutRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		pattern     string
		expectError bool
	}{
		{
			name:        "valid_exact_match",
			pattern:     `{"source": ["com.example"]}`,
			expectError: false,
		},
		{
			name:        "valid_prefix_matcher",
			pattern:     `{"source": [{"prefix": "com."}]}`,
			expectError: false,
		},
		{
			name:        "valid_nested_object",
			pattern:     `{"detail": {"status": ["ACTIVE"]}}`,
			expectError: false,
		},
		{
			name:        "valid_or_combinator",
			pattern:     `{"$or": [{"source": ["a"]}, {"source": ["b"]}]}`,
			expectError: false,
		},
		{
			name:        "invalid_scalar_value_for_leaf_field",
			pattern:     `{"source": "not-an-array"}`,
			expectError: true,
		},
		{
			name:        "invalid_unknown_matcher_key",
			pattern:     `{"source": [{"unknown-matcher": "value"}]}`,
			expectError: true,
		},
		{
			name:        "invalid_number_value_for_field",
			pattern:     `{"source": 42}`,
			expectError: true,
		},
		{
			name:        "invalid_or_not_array",
			pattern:     `{"$or": "not-an-array"}`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
				Name:         "test-rule",
				EventPattern: tt.pattern,
			})
			if tt.expectError {
				assert.Error(t, err, "expected error for malformed pattern %s", tt.pattern)
			} else {
				require.NoError(t, err, "expected no error for valid pattern %s", tt.pattern)
			}
		})
	}
}

// TestMalformedEventPattern_TestEventPattern verifies that
// TestEventPattern also returns an error for malformed patterns.
func TestMalformedEventPattern_TestEventPattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		pattern     string
		event       string
		expectError bool
	}{
		{
			name:        "valid_pattern_matching",
			pattern:     `{"source": ["com.example"]}`,
			event:       `{"source": "com.example"}`,
			expectError: false,
		},
		{
			name:        "invalid_scalar_in_pattern",
			pattern:     `{"source": "not-an-array"}`,
			event:       `{"source": "com.example"}`,
			expectError: true,
		},
		{
			name:        "invalid_unknown_matcher",
			pattern:     `{"source": [{"no-such-matcher": "x"}]}`,
			event:       `{"source": "x"}`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			_, err := b.TestEventPattern(context.Background(), tt.pattern, tt.event)
			if tt.expectError {
				assert.Error(t, err, "expected error for malformed pattern")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
