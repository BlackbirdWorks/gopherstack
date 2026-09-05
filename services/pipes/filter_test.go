package pipes_test

// Exercises the matchesAnyFilter/matchesJSONPattern engine (filter.go) end to
// end through the runner: JSON event-pattern field matching, prefix/suffix/
// anything-but operators, and multi-filter OR semantics.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// TestFilter_JSONPattern verifies that JSON event pattern filters are
// evaluated against the structured message body (not substring match).
func TestFilter_JSONPattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		pattern     string
		msgBodies   []string
		wantDeleted []string
	}{
		{
			name:    "exact_field_match_passes",
			pattern: `{"type":["order"]}`,
			msgBodies: []string{
				`{"type":"order","id":1}`,
				`{"type":"inventory","id":2}`,
			},
			wantDeleted: []string{"rh-0"},
		},
		{
			name:    "multi_field_match_both_must_match",
			pattern: `{"type":["order"],"status":["paid"]}`,
			msgBodies: []string{
				`{"type":"order","status":"paid","id":1}`,
				`{"type":"order","status":"pending","id":2}`,
				`{"type":"invoice","status":"paid","id":3}`,
			},
			wantDeleted: []string{"rh-0"},
		},
		{
			name:    "no_match_drops_all",
			pattern: `{"type":["missing-type"]}`,
			msgBodies: []string{
				`{"type":"order"}`,
			},
			wantDeleted: nil,
		},
		{
			name:    "empty_pattern_passes_all",
			pattern: "",
			msgBodies: []string{
				`{"type":"order"}`,
				`{"type":"inventory"}`,
			},
			wantDeleted: []string{"rh-0", "rh-1"},
		},
		{
			name:    "non_json_pattern_uses_substring",
			pattern: "order",
			msgBodies: []string{
				`{"type":"order"}`,
				`{"type":"inventory"}`,
			},
			wantDeleted: []string{"rh-0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b3Backend()
			_, err := b.CreatePipe(t.Context(), pipes.CreatePipeInput{
				Name:         tt.name + "-pipe",
				RoleARN:      "arn:aws:iam::111122223333:role/r",
				Source:       b3SQSSource,
				Target:       b3LambdaTarget,
				DesiredState: "RUNNING",
				SourceParameters: &pipes.SourceParameters{
					FilterCriteria: &pipes.FilterCriteria{
						Filters: []pipes.Filter{{Pattern: tt.pattern}},
					},
				},
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, tt.name+"-pipe")

			msgs := make([]*pipes.SQSMessage, len(tt.msgBodies))
			for i, body := range tt.msgBodies {
				msgs[i] = &pipes.SQSMessage{
					MessageID:     "m-" + string(rune('0'+i)),
					ReceiptHandle: "rh-" + string(rune('0'+i)),
					Body:          body,
				}
			}

			sqsReader := &b3MockSQSReader{messages: msgs}
			lambdaInvoker := &b3MockLambdaInvoker{}

			runner := pipes.NewRunner(b)
			runner.SetSQSReader(sqsReader)
			runner.SetLambdaInvoker(lambdaInvoker)

			pipes.PollAllPipesOnce(t.Context(), runner)

			sqsReader.mu.Lock()
			deleted := sqsReader.deleted
			sqsReader.mu.Unlock()

			assert.Equal(t, tt.wantDeleted, deleted)
		})
	}
}

// TestFilter_PatternOperators verifies prefix, suffix, and anything-but
// pattern operators in JSON event pattern matching.
func TestFilter_PatternOperators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pattern   string
		msgBody   string
		wantMatch bool
	}{
		{
			name:      "prefix_operator_matches",
			pattern:   `{"type":[{"prefix":"ord"}]}`,
			msgBody:   `{"type":"order"}`,
			wantMatch: true,
		},
		{
			name:      "prefix_operator_no_match",
			pattern:   `{"type":[{"prefix":"inv"}]}`,
			msgBody:   `{"type":"order"}`,
			wantMatch: false,
		},
		{
			name:      "suffix_operator_matches",
			pattern:   `{"type":[{"suffix":"der"}]}`,
			msgBody:   `{"type":"order"}`,
			wantMatch: true,
		},
		{
			name:      "suffix_operator_no_match",
			pattern:   `{"type":[{"suffix":"xyz"}]}`,
			msgBody:   `{"type":"order"}`,
			wantMatch: false,
		},
		{
			name:      "anything_but_matches_when_not_excluded",
			pattern:   `{"status":[{"anything-but":["cancelled","failed"]}]}`,
			msgBody:   `{"status":"paid"}`,
			wantMatch: true,
		},
		{
			name:      "anything_but_no_match_when_excluded",
			pattern:   `{"status":[{"anything-but":["cancelled","failed"]}]}`,
			msgBody:   `{"status":"cancelled"}`,
			wantMatch: false,
		},
		{
			// AWS event-pattern docs' own example: `"state": [ { "anything-but": "initializing" } ]`
			// -- a single string, not a list.
			name:      "anything_but_single_value_matches_when_not_excluded",
			pattern:   `{"status":[{"anything-but":"cancelled"}]}`,
			msgBody:   `{"status":"paid"}`,
			wantMatch: true,
		},
		{
			name:      "anything_but_single_value_no_match_when_excluded",
			pattern:   `{"status":[{"anything-but":"cancelled"}]}`,
			msgBody:   `{"status":"cancelled"}`,
			wantMatch: false,
		},
		{
			// docs: `"ProductName": [ { "exists": true } ]` matches when the field is present.
			name:      "exists_true_matches_present_field",
			pattern:   `{"type":[{"exists":true}]}`,
			msgBody:   `{"type":"order"}`,
			wantMatch: true,
		},
		{
			name:      "exists_true_no_match_absent_field",
			pattern:   `{"type":[{"exists":true}]}`,
			msgBody:   `{"other":"x"}`,
			wantMatch: false,
		},
		{
			// docs: `"ProductName": [ { "exists": false } ]` matches when the field is absent.
			name:      "exists_false_matches_absent_field",
			pattern:   `{"type":[{"exists":false}]}`,
			msgBody:   `{"other":"x"}`,
			wantMatch: true,
		},
		{
			name:      "exists_false_no_match_present_field",
			pattern:   `{"type":[{"exists":false}]}`,
			msgBody:   `{"type":"order"}`,
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b3Backend()
			_, err := b.CreatePipe(t.Context(), pipes.CreatePipeInput{
				Name:         "op-" + tt.name,
				RoleARN:      "arn:aws:iam::111122223333:role/r",
				Source:       b3SQSSource,
				Target:       b3LambdaTarget,
				DesiredState: "RUNNING",
				SourceParameters: &pipes.SourceParameters{
					FilterCriteria: &pipes.FilterCriteria{
						Filters: []pipes.Filter{{Pattern: tt.pattern}},
					},
				},
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, "op-"+tt.name)

			sqsReader := &b3MockSQSReader{
				messages: []*pipes.SQSMessage{{MessageID: "m1", ReceiptHandle: "rh1", Body: tt.msgBody}},
			}
			lambdaInvoker := &b3MockLambdaInvoker{}

			runner := pipes.NewRunner(b)
			runner.SetSQSReader(sqsReader)
			runner.SetLambdaInvoker(lambdaInvoker)

			pipes.PollAllPipesOnce(t.Context(), runner)

			sqsReader.mu.Lock()
			deleted := sqsReader.deleted
			sqsReader.mu.Unlock()

			if tt.wantMatch {
				assert.Equal(t, []string{"rh1"}, deleted, "message should pass filter and be deleted")
			} else {
				assert.Empty(t, deleted, "message should be dropped by filter and not deleted")
			}
		})
	}
}

// TestFilter_MultipleFilters verifies that multiple filter patterns create
// an OR condition (any matching filter passes the message).
func TestFilter_MultipleFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		filters     []pipes.Filter
		msgBodies   []string
		wantMatched int
	}{
		{
			name: "two_patterns_or_logic",
			filters: []pipes.Filter{
				{Pattern: `{"type":["order"]}`},
				{Pattern: `{"type":["payment"]}`},
			},
			msgBodies: []string{
				`{"type":"order"}`,
				`{"type":"payment"}`,
				`{"type":"inventory"}`,
			},
			wantMatched: 2,
		},
		{
			name: "empty_filter_passes_all",
			filters: []pipes.Filter{
				{Pattern: `{"type":["order"]}`},
				{Pattern: ""},
			},
			msgBodies: []string{
				`{"type":"order"}`,
				`{"type":"inventory"}`,
			},
			wantMatched: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b3Backend()
			_, err := b.CreatePipe(t.Context(), pipes.CreatePipeInput{
				Name:         "mf-" + tt.name,
				RoleARN:      "arn:aws:iam::111122223333:role/r",
				Source:       b3SQSSource,
				Target:       b3LambdaTarget,
				DesiredState: "RUNNING",
				SourceParameters: &pipes.SourceParameters{
					FilterCriteria: &pipes.FilterCriteria{
						Filters: tt.filters,
					},
				},
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, "mf-"+tt.name)

			msgs := make([]*pipes.SQSMessage, len(tt.msgBodies))
			for i, body := range tt.msgBodies {
				msgs[i] = &pipes.SQSMessage{
					MessageID:     "m" + string(rune('0'+i)),
					ReceiptHandle: "rh" + string(rune('0'+i)),
					Body:          body,
				}
			}

			sqsReader := &b3MockSQSReader{messages: msgs}
			lambdaInvoker := &b3MockLambdaInvoker{}

			runner := pipes.NewRunner(b)
			runner.SetSQSReader(sqsReader)
			runner.SetLambdaInvoker(lambdaInvoker)

			pipes.PollAllPipesOnce(t.Context(), runner)

			sqsReader.mu.Lock()
			deleted := sqsReader.deleted
			sqsReader.mu.Unlock()

			assert.Len(t, deleted, tt.wantMatched,
				"%d messages should pass the OR filter", tt.wantMatched)
		})
	}
}
