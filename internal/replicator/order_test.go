package replicator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/replicator"
)

func indexOf(s []string, v string) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}

	return -1
}

func TestOrderQueuesPutsDLQsFirst(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dlqOf  map[string]string
		name   string
		names  []string
		before [][2]string
	}{
		{
			name:   "dlq listed after",
			names:  []string{"main", "dlq"},
			dlqOf:  map[string]string{"main": "dlq", "dlq": ""},
			before: [][2]string{{"dlq", "main"}},
		},
		{
			name:   "chain",
			names:  []string{"a", "b", "c"},
			dlqOf:  map[string]string{"a": "b", "b": "c", "c": ""},
			before: [][2]string{{"c", "b"}, {"b", "a"}},
		},
		{
			name:   "dlq outside selection",
			names:  []string{"main"},
			dlqOf:  map[string]string{"main": "elsewhere"},
			before: nil,
		},
		{
			name:  "cycle terminates",
			names: []string{"a", "b"},
			dlqOf: map[string]string{"a": "b", "b": "a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := replicator.OrderQueues(tt.names, tt.dlqOf)
			require.Len(t, got, len(tt.names))

			for _, p := range tt.before {
				assert.Less(t, indexOf(got, p[0]), indexOf(got, p[1]), "%s before %s in %v", p[0], p[1], got)
			}
		})
	}
}

func TestOrderTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		selected []string
		want     []string
		before   [][2]string
		wantErr  bool
	}{
		{name: "all", before: [][2]string{
			{"kms", "sqs"}, {"iam", "lambda"}, {"sqs", "sns"}, {"lambda", "sns"}, {"sns", "eventbridge"},
		}},
		{name: "reorders", selected: []string{"lambda", "iam"}, want: []string{"iam", "lambda"}},
		{name: "dedupes and normalizes", selected: []string{"SNS", "sqs", "sns"}, want: []string{"sqs", "sns"}},
		{name: "unknown", selected: []string{"ec2"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := replicator.OrderTypes(tt.selected)
			if tt.wantErr {
				require.ErrorIs(t, err, replicator.ErrUnknownType)

				return
			}

			require.NoError(t, err)

			if tt.want != nil {
				assert.Equal(t, tt.want, got)
			}

			for _, p := range tt.before {
				assert.Less(t, indexOf(got, p[0]), indexOf(got, p[1]))
			}
		})
	}
}
