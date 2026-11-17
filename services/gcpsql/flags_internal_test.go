package gcpsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAppliedFlagAllowList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		flag string
		pg   bool
		want bool
	}{
		{name: "pg catalogue flag", flag: "work_mem", pg: true, want: true},
		{name: "mysql catalogue flag", flag: "wait_timeout", pg: false, want: true},
		{name: "pg flag on mysql engine", flag: "work_mem", pg: false, want: false},
		{name: "mysql flag on pg engine", flag: "wait_timeout", pg: true, want: false},
		{name: "archive command", flag: "archive_command", pg: true, want: false},
		{name: "archive mode", flag: "archive_mode", pg: true, want: false},
		{name: "read only default", flag: "default_transaction_read_only", pg: true, want: false},
		{name: "primary conninfo", flag: "primary_conninfo", pg: true, want: false},
		{name: "restore command", flag: "restore_command", pg: true, want: false},
		{name: "super read only", flag: "super_read_only", pg: false, want: false},
		{name: "read only", flag: "read_only", pg: false, want: false},
		{name: "cloudsql", flag: "cloudsql.iam_authentication", pg: true, want: false},
		{name: "empty", flag: "", pg: true, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, appliedFlag(tt.pg, tt.flag))
		})
	}
}
