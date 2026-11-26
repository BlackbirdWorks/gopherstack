package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildServiceTable(t *testing.T) {
	t.Parallel()

	entries := []summaryEntry{
		{
			Slug: "s3", Category: "Storage", DisplayName: "S3",
			Grade: "A", OpsCell: "26", NotesCell: "clean",
			ReadmeLink: "services/s3/README.md",
		},
		{
			Slug: "efs", Category: "Storage", DisplayName: "EFS",
			Grade: "B+", OpsCell: "—", NotesCell: "clean",
			ReadmeLink: "services/efs/README.md",
		},
		{
			Slug: "qldb", Category: "Database", DisplayName: "QLDB",
			Grade: "Removed", OpsCell: "—", NotesCell: "removed service",
			ReadmeLink: "services/qldb/README.md",
		},
	}

	tests := []struct {
		name string
		want string
	}{
		{name: "heading", want: "## Services\n\n"},
		{name: "totals", want: "**3 services:** 1 A, 1 B, 1 Removed."},
		{name: "index link", want: "](" + servicesIndexURL + ")"},
		{
			name: "storage details",
			want: "<details>\n<summary><strong>Storage</strong> (2 services)</summary>\n\n| Service |",
		},
		{name: "singular", want: "<strong>Database</strong> (1 service)</summary>"},
		{name: "blank before close", want: "| clean |\n\n</details>\n"},
		{name: "sorted rows", want: "| [EFS](services/efs/README.md) | B+ | — | clean |\n| [S3]"},
	}

	got := buildServiceTable(entries)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Contains(t, got, tc.want)
		})
	}

	require.NotContains(t, got, "### ")
}
