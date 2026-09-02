package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckManifest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		slug       string
		content    string
		wantSlug   string
		wantIssues []string
	}{
		{
			name: "valid fenced",
			slug: "dlm",
			content: "---\n" +
				"service: dlm\n" +
				"sdk_module: aws-sdk-go-v2/service/dlm@v1.39.4\n" +
				"last_audit_commit: fca4a71a1\n" +
				"last_audit_date: 2026-07-01\n" +
				"overall: A\n" +
				"---\n" +
				"## Notes\n",
			wantSlug: "dlm",
		},
		{
			name: "valid unfenced",
			slug: "servicediscovery",
			content: "service: servicediscovery\n" +
				"sdk_module: aws-sdk-go-v2/service/servicediscovery@v1.43.4\n" +
				"botocore_model: servicediscovery/2017-03-14/service-2.json\n" +
				"last_audit_commit: e50f52dce\n" +
				"last_audit_date: 2026-08-28\n" +
				"overall: A\n" +
				"## Notes\n",
			wantSlug: "servicediscovery",
		},
		{
			name: "unrecognized top-level field tolerated",
			slug: "cloudfront",
			content: "---\n" +
				"service: cloudfront\n" +
				"sibling_sdk_modules: [aws-sdk-go-v2/service/cloudfrontkeyvaluestore@v1.15.4]\n" +
				"last_audit_commit: abc1234\n" +
				"---\n",
			wantSlug: "cloudfront",
		},
		{
			name:    "no frontmatter at all",
			slug:    "orphan",
			content: "just some free text with no schema fields\n",
			wantIssues: []string{
				"service: field missing or empty",
			},
		},
		{
			name: "missing service field",
			slug: "s3",
			content: "---\n" +
				"sdk_module: aws-sdk-go-v2/service/s3@v1.0.0\n" +
				"last_audit_commit: abc1234\n" +
				"---\n",
			wantIssues: []string{
				"service: field missing or empty",
			},
		},
		{
			name: "empty service value",
			slug: "s3",
			content: "---\n" +
				"service:\n" +
				"last_audit_commit: abc1234\n" +
				"---\n",
			wantIssues: []string{
				"service: field missing or empty",
			},
		},
		{
			name: "service does not match directory",
			slug: "s3control",
			content: "---\n" +
				"service: s3\n" +
				"last_audit_commit: abc1234\n" +
				"---\n",
			wantSlug: "s3",
			wantIssues: []string{
				`service: "s3" does not match directory "s3control"`,
			},
		},
		{
			name: "unresolved merge conflict marker",
			slug: "ec2",
			content: "---\n" +
				"service: ec2\n" +
				"<<<<<<< HEAD\n" +
				"last_audit_commit: abc1234\n" +
				"=======\n" +
				"last_audit_commit: def5678\n" +
				">>>>>>> branch\n" +
				"---\n",
			wantSlug: "ec2",
			wantIssues: []string{
				"line 3: unresolved git merge-conflict marker",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := checkManifest(tt.slug, tt.content)

			require.Equal(t, tt.slug, r.service)
			assert.Equal(t, tt.wantSlug, r.docSlug)
			assert.Equal(t, tt.wantIssues, r.issues)
		})
	}
}

func TestFindMergeConflictMarker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		want    int
	}{
		{
			name:    "clean",
			content: "service: dlm\nlast_audit_commit: abc1234\n",
		},
		{
			name:    "conflict start marker",
			content: "service: dlm\n<<<<<<< HEAD\nlast_audit_commit: abc1234\n",
			want:    2,
		},
		{
			name:    "conflict separator only",
			content: "service: dlm\n=======\nlast_audit_commit: abc1234\n",
			want:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			lines := splitLines(tt.content)
			got := findMergeConflictMarker(lines)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFindServiceField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		content   string
		wantValue string
		wantFound bool
	}{
		{
			name:      "plain value",
			content:   "service: dlm\nlast_audit_commit: abc1234\n",
			wantValue: "dlm",
			wantFound: true,
		},
		{
			name:      "quoted value",
			content:   `service: "dlm"` + "\n",
			wantValue: "dlm",
			wantFound: true,
		},
		{
			name:    "not present",
			content: "last_audit_commit: abc1234\n",
		},
		{
			name:    "indented service line is not a top-level field",
			content: "  service: dlm\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			value, found := findServiceField(splitLines(tt.content))

			assert.Equal(t, tt.wantFound, found)
			assert.Equal(t, tt.wantValue, value)
		})
	}
}

func splitLines(content string) []string {
	return strings.Split(content, "\n")
}
