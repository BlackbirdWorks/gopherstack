package codegurureviewer_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cg "github.com/blackbirdworks/gopherstack/services/codegurureviewer"
)

func TestAnalyzerRules(t *testing.T) {
	t.Parallel()

	both := []string{"Security", "CodeQuality"}

	tests := []struct {
		files map[string]string
		types []string
		name  string
		want  []cg.ExportedFinding
	}{
		{
			name:  "aws access key in any text file",
			files: map[string]string{"conf/settings.ini": "[aws]\nid = AKIAABCDEFGHIJKLMNOP\n"},
			types: both,
			want: []cg.ExportedFinding{
				{Rule: "aws-hardcoded-access-key", Category: "SecurityIssues", Severity: "Critical", Start: 2, End: 2},
			},
		},
		{
			name:  "temporary access key prefix",
			files: map[string]string{"a.py": "k = 'ASIAABCDEFGHIJKLMNOP'\n"},
			types: both,
			want: []cg.ExportedFinding{
				{Rule: "aws-hardcoded-access-key", Category: "SecurityIssues", Severity: "Critical", Start: 1, End: 1},
			},
		},
		{
			name:  "short or lowercase key-like text is fine",
			files: map[string]string{"a.py": "k = 'AKIA123'\nj = 'akiaabcdefghijklmnop'\n"},
			types: both,
		},
		{
			name:  "binary files are skipped",
			files: map[string]string{"blob.bin": "AKIAABCDEFGHIJKLMNOP\x00\x01"},
			types: both,
		},
		{
			name:  "security only drops quality rules",
			files: map[string]string{"a.py": "f = open('x')\nk = 'AKIAABCDEFGHIJKLMNOP'\n"},
			types: []string{"Security"},
			want: []cg.ExportedFinding{
				{Rule: "aws-hardcoded-access-key", Category: "SecurityIssues", Severity: "Critical", Start: 2, End: 2},
			},
		},
		{
			name:  "go file never closed",
			files: map[string]string{"a.go": "package a\nfunc f() {\n\tf, _ := os.Open(\"x\")\n\t_ = f\n}\n"},
			types: both,
			want: []cg.ExportedFinding{
				{Rule: "unclosed-resource", Category: "ResourceLeaks", Severity: "High", Start: 3, End: 3},
			},
		},
		{
			name:  "go file closed by defer",
			files: map[string]string{"a.go": "package a\nfunc f() {\n\tf, _ := os.Open(\"x\")\n\tdefer f.Close()\n}\n"},
			types: both,
		},
		{
			name:  "python open without with or close",
			files: map[string]string{"a.py": "data = open('x')\nprint(data.read())\n"},
			types: both,
			want: []cg.ExportedFinding{
				{Rule: "unclosed-resource", Category: "ResourceLeaks", Severity: "High", Start: 1, End: 1},
			},
		},
		{
			name:  "python with statement and explicit close are fine",
			files: map[string]string{"a.py": "with open('x') as f:\n    pass\ng = open('y')\ng.close()\n"},
			types: both,
		},
		{
			name:  "java reader outside try-with-resources",
			files: map[string]string{"A.java": "class A {\n  void f() {\n    var r = new FileReader(\"x\");\n  }\n}\n"},
			types: both,
			want: []cg.ExportedFinding{
				{Rule: "unclosed-resource", Category: "ResourceLeaks", Severity: "High", Start: 3, End: 3},
			},
		},
		{
			name: "java try-with-resources is fine",
			files: map[string]string{
				"A.java": "class A {\n  void f() {\n    try (var r = new FileReader(\"x\")) {}\n  }\n}\n",
			},
			types: both,
		},
		{
			name: "err nil block returning err",
			files: map[string]string{
				"a.go": "package a\nfunc f() error {\n\tif err == nil {\n\n\t\treturn err\n\t}\n\treturn nil\n}\n",
			},
			types: both,
			want: []cg.ExportedFinding{
				{Rule: "err-nil-misuse", Category: "CodeInconsistencies", Severity: "Medium", Start: 3, End: 5},
			},
		},
		{
			name: "err nil one-liner",
			files: map[string]string{
				"a.go": "package a\nfunc f() error {\n\tif err == nil { return err }\n\treturn nil\n}\n",
			},
			types: both,
			want: []cg.ExportedFinding{
				{Rule: "err-nil-misuse", Category: "CodeInconsistencies", Severity: "Medium", Start: 3, End: 3},
			},
		},
		{
			name: "err nil returning nil is fine",
			files: map[string]string{
				"a.go": "package a\nfunc f() error {\n\tif err == nil {\n\t\treturn nil\n\t}\n\treturn err\n}\n",
			},
			types: both,
		},
		{
			name:  "err nil rule is go only",
			files: map[string]string{"a.py": "if err == nil {\n  return err\n"},
			types: both,
		},
		{
			name: "sql concatenation styles",
			files: map[string]string{
				"a.java": "String q = \"SELECT * FROM t WHERE id = \" + id;\n",
				"b.go":   "package b\nq := fmt.Sprintf(\"DELETE FROM t WHERE id = %s\", id)\n",
				"c.py":   "q = f\"insert into t values ({v})\"\n",
				"d.ts":   "const q = `select a from t where b = ${b}`;\n",
				"e.py":   "q = \"update t set a = {}\".format(a)\n",
			},
			types: both,
			want: []cg.ExportedFinding{
				{Rule: "sql-string-concatenation", Category: "SecurityIssues", Severity: "High", Start: 1, End: 1},
				{Rule: "sql-string-concatenation", Category: "SecurityIssues", Severity: "High", Start: 2, End: 2},
				{Rule: "sql-string-concatenation", Category: "SecurityIssues", Severity: "High", Start: 1, End: 1},
				{Rule: "sql-string-concatenation", Category: "SecurityIssues", Severity: "High", Start: 1, End: 1},
				{Rule: "sql-string-concatenation", Category: "SecurityIssues", Severity: "High", Start: 1, End: 1},
			},
		},
		{
			name:  "parameterized sql is fine",
			files: map[string]string{"a.py": "cur.execute('SELECT * FROM t WHERE id = %s', (id,))\nq = 'select 1'\n"},
			types: both,
		},
		{
			name:  "three todo markers",
			files: map[string]string{"a.py": "# TODO a\nx = 1\n# FIXME b\n\n# TODO c\n"},
			types: both,
			want: []cg.ExportedFinding{
				{Rule: "todo-fixme-density", Category: "CodeMaintenanceIssues", Severity: "Low", Start: 1, End: 5},
			},
		},
		{
			name:  "two todo markers are fine",
			files: map[string]string{"a.py": "# TODO a\n# FIXME b\n"},
			types: both,
		},
		{
			name:  "todo rule skips non-code files",
			files: map[string]string{"notes.txt": "TODO a\nTODO b\nTODO c\n"},
			types: both,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, _ := cg.AnalyzeFiles(tt.files, tt.types)

			if len(tt.want) == 0 {
				assert.Empty(t, got)

				return
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestAnalyzerMeteredLines(t *testing.T) {
	t.Parallel()

	_, loc := cg.AnalyzeFiles(map[string]string{
		"a.go":      "package a\n\nfunc f() {}\n",
		"b.py":      "\n\nx = 1\n",
		"README.md": "# docs\nmore docs\n",
	}, []string{"Security", "CodeQuality"})

	assert.Equal(t, int64(3), loc, "only code files count, blank lines do not")
}

func TestAnalyzerDeterministic(t *testing.T) {
	t.Parallel()

	files := map[string]string{
		"z.py": "k = 'AKIAABCDEFGHIJKLMNOP'\n",
		"a.go": "package a\nfunc f() {\n\tf, _ := os.Open(\"x\")\n\t_ = f\n}\n",
		"m.py": "d = open('x')\n",
	}
	types := []string{"Security", "CodeQuality"}

	first, _ := cg.AnalyzeFiles(files, types)
	require.Len(t, first, 3)

	order := make([]string, 0, len(first))
	for _, f := range first {
		order = append(order, f.Rule)
	}

	assert.Equal(t, "unclosed-resource,unclosed-resource,aws-hardcoded-access-key", strings.Join(order, ","))

	for range 20 {
		again, _ := cg.AnalyzeFiles(files, types)
		assert.Equal(t, first, again)
	}
}
