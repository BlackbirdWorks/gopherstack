package main

import (
	"encoding/json/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSanitizeMarkdownForVitePress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		input  string
		srcDir string
		want   string
	}{
		{
			name: "escapes vue syntax in prose only",
			input: "Use {{var}} and <region> with `{{kept}}` and " +
				"<a href=\"/x\">link</a>.\n```go\nfmt.Println(\"{{raw}} <tag>\")\n```",
			srcDir: "docs",
			want: "Use &#123;&#123;var&#125;&#125; and &lt;region&gt; with `{{kept}}` and " +
				"<a href=\"/x\">link</a>.\n```go\nfmt.Println(\"{{raw}} <tag>\")\n```",
		},
		{
			name:   "guide links",
			input:  "See [Docker](docker.md) and [Services](services/) and [Arch](architecture/lambda.md).",
			srcDir: "docs",
			want:   "See [Docker](/guide/docker) and [Services](/services/) and [Arch](/architecture/lambda).",
		},
		{
			name: "service guide links",
			input: "See [DNS](../architecture/dns.md) and " +
				"[Audit](../../services/s3/PARITY.md) and [All](../../README.md#services).",
			srcDir: "docs/services",
			want: "See [DNS](/architecture/dns) and " +
				"[Audit](https://github.com/BlackbirdWorks/gopherstack/blob/main/services/s3/PARITY.md) " +
				"and [All](/services/).",
		},
		{
			name:   "arch links",
			input:  "Back to [Overview](README.md) or [DNS](dns.md).",
			srcDir: "docs/architecture",
			want:   "Back to [Overview](/architecture/) or [DNS](/architecture/dns).",
		},
		{
			name:   "inline code keeps link text",
			input:  "Write `[Docker](docker.md)` as [Docker](docker.md).",
			srcDir: "docs",
			want:   "Write `[Docker](docker.md)` as [Docker](/guide/docker).",
		},
		{
			name:   "code in link label",
			input:  "[`docker.md`](docker.md)",
			srcDir: "docs",
			want:   "[`docker.md`](/guide/docker)",
		},
		{
			name:   "longer fence holds shorter fence",
			input:  "````md\n```\n{{raw}}\n```\n{{still}}\n````\n{{out}}",
			srcDir: "docs",
			want:   "````md\n```\n{{raw}}\n```\n{{still}}\n````\n&#123;&#123;out&#125;&#125;",
		},
		{
			name:   "closing fence with trailing text does not close",
			input:  "```\n``` not closing\n{{raw}}\n```\n{{out}}",
			srcDir: "docs",
			want:   "```\n``` not closing\n{{raw}}\n```\n&#123;&#123;out&#125;&#125;",
		},
		{
			name:   "tilde fence",
			input:  "~~~\n{{raw}}\n~~~\n{{out}}",
			srcDir: "docs",
			want:   "~~~\n{{raw}}\n~~~\n&#123;&#123;out&#125;&#125;",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := sanitizeMarkdownForVitePress(tc.input, tc.srcDir)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestResolveSiteLink(t *testing.T) {
	t.Parallel()

	const gh = "https://github.com/BlackbirdWorks/gopherstack"

	tests := []struct {
		name   string
		target string
		srcDir string
		want   string
	}{
		{"arch to guide", "../kubernetes.md", "docs/architecture", "/guide/kubernetes"},
		{"arch to guide anchor", "../engines.md#auto", "docs/architecture", "/guide/engines#auto"},
		{"arch to arch", "lambda.md#runtime", "docs/architecture", "/architecture/lambda#runtime"},
		{"arch to unlisted guide", "../history/x.md", "docs/architecture", gh + "/blob/main/docs/history/x.md"},
		{"arch to service guide", "../services/s3.md", "docs/architecture", "/services/s3"},
		{"arch to source", "../../Taskfile.yml", "docs/architecture", gh + "/blob/main/Taskfile.yml"},
		{"guide to arch dir", "architecture/", "docs", "/architecture/"},
		{"guide to service guide", "services/s3.md", "docs", "/services/s3"},
		{"guide to repo file", "../Taskfile.yml", "docs", gh + "/blob/main/Taskfile.yml"},
		{"guide to repo dir", "../examples/", "docs", "/examples/"},
		{"service to guide", "../chaos.md", "docs/services", "/guide/chaos"},
		{"service to service", "sqs.md", "docs/services", "/services/sqs"},
		{"service to service readme", "../../services/sqs/README.md", "docs/services", "/services/sqs"},
		{"service to docs dir", "../../docs/services/sqs.md", "docs/services", "/services/sqs"},
		{"removed readme to guide", "../../docs/migration.md", "services/qldb", "/guide/migration"},
		{"removed readme sibling", "notes.txt", "services/qldb", gh + "/blob/main/services/qldb/notes.txt"},
		{"example to guide", "../../docs/quickstart.md#start", "examples/s3-access-logs", "/guide/quickstart#start"},
		{"example to example", "../sns-sqs-fanout/README.md", "examples/s3-access-logs", "/examples/sns-sqs-fanout"},
		{"example to script", "demo.sh", "examples/s3-access-logs", gh + "/blob/main/examples/s3-access-logs/demo.sh"},
		{"external", "https://example.com/a.md", "docs", "https://example.com/a.md"},
		{"anchor", "#top", "docs", "#top"},
		{"escapes repo", "../../../etc/passwd", "docs", "../../../etc/passwd"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target := tc.target
			if isExternalOrAnchor(target) {
				assert.Equal(t, tc.want, target)

				return
			}

			assert.Equal(t, tc.want, resolveSiteLink(target, tc.srcDir))
		})
	}
}

func TestBuildSiteServiceRecords(t *testing.T) {
	t.Parallel()

	entries := []summaryEntry{
		{Slug: "sqs", DisplayName: "SQS", Category: "Messaging & Integration"},
		{Slug: "qldb", DisplayName: "QLDB", Category: "Database"},
		{Slug: "azureblob", DisplayName: "Azure Blob Storage", Category: "Azure"},
		{Slug: "gcpstorage", DisplayName: "Google Cloud Storage", Category: "GCP"},
	}

	docBySlug := map[string]*ParityDoc{
		"sqs": {
			Service:       "sqs",
			Overall:       "A",
			Protocol:      "awsQuery",
			SDKModule:     "github.com/aws/aws-sdk-go-v2/service/sqs",
			LastAuditDate: "2026-08-01",
			LeaksStatus:   "none",
			Ops: []OpStatus{
				{Name: "CreateQueue", Wire: "ok", Errors: "ok", State: "ok", Persist: "ok"},
				{Name: "TagQueue", Wire: "ok", Errors: "partial", State: "ok", Persist: "ok"},
			},
			Gaps:           []string{"legacy note: nothing is missing"},
			ItemsStillOpen: []string{"TagQueue error shape"},
			StructuralGaps: []string{"Cross-account KMS"},
		},
		"azureblob":  {Service: "azureblob", Overall: "A"},
		"gcpstorage": {Service: "gcpstorage", Overall: "A"},
	}

	got := buildSiteServiceRecords(entries, docBySlug)
	require.Len(t, got, 4)

	tests := []struct {
		check func(t *testing.T, r siteServiceRecord)
		name  string
		index int
	}{
		{check: func(t *testing.T, r siteServiceRecord) {
			t.Helper()
			assert.Equal(t, "azureblob", r.Slug)
			assert.Equal(t, cloudAzure, r.Cloud)
		}, name: "azure cloud sorts first", index: 0},
		{check: func(t *testing.T, r siteServiceRecord) {
			t.Helper()
			assert.Equal(t, "gcpstorage", r.Slug)
			assert.Equal(t, cloudGCP, r.Cloud)
		}, name: "gcp cloud", index: 1},
		{check: func(t *testing.T, r siteServiceRecord) {
			t.Helper()
			assert.Equal(t, "qldb", r.Slug)
			assert.Equal(t, cloudAWS, r.Cloud)
			assert.True(t, r.Removed)
			assert.Equal(t, gradeRemoved, r.Grade)
		}, name: "removed service", index: 2},
		{check: func(t *testing.T, r siteServiceRecord) {
			t.Helper()
			assert.Equal(t, "sqs", r.Slug)
			assert.False(t, r.Removed)
			assert.Equal(t, "A", r.Grade)
			assert.Equal(t, 2, r.OpsTotal)
			assert.Equal(t, 1, r.OpsOK)
			assert.Equal(t, 1, r.OpsPartial)
			assert.Equal(t, 1, r.KnownGapsCount, "legacy gaps: must not count as open")
			assert.Equal(t, 1, r.StructuralGapsCount)
		}, name: "audited service counts", index: 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.check(t, got[tc.index])
		})
	}
}

func TestRenderLimitationsPage(t *testing.T) {
	t.Parallel()

	entries := []summaryEntry{
		{Slug: "s3", DisplayName: "S3", Category: "Storage"},
		{Slug: "qldb", DisplayName: "QLDB", Category: "Database"},
	}
	docBySlug := map[string]*ParityDoc{
		"s3": {
			Service:        "s3",
			Overall:        "B",
			Gaps:           []string{"legacy note"},
			ItemsStillOpen: []string{"Missing {{bucket}} ACL rule for <account>"},
			StructuralGaps: []string{"Real CloudFront edge POPs"},
		},
	}

	got := renderLimitationsPage(entries, docBySlug)

	tests := []struct {
		name    string
		want    string
		wantNot string
	}{
		{name: "title", want: "# Limitations & Parity Gaps"},
		{name: "warning alert", want: "> [!WARNING]"},
		{name: "gap heading", want: "### [S3](/services/s3#known-gaps) (Grade B — 1 gap)"},
		{name: "escaped item", want: "Missing &#123;&#123;bucket&#125;&#125; ACL rule for &lt;account&gt;"},
		{name: "structural link", want: "[S3](/services/s3#structural-gaps)"},
		{name: "removed link", want: "[QLDB](/services/qldb)"},
		{name: "legacy gaps hidden", wantNot: "legacy note"},
		{name: "no false lambda claim", wantNot: "/_aws/lambda/runtimes"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.want != "" {
				assert.Contains(t, got, tc.want)
			}
			if tc.wantNot != "" {
				assert.NotContains(t, got, tc.wantNot)
			}
		})
	}
}

func TestValidateSiteDir(t *testing.T) {
	// Not parallel: validateSiteDir resolves repo-relative source paths.
	repoRoot := repoRootDir(t)
	t.Chdir(repoRoot)

	link := filepath.Join(t.TempDir(), "docs-link")
	require.NoError(t, os.Symlink(filepath.Join(repoRoot, "docs"), link))

	tests := []struct {
		name    string
		dir     string
		wantErr bool
	}{
		{"site dir", "site", false},
		{"temp dir", filepath.Join(t.TempDir(), "site"), false},
		{"docs", "docs", true},
		{"docs architecture", "docs/architecture", true},
		{"examples", "examples", true},
		{"services", "services", true},
		{"repo root", ".", true},
		{"parent of repo", "..", true},
		{"symlink to docs", link, true},
		{"below symlink to docs", filepath.Join(link, "x"), true},
	}

	for _, tc := range tests { //nolint:paralleltest // t.Chdir forbids parallel
		t.Run(tc.name, func(t *testing.T) {
			err := validateSiteDir(tc.dir)
			if tc.wantErr {
				require.ErrorIs(t, err, errSiteDirOverlap)

				return
			}

			require.NoError(t, err)
		})
	}
}

func repoRootDir(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	require.NoError(t, err)

	repoRoot := filepath.Clean(filepath.Join(wd, "..", ".."))
	if _, statErr := os.Stat(filepath.Join(repoRoot, logoSrcPath)); statErr != nil {
		t.Skip("repo root assets not found from working directory")
	}

	return repoRoot
}

func TestGenerateSiteIntegration(t *testing.T) {
	// Not parallel: generateSite resolves repo-relative paths (assets/, docs/, examples/).
	t.Chdir(repoRootDir(t))

	slugs, err := discoverServiceSlugs(servicesDirName)
	require.NoError(t, err)

	entries, docs, _, err := collectServices(slugs, false)
	require.NoError(t, err)

	tests := []struct {
		name      string
		stale     []string
		wantFiles []string
	}{
		{
			name:  "fresh output",
			stale: nil,
			wantFiles: []string{
				"public/logo.png",
				"public/favicon.png",
				".vitepress/generated/stats.json",
				".vitepress/generated/services.json",
				".vitepress/generated/sidebar.json",
				"services/index.md",
				"services/s3.md",
				"services/qldb.md",
				"limitations.md",
				"guide/quickstart.md",
				"guide/migration.md",
				"guide/configuration.md",
				"guide/cli.md",
				"guide/performance.md",
				"architecture/index.md",
				"examples/index.md",
				"examples/azure-services.md",
			},
		},
		{
			name:      "stale pages removed",
			stale:     []string{"examples/deleted.md", "services/gone.md", "guide/old.md", "architecture/old.md"},
			wantFiles: []string{"services/index.md", "examples/index.md"},
		},
	}

	for _, tc := range tests { //nolint:paralleltest // t.Chdir forbids parallel
		t.Run(tc.name, func(t *testing.T) {
			outDir := filepath.Join(t.TempDir(), "site")
			for _, rel := range tc.stale {
				path := filepath.Join(outDir, rel)
				require.NoError(t, os.MkdirAll(filepath.Dir(path), siteDirMode))
				require.NoError(t, os.WriteFile(path, []byte("stale"), readmeFileMode))
			}

			require.NoError(t, generateSite(t.Context(), outDir, "v1.99.0", entries, docs))

			for _, rel := range tc.wantFiles {
				info, statErr := os.Stat(filepath.Join(outDir, rel))
				require.NoErrorf(t, statErr, "expected generated file %s", rel)
				assert.Positive(t, info.Size())
			}

			for _, rel := range tc.stale {
				_, statErr := os.Stat(filepath.Join(outDir, rel))
				require.ErrorIs(t, statErr, os.ErrNotExist, "stale file %s survived", rel)
			}

			rawStats, readErr := os.ReadFile(filepath.Join(outDir, ".vitepress", "generated", "stats.json"))
			require.NoError(t, readErr)

			var stats siteStats
			require.NoError(t, json.Unmarshal(rawStats, &stats, jsonx.V1()))
			assert.Equal(t, "v1.99.0", stats.Version)
			assert.Equal(t, len(entries), stats.TotalServices)
			assert.Positive(t, stats.TotalOps)
		})
	}
}

func TestGenerateSiteRejectsSourceOverlap(t *testing.T) {
	t.Chdir(repoRootDir(t))

	tests := []struct {
		name string
		dir  string
	}{
		{"docs dir", "docs"},
		{"examples dir", "examples"},
	}

	for _, tc := range tests { //nolint:paralleltest // t.Chdir forbids parallel
		t.Run(tc.name, func(t *testing.T) {
			err := generateSite(t.Context(), tc.dir, "v1", nil, nil)
			require.ErrorIs(t, err, errSiteDirOverlap)
		})
	}
}
