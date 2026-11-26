package main

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func TestSanitizeMarkdownForVitePress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		input      string
		sourceKind string
		want       string
	}{
		{
			name: "escapes vue mustaches and placeholders in prose but keeps inline code and fences",
			input: "Use {{var}} and <region> with `{{kept}}` and " +
				"<a href=\"/x\">link</a>.\n```go\nfmt.Println(\"{{raw}} <tag>\")\n```",
			sourceKind: "guide",
			want: "Use &#123;&#123;var&#125;&#125; and &lt;region&gt; with `{{kept}}` and " +
				"<a href=\"/x\">link</a>.\n```go\nfmt.Println(\"{{raw}} <tag>\")\n```",
		},
		{
			name:       "rewrites guide relative links",
			input:      "See [Docker](docker.md) and [Services](services/) and [Arch](architecture/lambda.md).",
			sourceKind: "guide",
			want:       "See [Docker](/guide/docker) and [Services](/services/) and [Arch](/architecture/lambda).",
		},
		{
			name: "rewrites service guide relative links",
			input: "See [DNS](../architecture/dns.md) and " +
				"[Audit](../../services/s3/PARITY.md) and [All](../../README.md#services).",
			sourceKind: "service",
			want: "See [DNS](/architecture/dns) and " +
				"[Audit](https://github.com/BlackbirdWorks/gopherstack/blob/main/services/s3/PARITY.md) " +
				"and [All](/services/).",
		},
		{
			name:       "rewrites arch links",
			input:      "Back to [Overview](README.md) or [DNS](dns.md).",
			sourceKind: "arch",
			want:       "Back to [Overview](/architecture/) or [DNS](/architecture/dns).",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := sanitizeMarkdownForVitePress(tc.input, tc.sourceKind)
			assert.Equal(t, tc.want, got)
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
			ItemsStillOpen: []string{"TagQueue error shape"},
			StructuralGaps: []string{"Cross-account KMS"},
		},
		"azureblob":  {Service: "azureblob", Overall: "A"},
		"gcpstorage": {Service: "gcpstorage", Overall: "A"},
	}

	got := buildSiteServiceRecords(entries, docBySlug)
	require.Len(t, got, 4)

	assert.Equal(t, "azureblob", got[0].Slug)
	assert.Equal(t, cloudAzure, got[0].Cloud)

	assert.Equal(t, "gcpstorage", got[1].Slug)
	assert.Equal(t, cloudGCP, got[1].Cloud)

	assert.Equal(t, "qldb", got[2].Slug)
	assert.Equal(t, cloudAWS, got[2].Cloud)
	assert.True(t, got[2].Removed)
	assert.Equal(t, gradeRemoved, got[2].Grade)

	assert.Equal(t, "sqs", got[3].Slug)
	assert.Equal(t, cloudAWS, got[3].Cloud)
	assert.False(t, got[3].Removed)
	assert.Equal(t, "A", got[3].Grade)
	assert.Equal(t, 2, got[3].OpsTotal)
	assert.Equal(t, 1, got[3].OpsOK)
	assert.Equal(t, 1, got[3].OpsPartial)
	assert.Equal(t, 1, got[3].KnownGapsCount)
	assert.Equal(t, 1, got[3].StructuralGapsCount)
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
			ItemsStillOpen: []string{"Missing {{bucket}} ACL rule for <account>"},
			StructuralGaps: []string{"Real CloudFront edge POPs"},
		},
	}

	got := renderLimitationsPage(entries, docBySlug)
	assert.Contains(t, got, "# Limitations & Parity Gaps")
	assert.Contains(t, got, "### [S3](/services/s3#known-gaps) (Grade B — 1 gap)")
	assert.Contains(t, got, "Missing &#123;&#123;bucket&#125;&#125; ACL rule for &lt;account&gt;")
	assert.Contains(t, got, "[S3](/services/s3#structural-gaps)")
	assert.Contains(t, got, "[QLDB](/services/qldb)")
}

func TestGenerateSiteIntegration(t *testing.T) {
	// Not parallel because generateSite resolves repo-relative paths (assets/, docs/, examples/).
	wd, err := os.Getwd()
	require.NoError(t, err)

	repoRoot := filepath.Clean(filepath.Join(wd, "..", ".."))
	if _, statErr := os.Stat(filepath.Join(repoRoot, logoSrcPath)); statErr != nil {
		t.Skip("repo root assets not found from working directory")
	}

	t.Chdir(repoRoot)

	outDir := filepath.Join(t.TempDir(), "site")
	log := logger.NewLogger(slog.LevelWarn)

	slugs, err := discoverServiceSlugs(servicesDirName)
	require.NoError(t, err)

	entries, docs, _, err := collectServices(slugs, false)
	require.NoError(t, err)

	err = generateSite(t.Context(), log, outDir, "v1.99.0", entries, docs)
	require.NoError(t, err)

	expectedFiles := []string{
		filepath.Join(outDir, "public", "logo.png"),
		filepath.Join(outDir, "public", "favicon.png"),
		filepath.Join(outDir, ".vitepress", "generated", "stats.json"),
		filepath.Join(outDir, ".vitepress", "generated", "services.json"),
		filepath.Join(outDir, ".vitepress", "generated", "sidebar.json"),
		filepath.Join(outDir, "services", "index.md"),
		filepath.Join(outDir, "services", "s3.md"),
		filepath.Join(outDir, "services", "qldb.md"),
		filepath.Join(outDir, "limitations.md"),
		filepath.Join(outDir, "guide", "quickstart.md"),
		filepath.Join(outDir, "guide", "migration.md"),
		filepath.Join(outDir, "guide", "performance.md"),
		filepath.Join(outDir, "architecture", "index.md"),
		filepath.Join(outDir, "examples", "index.md"),
		filepath.Join(outDir, "examples", "azure-services.md"),
	}

	for _, f := range expectedFiles {
		info, statErr := os.Stat(f)
		require.NoErrorf(t, statErr, "expected generated file %s", f)
		assert.Positive(t, info.Size())
	}

	rawStats, err := os.ReadFile(filepath.Join(outDir, ".vitepress", "generated", "stats.json"))
	require.NoError(t, err)

	var stats siteStats
	require.NoError(t, json.Unmarshal(rawStats, &stats))
	assert.Equal(t, "v1.99.0", stats.Version)
	assert.Positive(t, stats.TotalServices)
	assert.Positive(t, stats.TotalOps)
}
