package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	siteDirMode      = 0o755
	logoSrcPath      = "assets/logo.png"
	faviconSrcPath   = "dashboard/static/favicon.png"
	examplesDirName  = "examples"
	docsDirName      = "docs"
	docsArchDirName  = "docs/architecture"
	routeLimitations = "/limitations"
)

type siteStats struct {
	Version         string `json:"version"`
	GoVersion       string `json:"goVersion"`
	TotalServices   int    `json:"totalServices"`
	AuditedServices int    `json:"auditedServices"`
	RemovedServices int    `json:"removedServices"`
	TotalOps        int    `json:"totalOps"`
	TotalFamilies   int    `json:"totalFamilies"`
	GradeA          int    `json:"gradeA"`
	GradeB          int    `json:"gradeB"`
	OtherGrades     int    `json:"otherGrades"`
	CategoryCount   int    `json:"categoryCount"`
	ExampleCount    int    `json:"exampleCount"`
}

type sidebarLink struct {
	Text string `json:"text"`
	Link string `json:"link"`
}

type sidebarGroup struct {
	Text      string        `json:"text"`
	Items     []sidebarLink `json:"items"`
	Collapsed bool          `json:"collapsed,omitempty"`
}

type siteExample struct {
	Slug      string
	Purpose   string
	Requires  string
	HasReadme bool
}

type docPageSpec struct {
	Src   string
	Dest  string
	Title string
	Kind  string
}

// runSite generates the VitePress documentation site sources under siteDir
// without mutating tracked repository docs.
func runSite(ctx context.Context, log *slog.Logger, siteDir, version string) error {
	slugs, err := discoverServiceSlugs(servicesDirName)
	if err != nil {
		return err
	}

	entries, docs, _, err := collectServices(slugs, false)
	if err != nil {
		return err
	}

	logParseWarnings(ctx, log, docs)
	if warnErr := checkParseWarnings(docs); warnErr != nil {
		return warnErr
	}

	return generateSite(ctx, log, siteDir, version, entries, docs)
}

// generateSite writes all generated assets, JSON data, guides, service pages,
// limitations, and examples into siteDir.
func generateSite(
	ctx context.Context,
	log *slog.Logger,
	siteDir, version string,
	entries []summaryEntry,
	docs []*ParityDoc,
) error {
	cleanSiteDir := filepath.Clean(siteDir)

	docBySlug := make(map[string]*ParityDoc, len(docs))
	for _, d := range docs {
		docBySlug[d.Service] = d
	}

	examples, err := discoverExamples(examplesDirName)
	if err != nil {
		return err
	}

	if assetErr := copySiteAssets(cleanSiteDir); assetErr != nil {
		return assetErr
	}

	records := buildSiteServiceRecords(entries, docBySlug)
	if metaErr := writeSiteMetadata(cleanSiteDir, version, entries, docs, records, examples); metaErr != nil {
		return metaErr
	}

	if svcErr := writeServicePages(cleanSiteDir, entries, docBySlug); svcErr != nil {
		return svcErr
	}

	if limErr := writeLimitationsPage(cleanSiteDir, entries, docBySlug); limErr != nil {
		return limErr
	}

	if guideErr := writeGuideAndArchPages(cleanSiteDir); guideErr != nil {
		return guideErr
	}

	if exErr := writeExamplesPages(cleanSiteDir, examples); exErr != nil {
		return exErr
	}

	log.InfoContext(ctx, "vitepress site generated",
		"site_dir", cleanSiteDir,
		"services", len(entries),
		"examples", len(examples),
		"version", version,
	)

	return nil
}

// copySiteAssets copies the Gopherstack logo and favicon into site/public/.
func copySiteAssets(siteDir string) error {
	publicDir := filepath.Join(siteDir, "public")
	if err := os.MkdirAll(publicDir, siteDirMode); err != nil {
		return fmt.Errorf("mkdir %s: %w", publicDir, err)
	}

	assets := []struct {
		src  string
		dest string
	}{
		{logoSrcPath, filepath.Join(publicDir, "logo.png")},
		{faviconSrcPath, filepath.Join(publicDir, "favicon.png")},
	}

	for _, a := range assets {
		data, err := os.ReadFile(filepath.Clean(a.src))
		if err != nil {
			return fmt.Errorf("read %s: %w", a.src, err)
		}
		if writeErr := writeFileAtomic(a.dest, data); writeErr != nil {
			return writeErr
		}
	}

	return nil
}

// writeSiteMetadata writes stats.json, services.json, and sidebar.json into
// <siteDir>/.vitepress/generated/.
func writeSiteMetadata(
	siteDir, version string,
	entries []summaryEntry,
	docs []*ParityDoc,
	records []siteServiceRecord,
	examples []siteExample,
) error {
	genDir := filepath.Join(siteDir, ".vitepress", "generated")
	if err := os.MkdirAll(genDir, siteDirMode); err != nil {
		return fmt.Errorf("mkdir %s: %w", genDir, err)
	}

	goVer, err := readGoVersion(goModPath)
	if err != nil {
		return err
	}

	stats := buildSiteStats(version, goVer, entries, docs, examples)
	if writeErr := writeJSONFile(filepath.Join(genDir, "stats.json"), stats); writeErr != nil {
		return writeErr
	}

	if writeErr := writeJSONFile(filepath.Join(genDir, "services.json"), records); writeErr != nil {
		return writeErr
	}

	sidebar := buildSidebarConfig(records, examples)

	return writeJSONFile(filepath.Join(genDir, "sidebar.json"), sidebar)
}

func buildSiteStats(
	version, goVer string,
	entries []summaryEntry,
	docs []*ParityDoc,
	examples []siteExample,
) siteStats {
	totalOps := 0
	totalFamilies := 0
	countA := 0
	countB := 0
	countOther := 0

	for _, doc := range docs {
		totalOps += len(doc.Ops)
		totalFamilies += len(doc.Families)
		switch normalizeGrade(doc.Overall) {
		case gradeA:
			countA++
		case gradeB:
			countB++
		default:
			countOther++
		}
	}

	return siteStats{
		Version:         strings.TrimSpace(version),
		GoVersion:       goVer,
		TotalServices:   len(entries),
		AuditedServices: len(docs),
		RemovedServices: len(entries) - len(docs),
		TotalOps:        totalOps,
		TotalFamilies:   totalFamilies,
		GradeA:          countA,
		GradeB:          countB,
		OtherGrades:     countOther,
		CategoryCount:   len(categoryGroups()),
		ExampleCount:    len(examples),
	}
}

func buildSidebarConfig(records []siteServiceRecord, examples []siteExample) map[string][]sidebarGroup {
	guideSidebar := []sidebarGroup{
		{
			Text: "Getting Started",
			Items: []sidebarLink{
				{Text: "Quickstart", Link: "/guide/quickstart"},
				{Text: "Docker & Compose", Link: "/guide/docker"},
				{Text: "Performance", Link: "/guide/performance"},
				{Text: "Migrating from LocalStack", Link: "/guide/migration"},
				{Text: "Limitations & Parity Gaps", Link: routeLimitations},
			},
		},
		{
			Text: "Configuration & Features",
			Items: []sidebarLink{
				{Text: "Engine Modes", Link: "/guide/engines"},
				{Text: "Chaos Testing", Link: "/guide/chaos"},
				{Text: "App Inspector", Link: "/guide/app-inspector"},
				{Text: "IAM Policy Stream", Link: "/guide/iam-policy-stream"},
				{Text: "Pinned Resource IDs", Link: "/guide/pinned-ids"},
			},
		},
	}

	archSidebar := []sidebarGroup{
		{
			Text: "Architecture",
			Items: []sidebarLink{
				{Text: "Overview", Link: "/architecture/"},
				{Text: "Embedded DNS", Link: "/architecture/dns"},
				{Text: "ElastiCache Engine Modes", Link: "/architecture/elasticache"},
				{Text: "Lambda Runtime", Link: "/architecture/lambda"},
			},
		},
	}

	exampleItems := []sidebarLink{{Text: "Overview", Link: "/examples/"}}
	for _, ex := range examples {
		if ex.HasReadme {
			exampleItems = append(exampleItems, sidebarLink{
				Text: ex.Slug,
				Link: "/examples/" + ex.Slug,
			})
		}
	}
	examplesSidebar := []sidebarGroup{{Text: "Examples", Items: exampleItems}}

	return map[string][]sidebarGroup{
		"/guide/":        guideSidebar,
		routeLimitations: guideSidebar,
		"/architecture/": archSidebar,
		"/examples/":     examplesSidebar,
		"/services/":     buildServicesSidebar(records),
	}
}

func buildServicesSidebar(records []siteServiceRecord) []sidebarGroup {
	byCat := make(map[string][]sidebarLink)
	for _, r := range records {
		byCat[r.Category] = append(byCat[r.Category], sidebarLink{
			Text: r.DisplayName,
			Link: "/services/" + r.Slug,
		})
	}

	groups := []sidebarGroup{
		{
			Text: "Services",
			Items: []sidebarLink{
				{Text: "Service Index", Link: "/services/"},
				{Text: "Limitations & Gaps", Link: routeLimitations},
			},
		},
	}

	for _, cat := range categoryOrder() {
		items := byCat[cat]
		if len(items) == 0 {
			continue
		}
		groups = append(groups, sidebarGroup{
			Text:      cat,
			Collapsed: true,
			Items:     items,
		})
	}

	return groups
}

func writeJSONFile(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", path, err)
	}
	data = append(data, '\n')

	return writeFileAtomic(path, data)
}

// writeGuideAndArchPages copies and transforms repository guide and architecture
// docs into <siteDir>/guide/ and <siteDir>/architecture/, plus writes performance.md.
func writeGuideAndArchPages(siteDir string) error {
	guideDir := filepath.Join(siteDir, "guide")
	archDir := filepath.Join(siteDir, "architecture")
	for _, d := range []string{guideDir, archDir} {
		if err := os.MkdirAll(d, siteDirMode); err != nil {
			return fmt.Errorf("mkdir %s: %w", d, err)
		}
	}

	specs := []docPageSpec{
		{
			filepath.Join(docsDirName, "quickstart.md"),
			filepath.Join(guideDir, "quickstart.md"),
			"Quickstart",
			kindGuide,
		},
		{
			filepath.Join(docsDirName, "docker.md"),
			filepath.Join(guideDir, "docker.md"),
			"Docker & Docker Compose",
			kindGuide,
		},
		{
			filepath.Join(docsDirName, "migration.md"),
			filepath.Join(guideDir, "migration.md"),
			"Migrating from LocalStack",
			kindGuide,
		},
		{filepath.Join(docsDirName, "engines.md"), filepath.Join(guideDir, "engines.md"), "Engine Modes", kindGuide},
		{filepath.Join(docsDirName, "chaos.md"), filepath.Join(guideDir, "chaos.md"), "Chaos Testing", kindGuide},
		{
			filepath.Join(docsDirName, "app-inspector.md"),
			filepath.Join(guideDir, "app-inspector.md"),
			"App Inspector",
			kindGuide,
		},
		{
			filepath.Join(docsDirName, "iam-policy-stream.md"),
			filepath.Join(guideDir, "iam-policy-stream.md"),
			"IAM Policy Stream",
			kindGuide,
		},
		{
			filepath.Join(docsDirName, "pinned-ids.md"),
			filepath.Join(guideDir, "pinned-ids.md"),
			"Pinned Resource IDs",
			kindGuide,
		},
		{
			filepath.Join(docsArchDirName, "README.md"),
			filepath.Join(archDir, "index.md"),
			"Architecture Overview",
			kindArch,
		},
		{filepath.Join(docsArchDirName, "dns.md"), filepath.Join(archDir, "dns.md"), "Embedded DNS", kindArch},
		{
			filepath.Join(docsArchDirName, "elasticache.md"),
			filepath.Join(archDir, "elasticache.md"),
			"ElastiCache Engine Modes",
			kindArch,
		},
		{filepath.Join(docsArchDirName, "lambda.md"), filepath.Join(archDir, "lambda.md"), "Lambda Runtime", kindArch},
	}

	for _, spec := range specs {
		if err := copyTransformedDoc(spec); err != nil {
			return err
		}
	}

	return writeFileAtomic(filepath.Join(guideDir, "performance.md"), []byte(renderPerformancePage()))
}

func copyTransformedDoc(spec docPageSpec) error {
	raw, err := os.ReadFile(filepath.Clean(spec.Src))
	if err != nil {
		return fmt.Errorf("read %s: %w", spec.Src, err)
	}

	body := sanitizeMarkdownForVitePress(string(raw), spec.Kind)
	title := extractMarkdownTitle(string(raw), spec.Title)
	out := "---\ntitle: " + escapeYAMLString(title) + "\n---\n\n" + body

	return writeFileAtomic(spec.Dest, []byte(out))
}

// renderPerformancePage builds the curated Performance & Benchmarks page.
func renderPerformancePage() string {
	var b strings.Builder

	b.WriteString("---\ntitle: Performance\n")
	b.WriteString("description: How Gopherstack achieves sub-100 ms cold starts and ~30 MB memory footprint in Go.\n")
	b.WriteString("---\n\n")
	b.WriteString("# Performance\n\n")
	b.WriteString("Gopherstack is written in pure Go as a single binary with in-memory backends, " +
		"designed for fast local iteration and parallel CI test suites.\n\n")
	b.WriteString("## At a Glance\n\n")
	b.WriteString("| Metric | LocalStack | Gopherstack |\n| --- | --- | --- |\n")
	b.WriteString("| **Runtime** | Python | Single Go binary |\n")
	b.WriteString("| **Memory footprint** | 300 MB+ | ~30 MB |\n")
	b.WriteString("| **Cold start** | 5–30 s | <100 ms |\n")
	b.WriteString("| **Service startup** | Lazy / per-service init | All services ready immediately |\n\n")
	b.WriteString("## Engineering Optimizations\n\n")
	b.WriteString(
		"- **O(1) ARN indexing** — Tagging and resource lookups use a centralized ARN index so operations " +
			"stay constant-time even with thousands of resources.\n",
	)
	b.WriteString(
		"- **Memory optimization** — Struct field alignment (`fieldalignment`) and compact in-memory data " +
			"structures minimize allocations and GC pressure.\n",
	)
	b.WriteString(
		"- **Profile-Guided Optimization (PGO)** — Release binaries are built with `default.pgo` captured " +
			"from real multi-service workloads.\n",
	)
	b.WriteString(
		"- **Runtime response compression** — Dashboard assets and JSON responses (>= 1 KiB) negotiate " +
			"`zstd`, `br`, or `gzip`, with embedded static assets compressed once and cached in memory.\n\n",
	)
	b.WriteString("## Running Benchmarks\n\n")
	b.WriteString("In-process service benchmarks live under `bench/` and per-service `*_bench_test.go` files:\n\n")
	b.WriteString("```bash\n# Run all Go benchmarks across the repository\ntask bench\n\n")
	b.WriteString("# Run core backend benchmarks (S3, SQS, DynamoDB, KMS, Secrets Manager)\n")
	b.WriteString("go test -bench=. -benchmem ./bench/\n")
	b.WriteString("```\n")

	return b.String()
}

// discoverExamples inspects examples/*/ and extracts descriptions from examples/README.md.
func discoverExamples(dir string) ([]siteExample, error) {
	meta := parseExamplesTable(filepath.Join(dir, readmeFileName))

	dirEntries, err := os.ReadDir(filepath.Clean(dir))
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", dir, err)
	}

	examples := make([]siteExample, 0, len(dirEntries))
	for _, entry := range dirEntries {
		if !entry.IsDir() {
			continue
		}

		slug := entry.Name()
		_, readmeErr := os.Stat(filepath.Join(dir, slug, readmeFileName))
		info := meta[slug]
		if info.Purpose == "" {
			info.Purpose = defaultExamplePurpose(slug)
		}
		if info.Requires == "" {
			info.Requires = "Docker Compose, Bash"
		}

		examples = append(examples, siteExample{
			Slug:      slug,
			Purpose:   info.Purpose,
			Requires:  info.Requires,
			HasReadme: readmeErr == nil,
		})
	}

	sort.Slice(examples, func(i, j int) bool {
		return examples[i].Slug < examples[j].Slug
	})

	return examples, nil
}

func defaultExamplePurpose(slug string) string {
	return "Runnable end-to-end example for `" + slug + "`."
}

// parseExamplesTable parses the "| `slug` | Purpose | Tools Required |" table
// from examples/README.md so the generated site stays in sync with it.
func parseExamplesTable(readmePath string) map[string]siteExample {
	out := make(map[string]siteExample)
	raw, err := os.ReadFile(filepath.Clean(readmePath))
	if err != nil {
		return out
	}

	for line := range strings.SplitSeq(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "|") {
			continue
		}
		cols := strings.Split(strings.Trim(trimmed, "|"), "|")
		const expectedCols = 3
		if len(cols) < expectedCols {
			continue
		}
		slug := strings.TrimSpace(cols[0])
		slug = strings.Trim(slug, "*` ")
		if slug == "" || slug == "Example" || strings.HasPrefix(slug, "-") {
			continue
		}
		out[slug] = siteExample{
			Slug:     slug,
			Purpose:  strings.TrimSpace(cols[1]),
			Requires: strings.TrimSpace(cols[2]),
		}
	}

	return out
}

// writeExamplesPages writes site/examples/index.md and per-example pages for
// every example that ships a README.md.
func writeExamplesPages(siteDir string, examples []siteExample) error {
	exOutDir := filepath.Join(siteDir, examplesDirName)
	if err := os.MkdirAll(exOutDir, siteDirMode); err != nil {
		return fmt.Errorf("mkdir %s: %w", exOutDir, err)
	}

	indexContent := renderExamplesIndexPage(examples)
	if err := writeFileAtomic(filepath.Join(exOutDir, "index.md"), []byte(indexContent)); err != nil {
		return err
	}

	for _, ex := range examples {
		if !ex.HasReadme {
			continue
		}
		if err := writeSingleExamplePage(exOutDir, ex); err != nil {
			return err
		}
	}

	return nil
}

func writeSingleExamplePage(exOutDir string, ex siteExample) error {
	srcPath := filepath.Clean(filepath.Join(examplesDirName, ex.Slug, readmeFileName))
	raw, err := os.ReadFile(srcPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", srcPath, err)
	}

	title := extractMarkdownTitle(string(raw), ex.Slug)
	body := stripLeadingH1(string(raw))
	body = sanitizeMarkdownForVitePress(body, "example")

	var b strings.Builder
	b.WriteString("---\ntitle: " + escapeYAMLString(title) + "\n")
	b.WriteString("description: " + escapeYAMLString(ex.Purpose) + "\n---\n\n")
	b.WriteString("# " + title + "\n\n")
	fmt.Fprintf(&b, "[View source on GitHub](%s/tree/main/examples/%s) · [All Examples](/examples/)\n\n",
		githubRepoBase, ex.Slug)
	b.WriteString(strings.TrimSpace(body) + "\n")

	return writeFileAtomic(filepath.Join(exOutDir, ex.Slug+".md"), []byte(b.String()))
}

func renderExamplesIndexPage(examples []siteExample) string {
	var b strings.Builder

	b.WriteString("---\ntitle: Examples\n")
	b.WriteString("description: Runnable end-to-end multi-service examples for Gopherstack.\n---\n\n")
	b.WriteString("# Examples\n\n")
	b.WriteString("Complete, runnable examples live in [`examples/`](" + githubRepoBase + "/tree/main/examples) " +
		"— demonstrating serverless workflows, event-driven pipelines, OpenTofu provisioning, " +
		"Docker-backed EC2/Lambda, ElastiCache Valkey, and Azure services.\n\n")
	b.WriteString("## Running an Example\n\n")
	b.WriteString(
		"```bash\ncd examples/kinesis-lambda-aggregator\n" +
			"docker compose up -d\n./demo.sh\n\n# Tear down when finished\ndocker compose down -v\n```\n\n",
	)
	b.WriteString("## Available Examples\n\n")
	b.WriteString("| Example | Purpose | Tools Required | Source |\n")
	b.WriteString("| --- | --- | --- | --- |\n")

	for _, ex := range examples {
		nameCell := "`" + ex.Slug + "`"
		if ex.HasReadme {
			nameCell = "[`" + ex.Slug + "`](/examples/" + ex.Slug + ")"
		}
		ghLink := fmt.Sprintf("[GitHub](%s/tree/main/examples/%s)", githubRepoBase, ex.Slug)
		fmt.Fprintf(&b, "| %s | %s | %s | %s |\n",
			nameCell,
			sanitizeInlineProse(ex.Purpose),
			sanitizeInlineProse(ex.Requires),
			ghLink,
		)
	}

	return b.String()
}
