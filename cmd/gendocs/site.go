package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
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
}

type guideFile struct {
	file  string
	title string
}

type archFile struct {
	src   string
	dest  string
	title string
}

func guideFiles() []guideFile {
	return []guideFile{
		{"quickstart.md", "Quickstart"},
		{"docker.md", "Docker & Docker Compose"},
		{"migration.md", "Migrating from LocalStack"},
		{"configuration.md", "Configuration"},
		{"engines.md", "Engine Modes"},
		{"persistence.md", "Persistence"},
		{"chaos.md", "Chaos Testing"},
		{"security-groups.md", "Security Groups"},
		{"app-inspector.md", "App Inspector"},
		{"iam-policy-stream.md", "IAM Policy Stream"},
		{"pinned-ids.md", "Pinned Resource IDs"},
		{"testcontainers.md", "Testcontainers"},
		{"kubernetes.md", "Kubernetes"},
		{"extensions.md", "Extensions"},
		{"mcp.md", "MCP Server"},
		{"replicator.md", "Replicator"},
	}
}

func archFiles() []archFile {
	return []archFile{
		{"README.md", "index.md", "Architecture Overview"},
		{"dns.md", "dns.md", "Embedded DNS"},
		{"elasticache.md", "elasticache.md", "ElastiCache Engine Modes"},
		{"lambda.md", "lambda.md", "Lambda Runtime"},
	}
}

func guideDocNames() []string {
	files := guideFiles()
	names := make([]string, 0, len(files))
	for _, g := range files {
		names = append(names, g.file)
	}

	return names
}

func archDocNames() []string {
	files := archFiles()
	names := make([]string, 0, len(files))
	for _, a := range files {
		names = append(names, a.src)
	}

	return names
}

// runSite generates the VitePress documentation site sources under siteDir
// without mutating tracked repository docs.
func runSite(ctx context.Context, siteDir, version string) error {
	slugs, err := discoverServiceSlugs(servicesDirName)
	if err != nil {
		return err
	}

	entries, docs, _, err := collectServices(slugs, false)
	if err != nil {
		return err
	}

	logParseWarnings(ctx, logger.Load(ctx), docs)
	if warnErr := checkParseWarnings(docs); warnErr != nil {
		return warnErr
	}

	return generateSite(ctx, siteDir, version, entries, docs)
}

// generateSite writes all generated assets, JSON data, guides, service pages,
// limitations, and examples into siteDir.
func generateSite(
	ctx context.Context,
	siteDir, version string,
	entries []summaryEntry,
	docs []*ParityDoc,
) error {
	cleanSiteDir := filepath.Clean(siteDir)
	if err := validateSiteDir(cleanSiteDir); err != nil {
		return err
	}

	docBySlug := make(map[string]*ParityDoc, len(docs))
	for _, d := range docs {
		docBySlug[d.Service] = d
	}

	examples, err := discoverExamples(examplesDirName)
	if err != nil {
		return err
	}

	if cleanErr := removeGeneratedDirs(cleanSiteDir); cleanErr != nil {
		return cleanErr
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

	logger.Load(ctx).InfoContext(ctx, "vitepress site generated",
		"site_dir", cleanSiteDir,
		"services", len(entries),
		"examples", len(examples),
		"version", version,
	)

	return nil
}

var errSiteDirOverlap = errors.New("site output directory overlaps repository sources")

// validateSiteDir rejects output directories that would overwrite or delete
// repository sources the generator reads, including through symlinks.
func validateSiteDir(siteDir string) error {
	site, err := resolveReal(siteDir)
	if err != nil {
		return err
	}

	root, err := resolveReal(".")
	if err != nil {
		return err
	}

	if pathWithin(root, site) {
		return fmt.Errorf("%w: %s contains the repository root", errSiteDirOverlap, siteDir)
	}

	sources := []string{
		docsDirName, examplesDirName, servicesDirName,
		filepath.Dir(logoSrcPath), filepath.Dir(faviconSrcPath),
	}
	for _, src := range sources {
		resolved, resolveErr := resolveReal(src)
		if resolveErr != nil {
			return resolveErr
		}

		if pathWithin(site, resolved) || pathWithin(resolved, site) {
			return fmt.Errorf("%w: %s overlaps %s", errSiteDirOverlap, siteDir, src)
		}
	}

	return nil
}

// resolveReal returns an absolute path with symlinks resolved in its longest
// existing prefix.
func resolveReal(p string) (string, error) {
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", fmt.Errorf("abs %s: %w", p, err)
	}

	existing, rest := abs, ""
	for {
		if _, statErr := os.Lstat(existing); statErr == nil {
			break
		}

		parent := filepath.Dir(existing)
		if parent == existing {
			return abs, nil
		}

		rest = filepath.Join(filepath.Base(existing), rest)
		existing = parent
	}

	resolved, err := filepath.EvalSymlinks(existing)
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", existing, err)
	}

	return filepath.Join(resolved, rest), nil
}

// pathWithin reports whether child equals parent or lies beneath it.
func pathWithin(child, parent string) bool {
	rel, err := filepath.Rel(parent, child)

	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// removeGeneratedDirs deletes the generator-owned page trees so pages whose
// sources disappeared do not survive regeneration.
func removeGeneratedDirs(siteDir string) error {
	for _, name := range []string{"services", "guide", "architecture", examplesDirName} {
		dir := filepath.Join(siteDir, name)
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("remove %s: %w", dir, err)
		}
	}

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
				{Text: "Configuration", Link: "/guide/configuration"},
				{Text: "Engine Modes", Link: "/guide/engines"},
				{Text: "Persistence", Link: "/guide/persistence"},
				{Text: "Chaos Testing", Link: "/guide/chaos"},
				{Text: "Security Groups", Link: "/guide/security-groups"},
				{Text: "App Inspector", Link: "/guide/app-inspector"},
				{Text: "IAM Policy Stream", Link: "/guide/iam-policy-stream"},
				{Text: "Pinned Resource IDs", Link: "/guide/pinned-ids"},
			},
		},
		{
			Text: "Deployment & Integrations",
			Items: []sidebarLink{
				{Text: "Testcontainers", Link: "/guide/testcontainers"},
				{Text: "Kubernetes", Link: "/guide/kubernetes"},
				{Text: "Extensions", Link: "/guide/extensions"},
				{Text: "MCP Server", Link: "/guide/mcp"},
				{Text: "Replicator", Link: "/guide/replicator"},
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

	for _, spec := range guideAndArchDocSpecs(guideDir, archDir) {
		if err := copyTransformedDoc(spec); err != nil {
			return err
		}
	}

	return writeFileAtomic(filepath.Join(guideDir, "performance.md"), []byte(renderPerformancePage()))
}

func guideAndArchDocSpecs(guideDir, archDir string) []docPageSpec {
	specs := make([]docPageSpec, 0, len(guideFiles())+len(archFiles()))
	for _, gf := range guideFiles() {
		specs = append(specs, docPageSpec{
			Src:   filepath.Join(docsDirName, gf.file),
			Dest:  filepath.Join(guideDir, gf.file),
			Title: gf.title,
		})
	}
	for _, af := range archFiles() {
		specs = append(specs, docPageSpec{
			Src:   filepath.Join(docsArchDirName, af.src),
			Dest:  filepath.Join(archDir, af.dest),
			Title: af.title,
		})
	}

	return specs
}

func copyTransformedDoc(spec docPageSpec) error {
	raw, err := os.ReadFile(filepath.Clean(spec.Src))
	if err != nil {
		return fmt.Errorf("read %s: %w", spec.Src, err)
	}

	body := sanitizeMarkdownForVitePress(string(raw), filepath.ToSlash(filepath.Dir(spec.Src)))
	title := extractMarkdownTitle(string(raw), spec.Title)
	out := "---\ntitle: " + escapeYAMLString(title) + "\n---\n\n" + body

	return writeFileAtomic(spec.Dest, []byte(out))
}

// renderPerformancePage builds the Performance page.
func renderPerformancePage() string {
	var b strings.Builder

	b.WriteString("---\ntitle: Performance\n")
	b.WriteString("description: How to run the Gopherstack benchmarks.\n---\n\n")
	b.WriteString("# Performance\n\n")
	b.WriteString("- Single Go binary. Services run in-process with in-memory state.\n")
	b.WriteString("- Persistence is off by default. `--persist` writes snapshots to disk.\n")
	b.WriteString("- No per-service startup step: services register when the server starts.\n\n")
	b.WriteString("> [!NOTE]\n")
	b.WriteString("> This site publishes no startup-time, memory or LocalStack comparison figures. ")
	b.WriteString("Measure on your own hardware.\n\n")
	b.WriteString("## Benchmarks\n\n")
	b.WriteString("Baseline results and profiling notes: ")
	b.WriteString("[`bench/README.md`](" + githubRepoBase + "/blob/main/bench/README.md).\n\n")
	b.WriteString("```bash\n# All benchmarks\ntask bench\n\n")
	b.WriteString("# S3, SQS, DynamoDB, KMS and Secrets Manager backends\n")
	b.WriteString("go test -bench=. -benchmem ./bench/\n\n")
	b.WriteString("# Full request path through the router\n")
	b.WriteString("go test -run '^$' -bench 'BenchmarkE2E_' -benchmem -cpu=4 ./internal/app/\n")
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
	return "Example: `" + slug + "`."
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
	body = sanitizeMarkdownForVitePress(body, path.Join(examplesDirName, ex.Slug))

	var b strings.Builder
	b.WriteString("---\ntitle: " + escapeYAMLString(title) + "\n")
	b.WriteString("description: " + escapeYAMLString(ex.Purpose) + "\n---\n\n")
	b.WriteString("# " + title + "\n\n")
	fmt.Fprintf(&b, "[Source on GitHub](%s/tree/main/examples/%s) · [All examples](/examples/)\n\n",
		githubRepoBase, ex.Slug)
	b.WriteString(strings.TrimSpace(body) + "\n")

	return writeFileAtomic(filepath.Join(exOutDir, ex.Slug+".md"), []byte(b.String()))
}

func renderExamplesIndexPage(examples []siteExample) string {
	var b strings.Builder

	b.WriteString("---\ntitle: Examples\n")
	b.WriteString("description: Runnable multi-service examples.\n---\n\n")
	b.WriteString("# Examples\n\n")
	b.WriteString("Source: [`examples/`](" + githubRepoBase + "/tree/main/examples). ")
	b.WriteString("Most examples ship a `docker-compose.yml` and a `demo.sh`; check each README.\n\n")
	b.WriteString("```bash\ncd examples/<name>\ndocker compose up -d\n./demo.sh\ndocker compose down -v\n```\n\n")
	b.WriteString("| Example | Purpose | Requires | Source |\n")
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
