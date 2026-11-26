package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	gradeA           = "A"
	gradeB           = "B"
	gradeRemoved     = "Removed"
	maxInlineOpRows  = 500
	serviceGuideKind = "service"
	cloudAWS         = "AWS"
	cloudAzure       = "Azure"
	cloudGCP         = "GCP"
	cloudOCI         = "OCI"
)

// siteServiceRecord is the JSON shape consumed by ServiceIndex.vue.
type siteServiceRecord struct {
	Slug                string `json:"slug"`
	DisplayName         string `json:"displayName"`
	Cloud               string `json:"cloud"`
	Category            string `json:"category"`
	Grade               string `json:"grade"`
	Protocol            string `json:"protocol"`
	SDKModule           string `json:"sdkModule"`
	LastAuditDate       string `json:"lastAuditDate"`
	LeaksStatus         string `json:"leaksStatus"`
	OpsTotal            int    `json:"opsTotal"`
	OpsOK               int    `json:"opsOK"`
	OpsPartial          int    `json:"opsPartial"`
	OpsGap              int    `json:"opsGap"`
	OpsDeferred         int    `json:"opsDeferred"`
	FamiliesTotal       int    `json:"familiesTotal"`
	KnownGapsCount      int    `json:"knownGapsCount"`
	StructuralGapsCount int    `json:"structuralGapsCount"`
	DeferredCount       int    `json:"deferredCount"`
	HasGuide            bool   `json:"hasGuide"`
	Removed             bool   `json:"removed"`
}

func cloudForCategory(category string) string {
	switch category {
	case cloudAzure, cloudGCP, cloudOCI:
		return category
	default:
		return cloudAWS
	}
}

// buildSiteServiceRecords builds sorted siteServiceRecord rows for every
// discovered service slug.
func buildSiteServiceRecords(entries []summaryEntry, docBySlug map[string]*ParityDoc) []siteServiceRecord {
	records := make([]siteServiceRecord, 0, len(entries))

	for _, e := range entries {
		cloud := cloudForCategory(e.Category)
		doc := docBySlug[e.Slug]
		if doc == nil {
			records = append(records, siteServiceRecord{
				Slug:        e.Slug,
				DisplayName: e.DisplayName,
				Cloud:       cloud,
				Category:    e.Category,
				Grade:       gradeRemoved,
				Removed:     true,
			})

			continue
		}

		ot := doc.opsTally()
		records = append(records, siteServiceRecord{
			Slug:                e.Slug,
			DisplayName:         e.DisplayName,
			Cloud:               cloud,
			Category:            e.Category,
			Grade:               doc.Overall,
			Protocol:            doc.Protocol,
			SDKModule:           doc.SDKModule,
			LastAuditDate:       doc.LastAuditDate,
			LeaksStatus:         doc.LeaksStatus,
			OpsTotal:            len(doc.Ops),
			OpsOK:               ot.ok,
			OpsPartial:          ot.partial,
			OpsGap:              ot.gap,
			OpsDeferred:         ot.deferred,
			FamiliesTotal:       len(doc.Families),
			KnownGapsCount:      len(doc.openItems()),
			StructuralGapsCount: len(doc.StructuralGaps),
			DeferredCount:       len(doc.Deferred),
			HasGuide:            guideExists(e.Slug),
			Removed:             false,
		})
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].DisplayName < records[j].DisplayName
	})

	return records
}

// writeServicePages generates site/services/index.md and one page per service
// under site/services/<slug>.md.
func writeServicePages(siteDir string, entries []summaryEntry, docBySlug map[string]*ParityDoc) error {
	servicesOutDir := filepath.Join(siteDir, "services")
	if err := os.MkdirAll(servicesOutDir, siteDirMode); err != nil {
		return fmt.Errorf("mkdir %s: %w", servicesOutDir, err)
	}

	indexContent := renderServiceIndexPage(len(entries), len(docBySlug))
	if err := writeFileAtomic(filepath.Join(servicesOutDir, "index.md"), []byte(indexContent)); err != nil {
		return err
	}

	for _, e := range entries {
		content, err := renderSiteServicePage(e, docBySlug[e.Slug])
		if err != nil {
			return err
		}

		outPath := filepath.Join(servicesOutDir, e.Slug+".md")
		if writeErr := writeFileAtomic(outPath, []byte(content)); writeErr != nil {
			return writeErr
		}
	}

	return nil
}

// renderServiceIndexPage generates the VitePress /services/ landing page.
func renderServiceIndexPage(totalServices, auditedServices int) string {
	var b strings.Builder

	b.WriteString("---\ntitle: Service Index\n")
	b.WriteString("description: Searchable index of all AWS, Azure, and GCP services emulated by Gopherstack.\n")
	b.WriteString("aside: false\n---\n\n")
	b.WriteString("# Service Index\n\n")
	fmt.Fprintf(&b,
		"Gopherstack includes **%d services** (%d audited against `PARITY.md` manifests, "+
			"plus removed legacy placeholders). Filter by category, parity grade, or protocol below, "+
			"or click any service to view its audited operations, known gaps, and usage guide.\n\n",
		totalServices, auditedServices,
	)
	b.WriteString("<ServiceIndex />\n\n")
	b.WriteString("## Parity Grades\n\n")
	b.WriteString("| Grade | Meaning |\n| --- | --- |\n")
	b.WriteString(
		"| **A** | Core operations, error shapes, state transitions, and persistence are audited and passing. |\n",
	)
	b.WriteString(
		"| **B** | Functional coverage with tracked gaps listed on the service page and [Limitations](/limitations). |\n",
	)
	b.WriteString(
		"| **Removed** | Deprecated by AWS and removed from Gopherstack " +
			"(see [Limitations](/limitations#removed-services)). |\n",
	)

	return b.String()
}

// renderSiteServicePage generates the Markdown page for a single service.
func renderSiteServicePage(entry summaryEntry, doc *ParityDoc) (string, error) {
	if doc == nil {
		return renderRemovedServicePage(entry)
	}

	var b strings.Builder
	desc := fmt.Sprintf("Gopherstack %s (%s) — Parity Grade %s.", entry.DisplayName, entry.Category, doc.Overall)

	b.WriteString("---\n")
	b.WriteString("title: " + escapeYAMLString(entry.DisplayName) + "\n")
	b.WriteString("description: " + escapeYAMLString(desc) + "\n")
	b.WriteString("---\n\n")
	b.WriteString("# " + entry.DisplayName + "\n\n")
	b.WriteString(renderHeaderLine(doc) + "\n\n")
	b.WriteString("**Category:** " + entry.Category + " · ")
	fmt.Fprintf(&b, "[View PARITY.md on GitHub](%s/blob/main/services/%s/PARITY.md) · ",
		githubRepoBase, entry.Slug)
	b.WriteString("[All Services](/services/)\n\n")

	if err := appendServiceGuide(&b, entry.Slug); err != nil {
		return "", err
	}

	b.WriteString("## Parity & Coverage\n\n")
	b.WriteString("| Metric | Value |\n| --- | --- |\n")
	writeCoverageRows(&b, doc)
	b.WriteString("\n")

	writeSiteGapList(&b, "Known Gaps", doc.openItems(), "")
	writeSiteGapList(
		&b,
		"Structural Gaps",
		doc.StructuralGaps,
		"These do not block an A grade — no implementation could produce real data "+
			"here because the underlying data source cannot exist in an emulator.",
	)
	writeSiteGapList(&b, "Deferred Items", doc.Deferred, "")

	writeSiteOperationsSection(&b, doc)

	return b.String(), nil
}

// renderRemovedServicePage generates a page for a removed service (qldb, qldbsession).
func renderRemovedServicePage(entry summaryEntry) (string, error) {
	readmePath := filepath.Clean(filepath.Join(servicesDirName, entry.Slug, readmeFileName))
	raw, err := os.ReadFile(readmePath)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", readmePath, err)
	}

	body := stripLeadingH1(string(raw))
	body = sanitizeMarkdownForVitePress(body, serviceGuideKind)

	var b strings.Builder
	b.WriteString("---\n")
	b.WriteString("title: " + escapeYAMLString(entry.DisplayName+" (Removed)") + "\n")
	b.WriteString("description: " + escapeYAMLString(entry.DisplayName+" has been removed from Gopherstack.") + "\n")
	b.WriteString("---\n\n")
	b.WriteString("# " + entry.DisplayName + " (Removed)\n\n")
	b.WriteString("::: warning Removed Service\n")
	b.WriteString(strings.TrimSpace(body) + "\n")
	b.WriteString(":::\n\n")
	b.WriteString("[Back to Service Index](/services/)\n")

	return b.String(), nil
}

// appendServiceGuide embeds docs/services/<slug>.md when present.
func appendServiceGuide(b *strings.Builder, slug string) error {
	if !guideExists(slug) {
		return nil
	}

	guidePath := filepath.Clean(filepath.Join(docsServicesDir, slug+".md"))
	raw, err := os.ReadFile(guidePath)
	if err != nil {
		return fmt.Errorf("read %s: %w", guidePath, err)
	}

	body := stripLeadingH1(string(raw))
	body = sanitizeMarkdownForVitePress(body, serviceGuideKind)
	body = strings.TrimSpace(body)
	if body == "" {
		return nil
	}

	b.WriteString(body + "\n\n")

	return nil
}

// writeSiteGapList writes a full (non-truncated) heading and bullet list when items is non-empty.
func writeSiteGapList(b *strings.Builder, heading string, items []string, note string) {
	if len(items) == 0 {
		return
	}

	b.WriteString("## " + heading + "\n\n")
	if note != "" {
		b.WriteString(note + "\n\n")
	}

	for _, item := range items {
		b.WriteString("- " + sanitizeInlineProse(item) + "\n")
	}
	b.WriteString("\n")
}

// writeSiteOperationsSection writes collapsible operations / feature-family
// breakdown tables from PARITY.md.
func writeSiteOperationsSection(b *strings.Builder, doc *ParityDoc) {
	if len(doc.Ops) > 0 {
		b.WriteString("## Audited Operations\n\n")
		fmt.Fprintf(b, "<details>\n<summary>View %d audited PARITY operation entries</summary>\n\n", len(doc.Ops))
		b.WriteString("| Operation | Wire | Errors | State | Persist |\n")
		b.WriteString("| --- | --- | --- | --- | --- |\n")

		limit := min(len(doc.Ops), maxInlineOpRows)
		for _, op := range doc.Ops[:limit] {
			fmt.Fprintf(b, "| `%s` | %s | %s | %s | %s |\n",
				escapeMarkdownCell(op.Name),
				cellOrDash(op.Wire),
				cellOrDash(op.Errors),
				cellOrDash(op.State),
				cellOrDash(op.Persist),
			)
		}
		if len(doc.Ops) > limit {
			fmt.Fprintf(b, "\n*Showing %d of %d entries — see `PARITY.md` on GitHub for the full list.*\n",
				limit, len(doc.Ops))
		}
		b.WriteString("\n</details>\n\n")
	}

	if len(doc.Families) > 0 {
		b.WriteString("## Audited Feature Families\n\n")
		b.WriteString("| Feature Family | Status |\n| --- | --- |\n")
		for _, f := range doc.Families {
			fmt.Fprintf(b, "| `%s` | %s |\n", escapeMarkdownCell(f.Name), cellOrDash(f.Status))
		}
		b.WriteString("\n")
	}
}

func escapeMarkdownCell(s string) string {
	s = strings.ReplaceAll(s, "|", `\|`)
	s = strings.ReplaceAll(s, "`", "")

	return s
}

func cellOrDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "—"
	}

	return strings.TrimSpace(s)
}

// writeLimitationsPage generates site/limitations.md combining platform-wide
// design boundaries with live aggregated gaps from PARITY.md.
func writeLimitationsPage(siteDir string, entries []summaryEntry, docBySlug map[string]*ParityDoc) error {
	content := renderLimitationsPage(entries, docBySlug)

	return writeFileAtomic(filepath.Join(siteDir, "limitations.md"), []byte(content))
}

// renderLimitationsPage builds the Markdown for site/limitations.md.
func renderLimitationsPage(entries []summaryEntry, docBySlug map[string]*ParityDoc) string {
	var b strings.Builder

	b.WriteString("---\ntitle: Limitations & Parity Gaps\n")
	b.WriteString(
		"description: Platform design boundaries, metadata-only services, and open parity gaps across Gopherstack.\n",
	)
	b.WriteString("---\n\n")
	b.WriteString("# Limitations & Parity Gaps\n\n")
	b.WriteString("Gopherstack prioritizes sub-100 ms cold starts, low memory usage, and high-fidelity AWS wire " +
		"protocols. Because it runs as an in-memory emulator rather than real cloud infrastructure, " +
		"keep the following architectural boundaries and per-service gaps in mind.\n\n")

	writePlatformBoundaries(&b)
	writeOpenGapsSummary(&b, entries, docBySlug)
	writeStructuralGapsSummary(&b, entries, docBySlug)
	writeRemovedServicesSummary(&b, entries, docBySlug)

	return b.String()
}

func writePlatformBoundaries(b *strings.Builder) {
	b.WriteString("## Platform & Architectural Boundaries\n\n")
	b.WriteString("| Area | Behaviour & Limitation |\n| --- | --- |\n")
	b.WriteString(
		"| **In-memory state by default** | State lives in memory unless `--persist` / `PERSIST=true` is enabled " +
			"(debounced JSON snapshots on disk). |\n",
	)
	b.WriteString(
		"| **Single-port HTTP routing** | All services share port `8000` (plus `4566` with `--localstack-compat`). " +
			"Transparent per-service DNS/proxy injection is not used unless `--dns-addr` is enabled. |\n",
	)
	b.WriteString(
		"| **Container runtime for Lambda & engines** | Zip/image Lambda execution and `docker` engine modes " +
			"require a reachable Docker or Podman socket (`--engines auto` probes once at startup). |\n",
	)
	b.WriteString(
		"| **Metadata-only control planes** | Services such as RDS, Redshift, and Aurora DSQL default to " +
			"metadata-only stubs unless switched to a container-backed engine where supported " +
			"(see [Engine Modes](/guide/engines)). |\n",
	)
	b.WriteString(
		"| **LocalStack features not replicated** | LocalStack Cloud Pods, LocalStack extensions, " +
			"`/health` (collides with S3 bucket `health`), and `/_aws/lambda/runtimes` are omitted. " +
			"See [Migrating from LocalStack](/guide/migration). |\n",
	)
	b.WriteString(
		"| **IAM enforcement is opt-in** | Requests succeed without policy evaluation unless `--enforce-iam` " +
			"(`GOPHERSTACK_ENFORCE_IAM=true`) is set. |\n",
	)
	b.WriteString(
		"| **Desktop binaries** | `gopherstack-desktop` release archives are currently unsigned; " +
			"macOS requires right-click → Open on first launch. |\n\n",
	)
}

func writeOpenGapsSummary(b *strings.Builder, entries []summaryEntry, docBySlug map[string]*ParityDoc) {
	type serviceGaps struct {
		entry summaryEntry
		grade string
		items []string
	}

	var withGaps []serviceGaps
	totalGaps := 0
	for _, e := range entries {
		doc := docBySlug[e.Slug]
		if doc == nil {
			continue
		}
		items := doc.openItems()
		if len(items) == 0 {
			continue
		}
		totalGaps += len(items)
		withGaps = append(withGaps, serviceGaps{entry: e, grade: doc.Overall, items: items})
	}

	sort.Slice(withGaps, func(i, j int) bool {
		return withGaps[i].entry.DisplayName < withGaps[j].entry.DisplayName
	})

	b.WriteString("## Open Parity Gaps by Service\n\n")
	fmt.Fprintf(
		b,
		"Generated automatically from `services/*/PARITY.md`: **%d open gap(s)** across **%d service(s)**.\n\n",
		totalGaps,
		len(withGaps),
	)

	if len(withGaps) == 0 {
		b.WriteString("No open parity gaps are currently recorded.\n\n")

		return
	}

	for _, sg := range withGaps {
		fmt.Fprintf(b, "### [%s](/services/%s#known-gaps) (Grade %s — %s)\n\n",
			sg.entry.DisplayName, sg.entry.Slug, sg.grade, plural(len(sg.items), "gap", "gaps"))
		for _, item := range sg.items {
			b.WriteString("- " + sanitizeInlineProse(item) + "\n")
		}
		b.WriteString("\n")
	}
}

func writeStructuralGapsSummary(b *strings.Builder, entries []summaryEntry, docBySlug map[string]*ParityDoc) {
	type structRow struct {
		entry summaryEntry
		count int
	}

	var rows []structRow
	total := 0
	for _, e := range entries {
		doc := docBySlug[e.Slug]
		if doc == nil || len(doc.StructuralGaps) == 0 {
			continue
		}
		total += len(doc.StructuralGaps)
		rows = append(rows, structRow{entry: e, count: len(doc.StructuralGaps)})
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].count != rows[j].count {
			return rows[i].count > rows[j].count
		}

		return rows[i].entry.DisplayName < rows[j].entry.DisplayName
	})

	b.WriteString("## Structural Gaps (Cannot Be Emulated)\n\n")
	fmt.Fprintf(b,
		"Across **%d services**, **%d structural gap(s)** represent external cloud dependencies "+
			"(real internet traffic, billing/settlement systems, proprietary ML inference, or physical hardware) "+
			"that cannot exist in a local emulator. They do not block a Grade **A**.\n\n",
		len(rows), total,
	)

	if len(rows) == 0 {
		return
	}

	b.WriteString("<details>\n<summary>Services with structural gaps (" + strconv.Itoa(len(rows)) + ")</summary>\n\n")
	b.WriteString("| Service | Category | Structural Gaps |\n| --- | --- | --- |\n")
	for _, r := range rows {
		fmt.Fprintf(b, "| [%s](/services/%s#structural-gaps) | %s | %d |\n",
			r.entry.DisplayName, r.entry.Slug, r.entry.Category, r.count)
	}
	b.WriteString("\n</details>\n\n")
}

func writeRemovedServicesSummary(b *strings.Builder, entries []summaryEntry, docBySlug map[string]*ParityDoc) {
	var removed []summaryEntry
	for _, e := range entries {
		if docBySlug[e.Slug] == nil {
			removed = append(removed, e)
		}
	}

	if len(removed) == 0 {
		return
	}

	sort.Slice(removed, func(i, j int) bool {
		return removed[i].DisplayName < removed[j].DisplayName
	})

	b.WriteString("## Removed Services\n\n")
	b.WriteString("The following deprecated AWS services were removed from Gopherstack:\n\n")
	for _, r := range removed {
		fmt.Fprintf(b, "- [%s](/services/%s) (`services/%s`)\n", r.DisplayName, r.Slug, r.Slug)
	}
	b.WriteString("\n")
}
