package main

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	gradeA          = "A"
	gradeB          = "B"
	gradeRemoved    = "Removed"
	maxInlineOpRows = 500
	cloudAWS        = "AWS"
	cloudAzure      = "Azure"
	cloudGCP        = "GCP"
	cloudOCI        = "OCI"
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
			KnownGapsCount:      len(doc.ItemsStillOpen),
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
	b.WriteString("description: Index of every service Gopherstack emulates, with parity grades.\n")
	b.WriteString("aside: false\n---\n\n")
	b.WriteString("# Service Index\n\n")
	fmt.Fprintf(&b, "- %d services, %d with a `PARITY.md` audit.\n", totalServices, auditedServices)
	b.WriteString("- Filter by cloud, category, grade or protocol.\n")
	b.WriteString("- Each service page lists audited operations and open gaps.\n\n")
	b.WriteString("<ServiceIndex />\n\n")
	b.WriteString("## Grades\n\n")
	b.WriteString("- **A, B, C**: the `overall:` grade in the service's `PARITY.md`. ")
	b.WriteString("Open items are listed on the service page and in [Limitations](/limitations).\n")
	b.WriteString("- **Removed**: no longer emulated. See [Limitations](/limitations#removed-services).\n")

	return b.String()
}

// renderSiteServicePage generates the Markdown page for a single service.
func renderSiteServicePage(entry summaryEntry, doc *ParityDoc) (string, error) {
	if doc == nil {
		return renderRemovedServicePage(entry)
	}

	var b strings.Builder
	desc := fmt.Sprintf("%s (%s): parity grade %s.", entry.DisplayName, entry.Category, doc.Overall)

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
	writeCoverageRows(&b, doc, len(doc.ItemsStillOpen))
	b.WriteString("\n")

	writeSiteGapList(&b, "Known Gaps", doc.ItemsStillOpen, "")
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
	body = sanitizeMarkdownForVitePress(body, path.Join(servicesDirName, entry.Slug))

	var b strings.Builder
	b.WriteString("---\n")
	b.WriteString("title: " + escapeYAMLString(entry.DisplayName+" (Removed)") + "\n")
	b.WriteString("description: " + escapeYAMLString(entry.DisplayName+" has been removed from Gopherstack.") + "\n")
	b.WriteString("---\n\n")
	b.WriteString("# " + entry.DisplayName + " (Removed)\n\n")
	b.WriteString("::: warning Removed\n")
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
	body = sanitizeMarkdownForVitePress(body, docsServicesDir)
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
		fmt.Fprintf(b, "<details>\n<summary>%d audited operations</summary>\n\n", len(doc.Ops))
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
			fmt.Fprintf(b, "\nShowing %d of %d entries. See `PARITY.md` on GitHub for the rest.\n",
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
	b.WriteString("description: Platform limits and open parity gaps, generated from each service's PARITY.md.\n")
	b.WriteString("---\n\n")
	b.WriteString("# Limitations & Parity Gaps\n\n")
	b.WriteString("> [!WARNING]\n")
	b.WriteString("> Gopherstack is an in-memory emulator, not AWS. IAM policies are not evaluated unless ")
	b.WriteString("`--enforce-iam` is set.\n\n")

	writePlatformBoundaries(&b)
	writeOpenGapsSummary(&b, entries, docBySlug)
	writeStructuralGapsSummary(&b, entries, docBySlug)
	writeRemovedServicesSummary(&b, entries, docBySlug)

	return b.String()
}

func writePlatformBoundaries(b *strings.Builder) {
	b.WriteString("## Platform\n\n")
	b.WriteString("- **State**: in memory. `--persist` (`PERSIST=true`) writes snapshots to disk.\n")
	b.WriteString("- **Ports**: all services share port `8000`. `--localstack-compat` also listens on `4566`.\n")
	b.WriteString("- **Endpoints**: per-service DNS or proxy injection is off. ")
	b.WriteString("Set `--dns-addr` to enable the embedded DNS server.\n")
	b.WriteString("- **Containers**: Lambda and the `docker` engine modes need a reachable Docker or Podman socket. ")
	b.WriteString("See [Engine Modes](/guide/engines).\n")
	b.WriteString("- **Stub engines**: container-backed services such as RDS run as in-memory metadata ")
	b.WriteString("unless you switch their engine.\n")
	b.WriteString("- **IAM**: not enforced by default. `--enforce-iam` (`GOPHERSTACK_ENFORCE_IAM=true`) turns it on.\n")
	b.WriteString("- **LocalStack**: not provided: `/health`, `/_aws/sns/*`, `/_aws/dynamodb/expired`, ")
	b.WriteString("in-process extensions. See [Migrating from LocalStack](/guide/migration).\n\n")
	b.WriteString("> [!NOTE]\n")
	b.WriteString("> `gopherstack-desktop` release archives are unsigned. macOS asks for approval on first launch.\n\n")
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
		items := doc.ItemsStillOpen
		if len(items) == 0 {
			continue
		}
		totalGaps += len(items)
		withGaps = append(withGaps, serviceGaps{entry: e, grade: doc.Overall, items: items})
	}

	sort.Slice(withGaps, func(i, j int) bool {
		return withGaps[i].entry.DisplayName < withGaps[j].entry.DisplayName
	})

	b.WriteString("## Open Gaps\n\n")
	fmt.Fprintf(b, "%s across %s, from `items_still_open:` in `services/*/PARITY.md`.\n\n",
		plural(totalGaps, "open gap", "open gaps"), plural(len(withGaps), "service", "services"))

	if len(withGaps) == 0 {
		b.WriteString("None recorded.\n\n")

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

	b.WriteString("## Structural Gaps\n\n")
	fmt.Fprintf(b,
		"%s across %s. These need real traffic, billing systems, ML inference or hardware, "+
			"so they cannot be emulated. They do not block grade A.\n\n",
		plural(total, "structural gap", "structural gaps"), plural(len(rows), "service", "services"),
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
	b.WriteString("These services are no longer emulated:\n\n")
	for _, r := range removed {
		fmt.Fprintf(b, "- [%s](/services/%s) (`services/%s`)\n", r.DisplayName, r.Slug, r.Slug)
	}
	b.WriteString("\n")
}
