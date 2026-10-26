package codebuild

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	reportStatusIncomplete = "INCOMPLETE"
	reportGroupActive      = "ACTIVE"
)

type specReport struct {
	DiscardPaths  any      `yaml:"discard-paths"`
	BaseDirectory string   `yaml:"base-directory"`
	FileFormat    string   `yaml:"file-format"`
	Files         []string `yaml:"files"`
}

// reportContent is what one buildspec report entry parsed to.
type reportContent struct {
	status    string
	tests     []TestCase
	coverages []CodeCoverage
}

// processReports parses every buildspec report entry and records a Report per entry.
func (r *buildRunner) processReports() {
	if r.spec == nil || len(r.spec.Reports) == 0 {
		return
	}

	names := make([]string, 0, len(r.spec.Reports))
	for n := range r.spec.Reports {
		names = append(names, n)
	}

	sort.Strings(names)

	r.log.write(r.ctx, marker("Preparing to copy reports"))

	for _, n := range names {
		if err := r.processReport(n, r.spec.Reports[n]); err != nil {
			r.log.write(r.ctx, marker("Report %s: %v", n, err))
		}
	}
}

func reportFormat(rep specReport) string {
	if rep.FileFormat == "" {
		return formatJUnit
	}

	return strings.ToUpper(rep.FileFormat)
}

func (r *buildRunner) processReport(name string, rep specReport) error {
	format := reportFormat(rep)

	rtype, ok := reportTypeForFormat(format)
	if !ok {
		return fmt.Errorf("%w: unknown file-format %q", errBuildspec, rep.FileFormat)
	}

	base, err := safeJoin(r.workDir, rep.BaseDirectory)
	if err != nil {
		return err
	}

	var files []string

	if _, serr := os.Stat(base); serr == nil {
		if files, err = matchFiles(base, rep.Files); err != nil {
			return err
		}
	}

	if len(files) == 0 {
		return fmt.Errorf("%w: no matching report paths found", errNoArtifactFiles)
	}

	content := r.parseReportFiles(base, format, files)

	return r.b.recordBuildReport(r.build, name, rtype, content)
}

func (r *buildRunner) parseReportFiles(base, format string, files []string) reportContent {
	parser, ok := reportParserFor(format)
	if !ok {
		r.log.write(r.ctx, marker("%v: %s", errUnsupportedReportFormat, format))

		return reportContent{status: reportStatusIncomplete}
	}

	var c reportContent

	for _, f := range files {
		data, err := os.ReadFile(filepath.Join(base, filepath.FromSlash(f)))
		if err == nil {
			err = parseReportFile(&c, parser, data, f)
		}

		if err != nil {
			r.log.write(r.ctx, marker("Failed to parse report file %s: %v", f, err))

			c.status = reportStatusFailed
		}
	}

	return c
}

func parseReportFile(c *reportContent, p reportParser, data []byte, source string) error {
	if p.tests != nil {
		cases, err := p.tests(data, source)
		c.tests = append(c.tests, cases...)

		return err
	}

	cov, err := p.coverage(data, source)
	c.coverages = append(c.coverages, cov...)

	return err
}

// resolveReportGroup finds the group a buildspec entry names, auto-creating "<project>-<name>" groups.
func (b *InMemoryBackend) resolveReportGroup(project, ref, rtype string) (*ReportGroup, error) {
	if strings.HasPrefix(ref, "arn:") {
		matches := b.reportGroupsByARN.Get(ref)
		if len(matches) == 0 {
			return nil, fmt.Errorf("%w: report group %s", ErrNotFound, ref)
		}

		return matches[0], b.checkGroupType(matches[0], rtype)
	}

	name := project + "-" + ref
	if rg, ok := b.reportGroups.Get(name); ok {
		return rg, b.checkGroupType(rg, rtype)
	}

	now := awstime.Epoch(time.Now())
	rg := &ReportGroup{
		Arn: b.buildReportGroupARN(name), Name: name, Type: rtype, Status: reportGroupActive,
		ExportConfig: ReportExportConfig{ExportConfigType: "NO_EXPORT"}, Tags: wireTags{},
		Created: now, LastModified: now,
	}
	b.reportGroups.Put(rg)

	return rg, nil
}

func (b *InMemoryBackend) checkGroupType(rg *ReportGroup, rtype string) error {
	if rg.Type != rtype {
		return fmt.Errorf("%w: report group %s is %s, buildspec reports %s", ErrValidation, rg.Name, rg.Type, rtype)
	}

	return nil
}

// recordBuildReport stores one report (and its parsed detail) for a finished build.
func (b *InMemoryBackend) recordBuildReport(build Build, ref, rtype string, c reportContent) error {
	b.mu.Lock("recordBuildReport")
	defer b.mu.Unlock()

	rg, err := b.resolveReportGroup(build.ProjectName, ref, rtype)
	if err != nil {
		return err
	}

	reportArn := arn.Build("codebuild", b.region, b.accountID, "report/"+rg.Name+":"+randomID())
	exportCfg := rg.ExportConfig
	rep := &Report{
		Arn: reportArn, Name: rg.Name, ReportGroupArn: rg.Arn, ExecutionID: build.Arn, Type: rtype,
		Status: c.status, Created: awstime.Epoch(time.Now()), ExportConfig: &exportCfg,
	}

	detail := &reportDetail{ReportArn: reportArn}

	for i := range c.tests {
		c.tests[i].ReportArn = reportArn
	}

	for i := range c.coverages {
		c.coverages[i].ReportARN = reportArn
		c.coverages[i].ID = reportArn + ":" + c.coverages[i].FilePath
	}

	detail.TestCases, detail.Coverages = c.tests, c.coverages

	if rtype == reportTypeTest {
		sum, status := summarizeTests(c.tests)
		rep.TestSummary = sum
		rep.Status = pickStatus(c.status, status)
	} else {
		rep.CodeCoverageSummary = summarizeCoverage(c.coverages)
		rep.Status = pickStatus(c.status, testStatusSucceeded)
	}

	b.reports.Put(rep)
	b.reportDetails.Put(detail)

	return nil
}

// pickStatus prefers an explicit non-success parse status over the derived one.
func pickStatus(parsed, derived string) string {
	if parsed != "" {
		return parsed
	}

	return derived
}
