package codebuild

import (
	"encoding/xml"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	reportTypeTest     = "TEST"
	reportTypeCoverage = "CODE_COVERAGE"

	formatJUnit     = "JUNITXML"
	formatCobertura = "COBERTURAXML"

	testStatusSucceeded = "SUCCEEDED"
	testStatusFailed    = "FAILED"
	testStatusError     = "ERROR"
	testStatusSkipped   = "SKIPPED"

	reportStatusFailed = "FAILED"

	nanosPerSecond = 1e9
	percentScale   = 100
	roundingFactor = 100
)

var errUnsupportedReportFormat = errors.New("report file format has no parser in the emulator")

// reportTypeForFormat maps a buildspec file-format onto its report group type.
func reportTypeForFormat(format string) (string, bool) {
	switch format {
	case formatJUnit, formatCucumber, formatTestNG, formatNUnit, formatNUnit3, formatTRX:
		return reportTypeTest, true
	case formatCobertura, formatClover, formatJaCoCo, formatSimpleCov:
		return reportTypeCoverage, true
	default:
		return "", false
	}
}

type junitSuite struct {
	Name   string       `xml:"name,attr"`
	Cases  []junitCase  `xml:"testcase"`
	Nested []junitSuite `xml:"testsuite"`
}

type junitCase struct {
	Failure   *junitDetail `xml:"failure"`
	Error     *junitDetail `xml:"error"`
	Skipped   *junitDetail `xml:"skipped"`
	ClassName string       `xml:"classname,attr"`
	Name      string       `xml:"name,attr"`
	Time      string       `xml:"time,attr"`
}

type junitDetail struct {
	Message string `xml:"message,attr"`
	Text    string `xml:",chardata"`
}

func (d *junitDetail) text() string {
	if d.Message != "" {
		return d.Message
	}

	return strings.TrimSpace(d.Text)
}

// parseJUnit parses a JUnit XML document whose root is <testsuites> or <testsuite>.
func parseJUnit(data []byte, reportArn, source string) ([]TestCase, error) {
	var root junitSuite
	if err := xml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("parse junit %s: %w", source, err)
	}

	return appendSuiteCases(nil, root, reportArn, source), nil
}

func appendSuiteCases(out []TestCase, s junitSuite, reportArn, source string) []TestCase {
	for _, c := range s.Cases {
		out = append(out, junitTestCase(c, s.Name, reportArn, source))
	}

	for _, n := range s.Nested {
		out = appendSuiteCases(out, n, reportArn, source)
	}

	return out
}

func junitTestCase(c junitCase, suite, reportArn, source string) TestCase {
	tc := TestCase{
		ReportArn:       reportArn,
		TestRawDataPath: source,
		Prefix:          c.ClassName,
		Name:            c.Name,
		TestSuiteName:   suite,
		Status:          testStatusSucceeded,
	}

	if secs, err := strconv.ParseFloat(strings.TrimSpace(c.Time), 64); err == nil && secs > 0 {
		tc.DurationInNanoSeconds = int64(secs * nanosPerSecond)
	}

	switch {
	case c.Failure != nil:
		tc.Status, tc.Message = testStatusFailed, c.Failure.text()
	case c.Error != nil:
		tc.Status, tc.Message = testStatusError, c.Error.text()
	case c.Skipped != nil:
		tc.Status, tc.Message = testStatusSkipped, c.Skipped.text()
	}

	return tc
}

// summarizeTests aggregates test cases and derives the report status.
func summarizeTests(cases []TestCase) (*TestReportSummary, string) {
	sum := &TestReportSummary{StatusCounts: map[string]int32{}, Total: clampInt32(len(cases))}

	status := testStatusSucceeded

	for _, c := range cases {
		sum.StatusCounts[c.Status]++
		sum.DurationInNanoSeconds += c.DurationInNanoSeconds

		if c.Status == testStatusFailed || c.Status == testStatusError {
			status = reportStatusFailed
		}
	}

	return sum, status
}

type coberturaDoc struct {
	Packages []struct {
		Classes []coberturaClass `xml:"classes>class"`
	} `xml:"packages>package"`
}

type coberturaClass struct {
	FileName string `xml:"filename,attr"`
	Lines    []struct {
		Number    string `xml:"number,attr"`
		Hits      string `xml:"hits,attr"`
		Branch    string `xml:"branch,attr"`
		Condition string `xml:"condition-coverage,attr"`
	} `xml:"lines>line"`
}

var conditionRe = regexp.MustCompile(`\((\d+)/(\d+)\)`)

type fileCoverage struct {
	covered, missed, bCovered, bMissed int32
}

// parseCobertura parses a Cobertura XML document into per-file coverage.
func parseCobertura(data []byte, reportArn, source string) ([]CodeCoverage, error) {
	var doc coberturaDoc
	if err := xml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse cobertura %s: %w", source, err)
	}

	files := map[string]*fileCoverage{}

	for _, p := range doc.Packages {
		for _, c := range p.Classes {
			addClassCoverage(files, c)
		}
	}

	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}

	sort.Strings(names)

	out := make([]CodeCoverage, 0, len(names))
	for _, n := range names {
		out = append(out, toCodeCoverage(reportArn, n, *files[n]))
	}

	return out, nil
}

func addClassCoverage(files map[string]*fileCoverage, c coberturaClass) {
	fc := files[c.FileName]
	if fc == nil {
		fc = &fileCoverage{}
		files[c.FileName] = fc
	}

	for _, l := range c.Lines {
		if hits, _ := strconv.Atoi(l.Hits); hits > 0 {
			fc.covered++
		} else {
			fc.missed++
		}

		if m := conditionRe.FindStringSubmatch(l.Condition); m != nil && isTrue(l.Branch) {
			cov, _ := strconv.ParseInt(m[1], 10, 32)
			tot, _ := strconv.ParseInt(m[2], 10, 32)
			covered, total := int32(cov), int32(tot)
			fc.bCovered += covered
			fc.bMissed += max(total-covered, 0)
		}
	}
}

func clampInt32(n int) int32 {
	return int32(min(max(n, 0), math.MaxInt32))
}

func pct(covered, missed int32) float64 {
	total := covered + missed
	if total == 0 {
		return 0
	}

	return math.Round(float64(covered)/float64(total)*percentScale*roundingFactor) / roundingFactor
}

func toCodeCoverage(reportArn, file string, fc fileCoverage) CodeCoverage {
	return CodeCoverage{
		ID:                       reportArn + ":" + file,
		ReportARN:                reportArn,
		FilePath:                 file,
		LineCoveragePercentage:   pct(fc.covered, fc.missed),
		LinesCovered:             fc.covered,
		LinesMissed:              fc.missed,
		BranchCoveragePercentage: pct(fc.bCovered, fc.bMissed),
		BranchesCovered:          fc.bCovered,
		BranchesMissed:           fc.bMissed,
	}
}

// summarizeCoverage aggregates per-file coverage into the report summary.
func summarizeCoverage(files []CodeCoverage) *CodeCoverageReportSummary {
	var s CodeCoverageReportSummary

	for _, f := range files {
		s.LinesCovered += f.LinesCovered
		s.LinesMissed += f.LinesMissed
		s.BranchesCovered += f.BranchesCovered
		s.BranchesMissed += f.BranchesMissed
	}

	s.LineCoveragePercentage = pct(s.LinesCovered, s.LinesMissed)
	s.BranchCoveragePercentage = pct(s.BranchesCovered, s.BranchesMissed)

	return &s
}

func isTrue(v string) bool {
	b, _ := strconv.ParseBool(v)

	return b
}
