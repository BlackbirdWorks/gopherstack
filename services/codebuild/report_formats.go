package codebuild

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	testStatusUnknown = "UNKNOWN"

	formatNUnit     = "NUNITXML"
	formatNUnit3    = "NUNIT3XML"
	formatTestNG    = "TESTNGXML"
	formatCucumber  = "CUCUMBERJSON"
	formatTRX       = "VISUALSTUDIOTRX"
	formatClover    = "CLOVERXML"
	formatJaCoCo    = "JACOCOXML"
	formatSimpleCov = "SIMPLECOV"

	clockParts     = 3
	cloverTypeCond = "cond"
	cloverTypeStmt = "stmt"
)

type reportParser struct {
	tests    func(data []byte, source string) ([]TestCase, error)
	coverage func(data []byte, source string) ([]CodeCoverage, error)
}

func reportParserFor(format string) (reportParser, bool) {
	switch format {
	case formatJUnit:
		return reportParser{tests: func(d []byte, s string) ([]TestCase, error) { return parseJUnit(d, "", s) }}, true
	case formatNUnit, formatNUnit3:
		return reportParser{tests: parseNUnit}, true
	case formatTestNG:
		return reportParser{tests: parseTestNG}, true
	case formatCucumber:
		return reportParser{tests: parseCucumber}, true
	case formatTRX:
		return reportParser{tests: parseTRX}, true
	case formatCobertura:
		return reportParser{coverage: func(d []byte, s string) ([]CodeCoverage, error) { return parseCobertura(d, "", s) }}, true
	case formatClover:
		return reportParser{coverage: parseClover}, true
	case formatJaCoCo:
		return reportParser{coverage: parseJaCoCo}, true
	case formatSimpleCov:
		return reportParser{coverage: parseSimpleCov}, true
	default:
		return reportParser{}, false
	}
}

func durationNanos(secs float64) int64 {
	if secs <= 0 {
		return 0
	}

	return int64(secs * nanosPerSecond)
}

func parseSeconds(s string) int64 {
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0
	}

	return durationNanos(v)
}

func coverageRows(files map[string]*fileCoverage) []CodeCoverage {
	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}

	sort.Strings(names)

	out := make([]CodeCoverage, 0, len(names))
	for _, n := range names {
		out = append(out, toCodeCoverage("", n, *files[n]))
	}

	return out
}

func fileEntry(files map[string]*fileCoverage, name string) *fileCoverage {
	fc := files[name]
	if fc == nil {
		fc = &fileCoverage{}
		files[name] = fc
	}

	return fc
}

func (f *fileCoverage) addLine(hit bool) {
	if hit {
		f.covered++
	} else {
		f.missed++
	}
}

func (f *fileCoverage) addBranch(hit bool) {
	if hit {
		f.bCovered++
	} else {
		f.bMissed++
	}
}

// NUnit 2 (test-results/.../test-case) and NUnit 3 (test-run/.../test-case).

type nunitSuite struct {
	Name    string       `xml:"name,attr"`
	Cases   []nunitCase  `xml:"test-case"`
	CasesV2 []nunitCase  `xml:"results>test-case"`
	Suites  []nunitSuite `xml:"test-suite"`
	SubV2   []nunitSuite `xml:"results>test-suite"`
}

type nunitCase struct {
	Name      string `xml:"name,attr"`
	ClassName string `xml:"classname,attr"`
	Result    string `xml:"result,attr"`
	Label     string `xml:"label,attr"`
	Time      string `xml:"time,attr"`
	Duration  string `xml:"duration,attr"`
	Failure   string `xml:"failure>message"`
	Reason    string `xml:"reason>message"`
}

func nunitStatus(result, label string) string {
	switch strings.ToLower(result) {
	case "success", "passed":
		return testStatusSucceeded
	case "failure", "failed":
		if strings.EqualFold(label, "error") {
			return testStatusError
		}

		return testStatusFailed
	case "error":
		return testStatusError
	case "ignored", "notrunnable", "skipped", "notexecuted":
		return testStatusSkipped
	default:
		return testStatusUnknown
	}
}

func parseNUnit(data []byte, source string) ([]TestCase, error) {
	var root nunitSuite
	if err := xml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("parse nunit %s: %w", source, err)
	}

	return appendNUnit(nil, root, source), nil
}

func appendNUnit(out []TestCase, s nunitSuite, source string) []TestCase {
	for _, c := range append(append([]nunitCase{}, s.Cases...), s.CasesV2...) {
		out = append(out, nunitTestCase(c, s.Name, source))
	}

	for _, n := range append(append([]nunitSuite{}, s.Suites...), s.SubV2...) {
		out = appendNUnit(out, n, source)
	}

	return out
}

func nunitTestCase(c nunitCase, suite, source string) TestCase {
	prefix, name := c.ClassName, c.Name

	if prefix == "" {
		if i := strings.LastIndex(c.Name, "."); i > 0 {
			prefix, name = c.Name[:i], c.Name[i+1:]
		}
	}

	dur := c.Duration
	if dur == "" {
		dur = c.Time
	}

	tc := TestCase{
		TestRawDataPath: source, Prefix: prefix, Name: name, TestSuiteName: suite,
		Status: nunitStatus(c.Result, c.Label), DurationInNanoSeconds: parseSeconds(dur),
	}

	tc.Message = strings.TrimSpace(c.Failure)
	if tc.Message == "" {
		tc.Message = strings.TrimSpace(c.Reason)
	}

	return tc
}

// TestNG (testng-results/suite/test/class/test-method).

type testNGDoc struct {
	Suites []struct {
		Name  string `xml:"name,attr"`
		Tests []struct {
			Classes []struct {
				Name    string `xml:"name,attr"`
				Methods []struct {
					Status     string `xml:"status,attr"`
					Name       string `xml:"name,attr"`
					DurationMs string `xml:"duration-ms,attr"`
					IsConfig   string `xml:"is-config,attr"`
					Message    string `xml:"exception>message"`
				} `xml:"test-method"`
			} `xml:"class"`
		} `xml:"test"`
	} `xml:"suite"`
}

func testNGStatus(s string) string {
	switch strings.ToUpper(s) {
	case "PASS":
		return testStatusSucceeded
	case "FAIL":
		return testStatusFailed
	case "SKIP":
		return testStatusSkipped
	default:
		return testStatusUnknown
	}
}

func parseTestNG(data []byte, source string) ([]TestCase, error) {
	var doc testNGDoc
	if err := xml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse testng %s: %w", source, err)
	}

	var out []TestCase

	for _, s := range doc.Suites {
		for _, t := range s.Tests {
			for _, c := range t.Classes {
				for _, m := range c.Methods {
					if isTrue(m.IsConfig) {
						continue
					}

					ms, _ := strconv.ParseFloat(m.DurationMs, 64)
					out = append(out, TestCase{
						TestRawDataPath: source, Prefix: c.Name, Name: m.Name, TestSuiteName: s.Name,
						Status: testNGStatus(m.Status), DurationInNanoSeconds: durationNanos(ms / 1e3),
						Message: strings.TrimSpace(m.Message),
					})
				}
			}
		}
	}

	return out, nil
}

// Cucumber JSON: one test case per scenario.

type cucumberFeature struct {
	Name     string `json:"name"`
	URI      string `json:"uri"`
	Elements []struct {
		Name  string `json:"name"`
		Type  string `json:"type"`
		Steps []struct {
			Result struct {
				Status       string  `json:"status"`
				Duration     float64 `json:"duration"`
				ErrorMessage string  `json:"error_message"`
			} `json:"result"`
		} `json:"steps"`
	} `json:"elements"`
}

func parseCucumber(data []byte, source string) ([]TestCase, error) {
	var features []cucumberFeature
	if err := json.Unmarshal(data, &features); err != nil {
		return nil, fmt.Errorf("parse cucumber %s: %w", source, err)
	}

	var out []TestCase

	for _, f := range features {
		for _, e := range f.Elements {
			if e.Type == "background" {
				continue
			}

			tc := TestCase{
				TestRawDataPath: source, Prefix: f.URI, Name: e.Name, TestSuiteName: f.Name,
				Status: testStatusSucceeded,
			}

			for _, st := range e.Steps {
				tc.DurationInNanoSeconds += int64(st.Result.Duration)
				tc.Status, tc.Message = cucumberStep(tc.Status, tc.Message, st.Result.Status, st.Result.ErrorMessage)
			}

			out = append(out, tc)
		}
	}

	return out, nil
}

func cucumberStep(cur, msg, step, errMsg string) (status, message string) {
	switch strings.ToLower(step) {
	case "failed":
		if cur != testStatusFailed {
			return testStatusFailed, strings.TrimSpace(errMsg)
		}
	case "skipped", "pending", "undefined", "ambiguous":
		if cur == testStatusSucceeded {
			return testStatusSkipped, msg
		}
	}

	return cur, msg
}

// Visual Studio TRX.

type trxDoc struct {
	Defs []struct {
		ID     string `xml:"id,attr"`
		Method struct {
			ClassName string `xml:"className,attr"`
		} `xml:"TestMethod"`
	} `xml:"TestDefinitions>UnitTest"`
	Results []struct {
		TestID   string `xml:"testId,attr"`
		Name     string `xml:"testName,attr"`
		Outcome  string `xml:"outcome,attr"`
		Duration string `xml:"duration,attr"`
		Message  string `xml:"Output>ErrorInfo>Message"`
	} `xml:"Results>UnitTestResult"`
}

func trxStatus(o string) string {
	switch strings.ToLower(o) {
	case "passed":
		return testStatusSucceeded
	case "failed":
		return testStatusFailed
	case "error", "timeout", "aborted":
		return testStatusError
	case "notexecuted", "inconclusive", "pending":
		return testStatusSkipped
	default:
		return testStatusUnknown
	}
}

// parseClock parses TRX durations of the form hh:mm:ss.fffffff.
func parseClock(s string) int64 {
	parts := strings.Split(strings.TrimSpace(s), ":")
	if len(parts) != clockParts {
		return 0
	}

	h, _ := strconv.Atoi(parts[0])
	m, _ := strconv.Atoi(parts[1])
	sec, _ := strconv.ParseFloat(parts[2], 64)

	return int64(time.Duration(h)*time.Hour+time.Duration(m)*time.Minute) + durationNanos(sec)
}

func parseTRX(data []byte, source string) ([]TestCase, error) {
	var doc trxDoc
	if err := xml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse trx %s: %w", source, err)
	}

	classes := map[string]string{}
	for _, d := range doc.Defs {
		classes[d.ID] = d.Method.ClassName
	}

	out := make([]TestCase, 0, len(doc.Results))
	for _, r := range doc.Results {
		class := classes[r.TestID]
		out = append(out, TestCase{
			TestRawDataPath: source, Prefix: class, Name: r.Name, TestSuiteName: class,
			Status: trxStatus(r.Outcome), DurationInNanoSeconds: parseClock(r.Duration),
			Message: strings.TrimSpace(r.Message),
		})
	}

	return out, nil
}

// JaCoCo XML: LINE and BRANCH counters per sourcefile.

type jacocoDoc struct {
	Packages []struct {
		Name    string `xml:"name,attr"`
		Sources []struct {
			Name     string `xml:"name,attr"`
			Counters []struct {
				Type    string `xml:"type,attr"`
				Missed  int32  `xml:"missed,attr"`
				Covered int32  `xml:"covered,attr"`
			} `xml:"counter"`
		} `xml:"sourcefile"`
	} `xml:"package"`
}

func parseJaCoCo(data []byte, source string) ([]CodeCoverage, error) {
	var doc jacocoDoc
	if err := xml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse jacoco %s: %w", source, err)
	}

	files := map[string]*fileCoverage{}

	for _, p := range doc.Packages {
		for _, s := range p.Sources {
			name := s.Name
			if p.Name != "" {
				name = p.Name + "/" + s.Name
			}

			fc := fileEntry(files, name)

			for _, c := range s.Counters {
				switch c.Type {
				case "LINE":
					fc.covered += c.Covered
					fc.missed += c.Missed
				case "BRANCH":
					fc.bCovered += c.Covered
					fc.bMissed += c.Missed
				}
			}
		}
	}

	return coverageRows(files), nil
}

// Clover XML: stmt and cond lines per file.

type cloverFile struct {
	Name  string `xml:"name,attr"`
	Path  string `xml:"path,attr"`
	Lines []struct {
		Type       string `xml:"type,attr"`
		Count      int    `xml:"count,attr"`
		TrueCount  int    `xml:"truecount,attr"`
		FalseCount int    `xml:"falsecount,attr"`
	} `xml:"line"`
}

type cloverDoc struct {
	Files    []cloverFile `xml:"project>file"`
	PkgFiles []cloverFile `xml:"project>package>file"`
}

func parseClover(data []byte, source string) ([]CodeCoverage, error) {
	var doc cloverDoc
	if err := xml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse clover %s: %w", source, err)
	}

	files := map[string]*fileCoverage{}

	for _, f := range append(doc.Files, doc.PkgFiles...) {
		name := f.Path
		if name == "" {
			name = f.Name
		}

		fc := fileEntry(files, name)

		for _, l := range f.Lines {
			switch l.Type {
			case cloverTypeStmt:
				fc.addLine(l.Count > 0)
			case cloverTypeCond:
				fc.addLine(l.Count > 0 || l.TrueCount > 0 || l.FalseCount > 0)
				fc.addBranch(l.TrueCount > 0)
				fc.addBranch(l.FalseCount > 0)
			}
		}
	}

	return coverageRows(files), nil
}

// SimpleCov JSON (.resultset.json): suite -> coverage -> file -> lines/branches.

type simpleCovFile struct {
	Lines    []*int                    `json:"lines"`
	Branches map[string]map[string]int `json:"branches"`
}

func parseSimpleCov(data []byte, source string) ([]CodeCoverage, error) {
	var doc map[string]struct {
		Coverage map[string]json.RawMessage `json:"coverage"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse simplecov %s: %w", source, err)
	}

	files := map[string]*fileCoverage{}

	for _, suite := range doc {
		for name, raw := range suite.Coverage {
			var f simpleCovFile
			if err := json.Unmarshal(raw, &f.Lines); err != nil {
				if err = json.Unmarshal(raw, &f); err != nil {
					return nil, fmt.Errorf("parse simplecov %s: file %s: %w", source, name, err)
				}
			}

			fc := fileEntry(files, name)

			for _, l := range f.Lines {
				if l != nil {
					fc.addLine(*l > 0)
				}
			}

			for _, branches := range f.Branches {
				for _, hits := range branches {
					fc.addBranch(hits > 0)
				}
			}
		}
	}

	return coverageRows(files), nil
}
