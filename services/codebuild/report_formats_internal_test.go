package codebuild

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	cbtypes "github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	nunit2Sample = `<test-results><test-suite name="Suite"><results>
<test-case name="Ns.Calc.Add" executed="True" result="Success" time="0.5"/>
<test-case name="Ns.Calc.Div" executed="True" result="Failure"
time="1"><failure><message><![CDATA[div by zero]]></message></failure></test-case>
<test-case name="Ns.Calc.Skip" executed="False"
result="Ignored"><reason><message>later</message></reason></test-case>
</results></test-suite></test-results>`

	nunit3Sample = `<test-run><test-suite name="Outer"><test-suite name="Inner">
<test-case name="Add" classname="Ns.Calc" result="Passed" duration="0.25"/>
<test-case name="Boom" classname="Ns.Calc" result="Failed" label="Error"
duration="1"><failure><message>kaboom</message></failure></test-case>
<test-case name="Odd" classname="Ns.Calc" result="Inconclusive"/>
</test-suite></test-suite></test-run>`

	testNGSample = `<testng-results><suite name="S"><test name="T"><class name="pkg.C">
<test-method status="PASS" name="ok" duration-ms="1500"/>
<test-method status="FAIL" name="bad" duration-ms="10"><exception
class="E"><message><![CDATA[assert failed]]></message></exception></test-method>
<test-method status="SKIP" name="skipped" duration-ms="0"/>
<test-method status="PASS" name="setup" is-config="true" duration-ms="5"/>
</class></test></suite></testng-results>`

	cucumberSample = `[{"name":"Feature A","uri":"features/a.feature","elements":[
{"name":"bg","type":"background","steps":[{"result":{"status":"passed","duration":1}}]},
{"name":"good","type":"scenario","steps":[{"result":{"status":"passed",
"duration":1000}},{"result":{"status":"passed","duration":500}}]},
{"name":"bad","type":"scenario","steps":[{"result":{"status":"passed",
"duration":1}},{"result":{"status":"failed","duration":2,"error_message":"nope"}}]},
{"name":"pend","type":"scenario","steps":[{"result":{"status":"pending","duration":0}}]}]}]`

	trxSample = `<TestRun xmlns="http://microsoft.com/schemas/VisualStudio/TeamTest/2010">
<Results>
<UnitTestResult testId="1" testName="Add" outcome="Passed" duration="00:00:01.5000000"/>
<UnitTestResult testId="2" testName="Div" outcome="Failed"
duration="00:00:00.2500000"><Output><ErrorInfo><Message>bad div</Message></ErrorInfo></Output></UnitTestResult>
<UnitTestResult testId="3" testName="Later" outcome="NotExecuted" duration="00:00:00"/>
</Results>
<TestDefinitions><UnitTest id="1"><TestMethod className="Ns.Calc"/></UnitTest><UnitTest
id="2"><TestMethod className="Ns.Calc"/></UnitTest></TestDefinitions>
</TestRun>`

	jacocoSample = `<report name="r"><package name="com/x"><sourcefile name="A.java">
<counter type="INSTRUCTION" missed="9" covered="9"/><counter type="LINE" missed="1"
covered="3"/><counter type="BRANCH" missed="1" covered="1"/>
</sourcefile><sourcefile name="B.java"><counter type="LINE" missed="0"
covered="2"/></sourcefile></package></report>`

	cloverSample = `<coverage><project><package name="p"><file name="a.js" path="src/a.js">
<line num="1" type="stmt" count="3"/><line num="2" type="stmt" count="0"/>
<line num="3" type="cond" count="1" truecount="1" falsecount="0"/><line num="4" type="method" count="1"/>
</file></package></project></coverage>`

	simpleCovSample = `{"RSpec":{"coverage":{
"lib/a.rb":{"lines":[null,1,0,2],"branches":{"[:if, 0, 1, 0]":{"[:then, 1, 2, 0]":1,"[:else, 2, 3, 0]":0}}},
"lib/b.rb":[null,1,1]}}}`
)

type wantCase struct{ name, status string }

func TestEngineExtraReportFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantCov   map[string][4]int32
		name      string
		format    string
		file      string
		wantState cbtypes.ReportStatusType
		wantCases []wantCase
		wantTotal int32
	}{
		{
			name: "nunit2", format: "NUNITXML", file: nunit2Sample, wantState: cbtypes.ReportStatusTypeFailed,
			wantCases: []wantCase{{"Add", "SUCCEEDED"}, {"Div", "FAILED"}, {"Skip", "SKIPPED"}},
		},
		{
			name: "nunit3", format: "NUNIT3XML", file: nunit3Sample, wantState: cbtypes.ReportStatusTypeFailed,
			wantCases: []wantCase{{"Add", "SUCCEEDED"}, {"Boom", "ERROR"}, {"Odd", "UNKNOWN"}},
		},
		{
			name: "testng", format: "TESTNGXML", file: testNGSample, wantState: cbtypes.ReportStatusTypeFailed,
			wantCases: []wantCase{{"ok", "SUCCEEDED"}, {"bad", "FAILED"}, {"skipped", "SKIPPED"}},
		},
		{
			name: "cucumber", format: "CUCUMBERJSON", file: cucumberSample, wantState: cbtypes.ReportStatusTypeFailed,
			wantCases: []wantCase{{"good", "SUCCEEDED"}, {"bad", "FAILED"}, {"pend", "SKIPPED"}},
		},
		{
			name: "trx", format: "VISUALSTUDIOTRX", file: trxSample, wantState: cbtypes.ReportStatusTypeFailed,
			wantCases: []wantCase{{"Add", "SUCCEEDED"}, {"Div", "FAILED"}, {"Later", "SKIPPED"}},
		},
		{
			name: "jacoco", format: "JACOCOXML", file: jacocoSample, wantState: cbtypes.ReportStatusTypeSucceeded,
			wantCov: map[string][4]int32{"com/x/A.java": {3, 1, 1, 1}, "com/x/B.java": {2, 0, 0, 0}},
		},
		{
			name: "clover", format: "CLOVERXML", file: cloverSample, wantState: cbtypes.ReportStatusTypeSucceeded,
			wantCov: map[string][4]int32{"src/a.js": {2, 1, 1, 1}},
		},
		{
			name: "simplecov", format: "SIMPLECOV", file: simpleCovSample, wantState: cbtypes.ReportStatusTypeSucceeded,
			wantCov: map[string][4]int32{"lib/a.rb": {2, 1, 1, 1}, "lib/b.rb": {2, 0, 0, 0}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := newFakeRuntime(writeFilesRule(map[string]string{"out/r.dat": tt.file}))
			h := newHarnessWith(t, rt, nil)
			c := sdkClientFor(t, h.b)
			createSDKProject(t, c, sdkProject{source: &cbtypes.ProjectSource{
				Type: cbtypes.SourceTypeNoSource, Buildspec: aws.String(reportSpec("g", tt.format, "r.dat")),
			}})
			startAndWait(t, h, c, &codebuildsdk.StartBuildInput{})

			arns := h.b.ListReports("")
			require.Len(t, arns, 1)

			got, err := c.BatchGetReports(t.Context(), &codebuildsdk.BatchGetReportsInput{ReportArns: arns})
			require.NoError(t, err)
			assert.Equal(t, tt.wantState, got.Reports[0].Status)

			if tt.wantCases != nil {
				out, derr := c.DescribeTestCases(
					t.Context(),
					&codebuildsdk.DescribeTestCasesInput{ReportArn: aws.String(arns[0])},
				)
				require.NoError(t, derr)

				have := map[string]string{}
				for _, tc := range out.TestCases {
					have[aws.ToString(tc.Name)] = aws.ToString(tc.Status)
				}

				want := map[string]string{}
				for _, w := range tt.wantCases {
					want[w.name] = w.status
				}

				assert.Equal(t, want, have)

				return
			}

			out, derr := c.DescribeCodeCoverages(
				t.Context(),
				&codebuildsdk.DescribeCodeCoveragesInput{ReportArn: aws.String(arns[0])},
			)
			require.NoError(t, derr)

			have := map[string][4]int32{}
			for _, cc := range out.CodeCoverages {
				have[aws.ToString(cc.FilePath)] = [4]int32{
					aws.ToInt32(cc.LinesCovered), aws.ToInt32(cc.LinesMissed),
					aws.ToInt32(cc.BranchesCovered), aws.ToInt32(cc.BranchesMissed),
				}
			}

			assert.Equal(t, tt.wantCov, have)
		})
	}
}

func TestReportFormatDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		format   string
		file     string
		caseName string
		wantMsg  string
		wantPfx  string
		wantNs   int64
	}{
		{"nunit2_fail", formatNUnit, nunit2Sample, "Div", "div by zero", "Ns.Calc", 1e9},
		{"nunit3_err", formatNUnit3, nunit3Sample, "Boom", "kaboom", "Ns.Calc", 1e9},
		{"testng_fail", formatTestNG, testNGSample, "bad", "assert failed", "pkg.C", 1e7},
		{"testng_ok", formatTestNG, testNGSample, "ok", "", "pkg.C", 1.5e9},
		{"cucumber_fail", formatCucumber, cucumberSample, "bad", "nope", "features/a.feature", 3},
		{"trx_fail", formatTRX, trxSample, "Div", "bad div", "Ns.Calc", 2.5e8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p, ok := reportParserFor(tt.format)
			require.True(t, ok)

			cases, err := p.tests([]byte(tt.file), "r.dat")
			require.NoError(t, err)

			for _, c := range cases {
				if c.Name == tt.caseName {
					assert.Equal(t, tt.wantMsg, c.Message)
					assert.Equal(t, tt.wantNs, c.DurationInNanoSeconds)
					assert.Equal(t, tt.wantPfx, c.Prefix)

					return
				}
			}

			t.Fatalf("case %s not parsed", tt.caseName)
		})
	}
}

func TestReportFormatsRejectGarbage(t *testing.T) {
	t.Parallel()

	for _, f := range []string{
		formatNUnit, formatNUnit3, formatTestNG, formatCucumber, formatTRX, formatClover, formatJaCoCo, formatSimpleCov,
	} {
		t.Run(f, func(t *testing.T) {
			t.Parallel()

			p, ok := reportParserFor(f)
			require.True(t, ok)

			var err error
			if p.tests != nil {
				_, err = p.tests([]byte("not a report"), "x")
			} else {
				_, err = p.coverage([]byte("not a report"), "x")
			}

			require.Error(t, err)
		})
	}
}
