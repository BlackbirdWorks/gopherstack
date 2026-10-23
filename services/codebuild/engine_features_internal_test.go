package codebuild

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	cbtypes "github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

type streamRuntime struct {
	*fakeRuntime
	emitted chan struct{}
	release chan struct{}
}

func (s *streamRuntime) ExecStream(ctx context.Context, id string, cmd []string, out io.Writer) (int, error) {
	if !strings.Contains(cmd[len(cmd)-1], "BUILD_CMD") {
		res, err := s.Exec(ctx, id, cmd)
		_, _ = io.WriteString(out, res.Stdout+res.Stderr)

		return res.ExitCode, err
	}

	_, _ = io.WriteString(out, "line one\npart")
	s.emitted <- struct{}{}

	select {
	case <-s.release:
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	_, _ = io.WriteString(out, "ial\nline two\n")

	return 0, nil
}

var _ container.StreamExecer = (*streamRuntime)(nil)

func TestEngineLogsStreamWhileRunning(t *testing.T) {
	t.Parallel()

	rt := &streamRuntime{fakeRuntime: newFakeRuntime(), emitted: make(chan struct{}, 1), release: make(chan struct{})}
	h := newHarnessWith(t, rt, nil)
	c := sdkClientFor(t, h.b)
	createSDKProject(t, c, sdkProject{source: &cbtypes.ProjectSource{
		Type: cbtypes.SourceTypeNoSource, Buildspec: aws.String(specAllPhases),
	}})

	out, err := c.StartBuild(t.Context(), &codebuildsdk.StartBuildInput{ProjectName: aws.String("proj")})
	require.NoError(t, err)

	<-rt.emitted

	partial := h.logs.joined()
	assert.Contains(t, partial, "line one")
	assert.NotContains(t, partial, "line two")
	assert.NotContains(t, partial, "part")

	mid, err := c.BatchGetBuilds(
		t.Context(),
		&codebuildsdk.BatchGetBuildsInput{Ids: []string{aws.ToString(out.Build.Id)}},
	)
	require.NoError(t, err)
	assert.Equal(t, cbtypes.StatusTypeInProgress, mid.Builds[0].BuildStatus)

	close(rt.release)
	h.b.WaitBuilds()

	full := h.logs.joined()
	assert.Less(t, strings.Index(full, "line one"), strings.Index(full, "partial"))
	assert.Less(t, strings.Index(full, "partial"), strings.Index(full, "line two"))
}

const junitMixed = `<testsuites><testsuite name="suiteA" tests="4">
<testcase classname="pkg.A" name="passes" time="0.5"/>
<testcase classname="pkg.A" name="fails" time="1"><failure message="expected 1 got 2">trace</failure></testcase>
<testcase classname="pkg.B" name="skips"><skipped/></testcase>
<testcase classname="pkg.B" name="errors" time="0.25"><error>boom</error></testcase>
</testsuite></testsuites>`

const junitPassing = `<testsuite name="s"><testcase classname="c" name="ok" time="2"/></testsuite>`

const coberturaSample = `<coverage line-rate="0.8" branch-rate="0.5"><packages><package name="p"><classes>
<class name="A" filename="src/a.go"><lines>
<line number="1" hits="1"/><line number="2" hits="0"/>
<line number="3" hits="2" branch="true" condition-coverage="50% (1/2)"/>
</lines></class>
<class name="B" filename="src/b.go"><lines><line number="1" hits="1"/><line number="2" hits="1"/></lines></class>
</classes></package></packages></coverage>`

func reportSpec(group, format, files string) string {
	return "version: 0.2\nphases:\n  build:\n    commands: [echo BUILD_CMD]\nreports:\n  " + group +
		":\n    files: [" + files + "]\n    base-directory: out\n    file-format: " + format + "\n"
}

func writeFilesRule(files map[string]string) fakeRule {
	return fakeRule{match: "BUILD_CMD", create: func(dir string) {
		for name, body := range files {
			full := filepath.Join(dir, filepath.FromSlash(name))
			_ = os.MkdirAll(filepath.Dir(full), 0o750)
			_ = os.WriteFile(full, []byte(body), 0o600)
		}
	}}
}

func TestEngineTestReports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantCounts map[string]int32
		name       string
		file       string
		format     string
		wantStatus cbtypes.ReportStatusType
		wantFailed int
		wantTotal  int32
	}{
		{
			name:       "mixed",
			file:       junitMixed,
			format:     "JUNITXML",
			wantStatus: cbtypes.ReportStatusTypeFailed,
			wantTotal:  4,
			wantCounts: map[string]int32{"SUCCEEDED": 1, "FAILED": 1, "SKIPPED": 1, "ERROR": 1},
			wantFailed: 1,
		},
		{
			name: "passing", file: junitPassing, format: "JUNITXML", wantStatus: cbtypes.ReportStatusTypeSucceeded,
			wantTotal: 1, wantCounts: map[string]int32{"SUCCEEDED": 1},
		},
		{name: "unsupported_format", file: "<x/>", format: "TESTNGXML", wantStatus: cbtypes.ReportStatusTypeIncomplete},
		{name: "unparseable", file: "not xml", format: "JUNITXML", wantStatus: cbtypes.ReportStatusTypeFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := newFakeRuntime(writeFilesRule(map[string]string{"out/results/r.xml": tt.file}))
			h := newHarnessWith(t, rt, nil)
			c := sdkClientFor(t, h.b)
			createSDKProject(t, c, sdkProject{source: &cbtypes.ProjectSource{
				Type:      cbtypes.SourceTypeNoSource,
				Buildspec: aws.String(reportSpec("unit", tt.format, "'results/*.xml'")),
			}})

			b := startAndWait(t, h, c, &codebuildsdk.StartBuildInput{})
			require.Equal(t, cbtypes.StatusTypeSucceeded, b.BuildStatus, failedPhaseMessage(b))

			groups, err := c.ListReportGroups(t.Context(), &codebuildsdk.ListReportGroupsInput{})
			require.NoError(t, err)
			require.Len(t, groups.ReportGroups, 1)
			assert.Contains(t, groups.ReportGroups[0], "report-group/proj-unit")

			lr, err := c.ListReportsForReportGroup(t.Context(), &codebuildsdk.ListReportsForReportGroupInput{
				ReportGroupArn: aws.String(groups.ReportGroups[0]),
			})
			require.NoError(t, err)
			require.Len(t, lr.Reports, 1)

			got, err := c.BatchGetReports(t.Context(), &codebuildsdk.BatchGetReportsInput{ReportArns: lr.Reports})
			require.NoError(t, err)
			rep := got.Reports[0]
			assert.Equal(t, tt.wantStatus, rep.Status)
			assert.Equal(t, cbtypes.ReportTypeTest, rep.Type)
			assert.Equal(t, aws.ToString(b.Arn), aws.ToString(rep.ExecutionId))

			if tt.wantCounts != nil {
				assert.Equal(t, tt.wantTotal, aws.ToInt32(rep.TestSummary.Total))
				assert.Equal(t, tt.wantCounts, rep.TestSummary.StatusCounts)
			}

			cases, err := c.DescribeTestCases(t.Context(), &codebuildsdk.DescribeTestCasesInput{ReportArn: rep.Arn})
			require.NoError(t, err)
			assert.Len(t, cases.TestCases, int(tt.wantTotal))

			failed, err := c.DescribeTestCases(t.Context(), &codebuildsdk.DescribeTestCasesInput{
				ReportArn: rep.Arn, Filter: &cbtypes.TestCaseFilter{Status: aws.String("FAILED")},
			})
			require.NoError(t, err)
			assert.Len(t, failed.TestCases, tt.wantFailed)
		})
	}
}

func TestEngineTestCaseDetails(t *testing.T) {
	t.Parallel()

	rt := newFakeRuntime(writeFilesRule(map[string]string{"out/r.xml": junitMixed}))
	h := newHarnessWith(t, rt, nil)
	c := sdkClientFor(t, h.b)
	createSDKProject(t, c, sdkProject{source: &cbtypes.ProjectSource{
		Type: cbtypes.SourceTypeNoSource, Buildspec: aws.String(reportSpec("unit", "JUNITXML", "r.xml")),
	}})
	startAndWait(t, h, c, &codebuildsdk.StartBuildInput{})

	arns := h.b.ListReports("")
	require.Len(t, arns, 1)

	out, err := c.DescribeTestCases(t.Context(), &codebuildsdk.DescribeTestCasesInput{
		ReportArn: aws.String(arns[0]), Filter: &cbtypes.TestCaseFilter{Keyword: aws.String("pkg.A")},
	})
	require.NoError(t, err)
	require.Len(t, out.TestCases, 2)

	var failedCase cbtypes.TestCase

	for _, tc := range out.TestCases {
		if aws.ToString(tc.Name) == "fails" {
			failedCase = tc
		}
	}

	assert.Equal(t, "FAILED", aws.ToString(failedCase.Status))
	assert.Equal(t, "expected 1 got 2", aws.ToString(failedCase.Message))
	assert.Equal(t, "suiteA", aws.ToString(failedCase.TestSuiteName))
	assert.Equal(t, "pkg.A", aws.ToString(failedCase.Prefix))
	assert.Equal(t, int64(1_000_000_000), aws.ToInt64(failedCase.DurationInNanoSeconds))
}

func TestEngineCoverageReports(t *testing.T) {
	t.Parallel()

	rt := newFakeRuntime(writeFilesRule(map[string]string{"out/cov.xml": coberturaSample}))
	h := newHarnessWith(t, rt, nil)
	c := sdkClientFor(t, h.b)
	createSDKProject(t, c, sdkProject{source: &cbtypes.ProjectSource{
		Type: cbtypes.SourceTypeNoSource, Buildspec: aws.String(reportSpec("cov", "COBERTURAXML", "cov.xml")),
	}})
	startAndWait(t, h, c, &codebuildsdk.StartBuildInput{})

	arns := h.b.ListReports("")
	require.Len(t, arns, 1)

	rep, err := c.BatchGetReports(t.Context(), &codebuildsdk.BatchGetReportsInput{ReportArns: arns})
	require.NoError(t, err)
	sum := rep.Reports[0].CodeCoverageSummary
	assert.Equal(t, cbtypes.ReportTypeCodeCoverage, rep.Reports[0].Type)
	assert.Equal(t, cbtypes.ReportStatusTypeSucceeded, rep.Reports[0].Status)
	assert.Equal(t, int32(4), aws.ToInt32(sum.LinesCovered))
	assert.Equal(t, int32(1), aws.ToInt32(sum.LinesMissed))
	assert.InDelta(t, 80.0, aws.ToFloat64(sum.LineCoveragePercentage), 0.001)
	assert.Equal(t, int32(1), aws.ToInt32(sum.BranchesCovered))
	assert.Equal(t, int32(1), aws.ToInt32(sum.BranchesMissed))

	tests := []struct {
		in        codebuildsdk.DescribeCodeCoveragesInput
		name      string
		wantFiles []string
	}{
		{name: "default_by_path", wantFiles: []string{"src/a.go", "src/b.go"}},
		{
			name: "by_line_desc", wantFiles: []string{"src/b.go", "src/a.go"},
			in: codebuildsdk.DescribeCodeCoveragesInput{
				SortBy:    cbtypes.ReportCodeCoverageSortByTypeLineCoveragePercentage,
				SortOrder: cbtypes.SortOrderTypeDescending,
			},
		},
		{
			name: "min_filter", wantFiles: []string{"src/b.go"},
			in: codebuildsdk.DescribeCodeCoveragesInput{MinLineCoveragePercentage: aws.Float64(90)},
		},
		{
			name: "max_filter", wantFiles: []string{"src/a.go"},
			in: codebuildsdk.DescribeCodeCoveragesInput{MaxLineCoveragePercentage: aws.Float64(90)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := tt.in
			in.ReportArn = aws.String(arns[0])

			out, derr := c.DescribeCodeCoverages(t.Context(), &in)
			require.NoError(t, derr)

			files := make([]string, 0, len(out.CodeCoverages))
			for _, cc := range out.CodeCoverages {
				files = append(files, aws.ToString(cc.FilePath))
			}

			assert.Equal(t, tt.wantFiles, files)
		})
	}

	all, err := c.DescribeCodeCoverages(
		t.Context(),
		&codebuildsdk.DescribeCodeCoveragesInput{ReportArn: aws.String(arns[0])},
	)
	require.NoError(t, err)
	assert.InDelta(t, 66.67, aws.ToFloat64(all.CodeCoverages[0].LineCoveragePercentage), 0.001)
	assert.Equal(t, int32(2), aws.ToInt32(all.CodeCoverages[0].LinesCovered))
	assert.Equal(t, int32(1), aws.ToInt32(all.CodeCoverages[0].BranchesMissed))
}

func TestEngineReportGroupResolution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		groupType  cbtypes.ReportType
		wantReport bool
	}{
		{name: "existing_matching_type", groupType: cbtypes.ReportTypeTest, wantReport: true},
		{name: "existing_wrong_type", groupType: cbtypes.ReportTypeCodeCoverage},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := newFakeRuntime(writeFilesRule(map[string]string{"out/r.xml": junitPassing}))
			h := newHarnessWith(t, rt, nil)
			c := sdkClientFor(t, h.b)

			rg, err := c.CreateReportGroup(t.Context(), &codebuildsdk.CreateReportGroupInput{
				Name: aws.String("shared"), Type: tt.groupType,
				ExportConfig: &cbtypes.ReportExportConfig{ExportConfigType: cbtypes.ReportExportConfigTypeNoExport},
			})
			require.NoError(t, err)

			spec := reportSpec(aws.ToString(rg.ReportGroup.Arn), "JUNITXML", "r.xml")
			createSDKProject(t, c, sdkProject{source: &cbtypes.ProjectSource{
				Type: cbtypes.SourceTypeNoSource, Buildspec: aws.String(spec),
			}})

			b := startAndWait(t, h, c, &codebuildsdk.StartBuildInput{})
			assert.Equal(t, cbtypes.StatusTypeSucceeded, b.BuildStatus)

			lr, err := c.ListReportsForReportGroup(t.Context(), &codebuildsdk.ListReportsForReportGroupInput{
				ReportGroupArn: rg.ReportGroup.Arn,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantReport, len(lr.Reports) == 1)
		})
	}
}

func TestEngineReportNoMatchingFiles(t *testing.T) {
	t.Parallel()

	h := newHarnessWith(t, newFakeRuntime(), nil)
	c := sdkClientFor(t, h.b)
	createSDKProject(t, c, sdkProject{source: &cbtypes.ProjectSource{
		Type: cbtypes.SourceTypeNoSource, Buildspec: aws.String(reportSpec("unit", "JUNITXML", "missing.xml")),
	}})

	b := startAndWait(t, h, c, &codebuildsdk.StartBuildInput{})
	assert.Equal(t, cbtypes.StatusTypeSucceeded, b.BuildStatus)
	assert.Empty(t, h.b.ListReports(""))
	assert.Contains(t, h.logs.joined(), "no matching report paths found")
}

const cacheSpec = `version: 0.2
phases:
  build:
    commands: [echo BUILD_CMD]
cache:
  paths:
    - 'deps/**/*'
`

type cacheObservation struct {
	lock    *lockmetrics.RWMutex
	existed []bool
	mode    os.FileMode
}

func cacheObserver() (*cacheObservation, fakeRule) {
	obs := &cacheObservation{lock: lockmetrics.New("cache")}

	return obs, fakeRule{match: "BUILD_CMD", create: func(dir string) {
		st, err := os.Stat(filepath.Join(dir, "deps", "bin", "tool"))

		obs.lock.Lock("obs")
		obs.existed = append(obs.existed, err == nil)

		if err == nil {
			obs.mode = st.Mode().Perm()
		}

		obs.lock.Unlock()

		_ = os.MkdirAll(filepath.Join(dir, "deps", "bin"), 0o750)
		_ = os.WriteFile(
			filepath.Join(dir, "deps", "bin", "tool"),
			[]byte("x"),
			0o755,
		)
	}}
}

func TestEngineCustomCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cache      *cbtypes.ProjectCache
		name       string
		wantReused bool
	}{
		{
			name:       "local_custom",
			cache:      localCache(cbtypes.CacheModeLocalCustomCache),
			wantReused: true,
		},
		{
			name:  "local_without_custom_mode",
			cache: localCache(cbtypes.CacheModeLocalSourceCache),
		},
		{name: "no_cache", cache: &cbtypes.ProjectCache{Type: cbtypes.CacheTypeNoCache}},
		{
			name:       "s3",
			cache:      &cbtypes.ProjectCache{Type: cbtypes.CacheTypeS3, Location: aws.String("cachebucket/pfx")},
			wantReused: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			obs, rule := cacheObserver()
			h := newHarnessWith(t, newFakeRuntime(rule), nil)
			c := sdkClientFor(t, h.b)
			createSDKProject(t, c, sdkProject{cache: tt.cache, source: &cbtypes.ProjectSource{
				Type: cbtypes.SourceTypeNoSource, Buildspec: aws.String(cacheSpec),
			}})

			for range 2 {
				b := startAndWait(t, h, c, &codebuildsdk.StartBuildInput{})
				require.Equal(t, cbtypes.StatusTypeSucceeded, b.BuildStatus, failedPhaseMessage(b))
			}

			obs.lock.RLock("read")
			defer obs.lock.RUnlock()

			require.Equal(t, []bool{false, tt.wantReused}, obs.existed)

			if tt.wantReused {
				assert.NotZero(t, obs.mode&0o100, "exec bit must survive the cache")
			}

			if tt.cache.Type == cbtypes.CacheTypeS3 {
				assert.Contains(t, h.store.keys(), "cachebucket/pfx/proj/cache.tar.gz")
			}
		})
	}
}

func TestEngineS3CacheSharedAcrossHosts(t *testing.T) {
	t.Parallel()

	cache := &cbtypes.ProjectCache{Type: cbtypes.CacheTypeS3, Location: aws.String("cachebucket/pfx")}
	obs, rule := cacheObserver()
	first := newHarnessWith(t, newFakeRuntime(rule), nil)
	second := newHarnessWith(t, newFakeRuntime(rule), nil)
	second.b.SetEngineServices(EngineServices{Store: first.store, Logs: second.logs, Values: fakeValues{}})

	for _, h := range []*engineHarness{first, second} {
		c := sdkClientFor(t, h.b)
		createSDKProject(t, c, sdkProject{cache: cache, source: &cbtypes.ProjectSource{
			Type: cbtypes.SourceTypeNoSource, Buildspec: aws.String(cacheSpec),
		}})
		startAndWait(t, h, c, &codebuildsdk.StartBuildInput{})
	}

	obs.lock.RLock("read")
	defer obs.lock.RUnlock()

	assert.Equal(t, []bool{false, true}, obs.existed)
}

func TestEngineAbsoluteCachePathsArchivedInContainer(t *testing.T) {
	t.Parallel()

	spec := "version: 0.2\nphases:\n  build:\n    commands: [echo BUILD_CMD]\ncache:\n  paths:\n    - '/root/.m2/**/*'\n"
	rt := newFakeRuntime()
	h := newHarnessWith(t, rt, nil)
	c := sdkClientFor(t, h.b)
	createSDKProject(t, c, sdkProject{
		cache: &cbtypes.ProjectCache{
			Type:  cbtypes.CacheTypeLocal,
			Modes: []cbtypes.CacheMode{cbtypes.CacheModeLocalCustomCache},
		},
		source: &cbtypes.ProjectSource{Type: cbtypes.SourceTypeNoSource, Buildspec: aws.String(spec)},
	})

	b := startAndWait(t, h, c, &codebuildsdk.StartBuildInput{})
	require.Equal(t, cbtypes.StatusTypeSucceeded, b.BuildStatus)
	assert.True(t, rt.ran("tar -cf /codebuild/cache/abs.tar -C /"))
	assert.True(t, rt.ran("'root/.m2'"))
}

func TestEngineLocalDockerLayerCacheMount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		privileged bool
		wantMount  bool
	}{
		{name: "privileged", privileged: true, wantMount: true},
		{name: "unprivileged"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := newFakeRuntime()
			h := newHarnessWith(t, rt, nil)
			h.project(t, ProjectConfig{
				Source: &ProjectSource{Type: "NO_SOURCE", Buildspec: specAllPhases},
				Cache:  &ProjectCache{Type: "LOCAL", Modes: []string{"LOCAL_DOCKER_LAYER_CACHE"}},
				Environment: &ProjectEnvironment{
					Type: "LINUX_CONTAINER", Image: "aws/codebuild/standard:7.0",
					ComputeType: "BUILD_GENERAL1_SMALL", PrivilegedMode: tt.privileged,
				},
			})
			h.run(t, StartBuildConfig{})

			found := false

			for _, m := range rt.mounts {
				found = found || strings.HasSuffix(m, ":/var/lib/docker")
			}

			assert.Equal(t, tt.wantMount, found)
		})
	}
}

func TestEngineLocalSourceCache(t *testing.T) {
	t.Parallel()

	repo := newTestRepo(t)
	cacheDir := t.TempDir()
	obs, rule := gitObserver()
	h := newHarnessWith(t, newFakeRuntime(rule), func(c *EngineConfig) {
		c.AllowFileGitSources = true
		c.CacheDir = cacheDir
	})
	c := sdkClientFor(t, h.b)
	createSDKProject(t, c, sdkProject{
		cache: &cbtypes.ProjectCache{
			Type:  cbtypes.CacheTypeLocal,
			Modes: []cbtypes.CacheMode{cbtypes.CacheModeLocalSourceCache},
		},
		source: &cbtypes.ProjectSource{Type: cbtypes.SourceTypeGithub, Location: aws.String("file://" + repo.dir)},
	})

	for range 2 {
		b := startAndWait(t, h, c, &codebuildsdk.StartBuildInput{})
		require.Equal(t, cbtypes.StatusTypeSucceeded, b.BuildStatus, failedPhaseMessage(b))
	}

	assert.FileExists(t, filepath.Join(cacheDir, "000000000000-us-east-1-proj", "source", ".git", "HEAD"))

	obs.lock.RLock("read")
	defer obs.lock.RUnlock()

	assert.Equal(t, "two", obs.version)
}

func localCache(mode cbtypes.CacheMode) *cbtypes.ProjectCache {
	return &cbtypes.ProjectCache{Type: cbtypes.CacheTypeLocal, Modes: []cbtypes.CacheMode{mode}}
}

func TestEngineInvalidateProjectCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cache *cbtypes.ProjectCache
		name  string
	}{
		{name: "local", cache: localCache(cbtypes.CacheModeLocalCustomCache)},
		{name: "s3", cache: &cbtypes.ProjectCache{Type: cbtypes.CacheTypeS3, Location: aws.String("cachebucket/pfx")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			obs, rule := cacheObserver()
			h := newHarnessWith(t, newFakeRuntime(rule), nil)
			c := sdkClientFor(t, h.b)
			createSDKProject(t, c, sdkProject{cache: tt.cache, source: &cbtypes.ProjectSource{
				Type: cbtypes.SourceTypeNoSource, Buildspec: aws.String(cacheSpec),
			}})

			startAndWait(t, h, c, &codebuildsdk.StartBuildInput{})

			_, err := c.InvalidateProjectCache(t.Context(), &codebuildsdk.InvalidateProjectCacheInput{
				ProjectName: aws.String("proj"),
			})
			require.NoError(t, err)

			startAndWait(t, h, c, &codebuildsdk.StartBuildInput{})

			obs.lock.RLock("read")
			defer obs.lock.RUnlock()

			assert.Equal(t, []bool{false, false}, obs.existed)
		})
	}
}
