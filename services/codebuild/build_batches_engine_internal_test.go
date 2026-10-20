package codebuild

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	codebuildsdk "github.com/aws/aws-sdk-go-v2/service/codebuild"
	cbtypes "github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

func nodeSpec(marker string) string {
	return "      buildspec: |\n        version: 0.2\n        phases:\n          build:\n" +
		"            commands: [echo " + marker + "]\n"
}

func batchListSpec() string {
	return "version: 0.2\nbatch:\n  build-list:\n    - identifier: one\n" + nodeSpec("NODE_ONE") +
		"    - identifier: two\n" + nodeSpec("NODE_TWO")
}

func graphSpec() string {
	return "version: 0.2\nbatch:\n  build-graph:\n    - identifier: a\n" + nodeSpec("NODE_A") +
		"    - identifier: b\n      depend-on: [a]\n" + nodeSpec("NODE_B")
}

const batchMatrixSpec = `version: 0.2
batch:
  build-matrix:
    static:
      ignore-failure: false
    dynamic:
      env:
        variables:
          FOO: [alpha, beta]
phases:
  build:
    commands: [echo MATRIX_CMD]
`

type batchHarness struct {
	*engineHarness
	c *codebuildsdk.Client
}

func newBatchHarness(t *testing.T, spec string, rules ...fakeRule) *batchHarness {
	t.Helper()

	h := newHarnessWith(t, newFakeRuntime(rules...), func(c *EngineConfig) { c.TimeoutUnit = time.Minute })
	c := sdkClientFor(t, h.b)
	createSDKProject(t, c, sdkProject{source: &cbtypes.ProjectSource{
		Type: cbtypes.SourceTypeNoSource, Buildspec: aws.String(spec),
	}})

	return &batchHarness{engineHarness: h, c: c}
}

func (h *batchHarness) start(t *testing.T) string {
	t.Helper()

	out, err := h.c.StartBuildBatch(t.Context(), &codebuildsdk.StartBuildBatchInput{ProjectName: aws.String("proj")})
	require.NoError(t, err)

	return aws.ToString(out.BuildBatch.Id)
}

func (h *batchHarness) settle(t *testing.T, id string) cbtypes.BuildBatch {
	t.Helper()

	h.b.WaitBuilds()

	out, err := h.c.BatchGetBuildBatches(t.Context(), &codebuildsdk.BatchGetBuildBatchesInput{Ids: []string{id}})
	require.NoError(t, err)
	require.Len(t, out.BuildBatches, 1)

	return out.BuildBatches[0]
}

func TestEngineBatchRunsChildBuilds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		spec      string
		wantRan   []string
		wantExp   []string
		wantCount int
	}{
		{name: "build_list", spec: batchListSpec(), wantRan: []string{"NODE_ONE", "NODE_TWO"}, wantCount: 2},
		{name: "build_graph", spec: graphSpec(), wantRan: []string{"NODE_A", "NODE_B"}, wantCount: 2},
		{
			name: "build_matrix", spec: batchMatrixSpec, wantRan: []string{"MATRIX_CMD"}, wantCount: 2,
			wantExp: []string{"export FOO='alpha'", "export FOO='beta'"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newBatchHarness(t, tt.spec)
			batch := h.settle(t, h.start(t))

			assert.Equal(t, cbtypes.StatusTypeSucceeded, batch.BuildBatchStatus)
			assert.True(t, batch.Complete)
			require.Len(t, batch.BuildGroups, tt.wantCount)

			for _, g := range batch.BuildGroups {
				assert.Equal(t, cbtypes.StatusTypeSucceeded, g.CurrentBuildSummary.BuildStatus)
			}

			for _, s := range append(tt.wantRan, tt.wantExp...) {
				assert.True(t, h.rt.ran(s), s)
			}

			ids, err := h.c.ListBuildsForProject(
				t.Context(),
				&codebuildsdk.ListBuildsForProjectInput{ProjectName: aws.String("proj")},
			)
			require.NoError(t, err)
			require.Len(t, ids.Ids, tt.wantCount)

			builds, err := h.c.BatchGetBuilds(t.Context(), &codebuildsdk.BatchGetBuildsInput{Ids: ids.Ids})
			require.NoError(t, err)

			for _, b := range builds.Builds {
				assert.Equal(t, aws.ToString(batch.Arn), aws.ToString(b.BuildBatchArn))
				assert.Equal(t, cbtypes.StatusTypeSucceeded, b.BuildStatus)
			}
		})
	}
}

func TestEngineBatchGraphOrdering(t *testing.T) {
	t.Parallel()

	var failA atomic.Bool

	failA.Store(true)

	order := lockmetrics.New("order")

	var seen []string

	record := func(name string) func(string) {
		return func(string) {
			order.Lock("o")
			seen = append(seen, name)
			order.Unlock()
		}
	}

	h := newBatchHarness(t, graphSpec(),
		fakeRule{match: "NODE_A", create: record("A"), codeFn: func() int {
			if failA.Load() {
				return 1
			}

			return 0
		}},
		fakeRule{match: "NODE_B", create: record("B")},
	)

	id := h.start(t)
	batch := h.settle(t, id)

	assert.Equal(t, cbtypes.StatusTypeFailed, batch.BuildBatchStatus)
	assert.False(t, h.rt.ran("echo NODE_B"), "dependent must not start after a failed dependency")

	failA.Store(false)

	retried, err := h.c.RetryBuildBatch(t.Context(), &codebuildsdk.RetryBuildBatchInput{
		Id: aws.String(id), RetryType: cbtypes.RetryBuildBatchTypeRetryFailedBuilds,
	})
	require.NoError(t, err)

	batch = h.settle(t, aws.ToString(retried.BuildBatch.Id))
	assert.Equal(t, cbtypes.StatusTypeSucceeded, batch.BuildBatchStatus)

	order.RLock("o")
	defer order.RUnlock()

	assert.Equal(t, []string{"A", "A", "B"}, seen)
}

func TestEngineBatchStop(t *testing.T) {
	t.Parallel()

	h := newBatchHarness(t, batchListSpec(), fakeRule{match: "NODE_ONE", block: true})
	id := h.start(t)

	<-h.rt.started

	out, err := h.c.StopBuildBatch(t.Context(), &codebuildsdk.StopBuildBatchInput{Id: aws.String(id)})
	require.NoError(t, err)
	assert.Equal(t, cbtypes.StatusTypeStopped, out.BuildBatch.BuildBatchStatus)

	batch := h.settle(t, id)
	assert.Equal(t, cbtypes.StatusTypeStopped, batch.BuildBatchStatus)

	builds, err := h.c.ListBuildsForProject(
		t.Context(),
		&codebuildsdk.ListBuildsForProjectInput{ProjectName: aws.String("proj")},
	)
	require.NoError(t, err)

	got, err := h.c.BatchGetBuilds(t.Context(), &codebuildsdk.BatchGetBuildsInput{Ids: builds.Ids})
	require.NoError(t, err)

	for _, b := range got.Builds {
		assert.True(t, b.BuildComplete)
	}
}

func TestEngineBatchListAndDelete(t *testing.T) {
	t.Parallel()

	h := newBatchHarness(t, batchListSpec())
	id := h.start(t)
	h.settle(t, id)

	all, err := h.c.ListBuildBatches(t.Context(), &codebuildsdk.ListBuildBatchesInput{})
	require.NoError(t, err)
	assert.Equal(t, []string{id}, all.Ids)

	forProject, err := h.c.ListBuildBatchesForProject(t.Context(), &codebuildsdk.ListBuildBatchesForProjectInput{
		ProjectName: aws.String("proj"),
		Filter:      &cbtypes.BuildBatchFilter{Status: cbtypes.StatusTypeSucceeded},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{id}, forProject.Ids)

	_, err = h.c.DeleteBuildBatch(t.Context(), &codebuildsdk.DeleteBuildBatchInput{Id: aws.String(id)})
	require.NoError(t, err)

	left, err := h.c.ListBuildBatches(t.Context(), &codebuildsdk.ListBuildBatchesInput{})
	require.NoError(t, err)
	assert.Empty(t, left.Ids)
}
