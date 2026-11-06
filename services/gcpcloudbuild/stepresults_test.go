package gcpcloudbuild_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

// digestRuntime adds container.ImageDigester to the fake runtime.
type digestRuntime struct{ *fakeRuntime }

func (digestRuntime) ImageDigest(_ context.Context, image string) (string, error) {
	return "sha256:local-" + image, nil
}

// writesOutput returns a behavior that writes body to $BUILDER_OUTPUT/output and exits with code.
func writesOutput(body string, code int) behavior {
	return func(_ context.Context, spec container.Spec, _ io.Writer) int {
		var dir string

		for _, kv := range spec.Env {
			if v, ok := strings.CutPrefix(kv, "BUILDER_OUTPUT="); ok {
				dir = mountHost(spec, v)
			}
		}

		if dir == "" || os.WriteFile(filepath.Join(dir, "output"), []byte(body), 0o600) != nil {
			return 99
		}

		return code
	}
}

func TestBuildStepResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		steps       []*cloudbuildpb.BuildStep
		behaviors   map[string]behavior
		wantStatus  cloudbuildpb.Build_Status
		wantImages  []string
		wantOutputs []string
	}{
		{
			name:       "outputs and digests in step order",
			steps:      []*cloudbuildpb.BuildStep{step("a@sha256:abc"), step("b"), step("c")},
			behaviors:  map[string]behavior{"a@sha256:abc": writesOutput("one", 0), "b": exits(0), "c": writesOutput("three", 0)},
			wantStatus: cloudbuildpb.Build_SUCCESS,
			wantImages: []string{"sha256:abc", "sha256:local-b", "sha256:local-c"}, wantOutputs: []string{"one", "", "three"},
		},
		{
			name:       "output cap",
			steps:      []*cloudbuildpb.BuildStep{step("big")},
			behaviors:  map[string]behavior{"big": writesOutput(strings.Repeat("x", 60<<10), 0)},
			wantStatus: cloudbuildpb.Build_SUCCESS,
			wantImages: []string{"sha256:local-big"}, wantOutputs: []string{strings.Repeat("x", 50<<10)},
		},
		{
			name:       "failed build keeps step results",
			steps:      []*cloudbuildpb.BuildStep{step("bad")},
			behaviors:  map[string]behavior{"bad": writesOutput("partial", 3)},
			wantStatus: cloudbuildpb.Build_FAILURE,
			wantImages: []string{"sha256:local-bad"}, wantOutputs: []string{"partial"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, tt.behaviors)
			e.backend.SetRuntime(digestRuntime{e.rt})

			got := e.run(t, buildOf(tt.steps...))
			require.Equal(t, tt.wantStatus, got.GetStatus(), got.GetStatusDetail())

			res := got.GetResults()
			assert.Equal(t, tt.wantImages, res.GetBuildStepImages())

			outs := make([]string, len(res.GetBuildStepOutputs()))
			for i, o := range res.GetBuildStepOutputs() {
				outs[i] = string(o)
			}

			assert.Equal(t, tt.wantOutputs, outs)
		})
	}
}

func TestNoStepResultsWithoutDigestsOrOutputs(t *testing.T) {
	t.Parallel()

	e := newEnv(t, map[string]behavior{"x": exits(0)})

	got := e.run(t, buildOf(step("x")))
	require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())
	assert.Empty(t, got.GetResults().GetBuildStepImages())
	assert.Empty(t, got.GetResults().GetBuildStepOutputs())
}

func TestPhaseAndStepTiming(t *testing.T) {
	t.Parallel()

	e := newEnv(t, map[string]behavior{"x": exits(0), "y": exits(0)})
	require.NoError(t, e.store.WriteObject("s", "a.tar.gz", tarGz(t, map[string]string{"f": "x"}), ""))

	b := buildOf(step("x"), step("y"))
	b.Source = &cloudbuildpb.Source{Source: &cloudbuildpb.Source_StorageSource{
		StorageSource: &cloudbuildpb.StorageSource{Bucket: "s", Object: "a.tar.gz"},
	}}
	b.Images = []string{"us-docker.pkg.dev/p/r/img"}

	got := e.run(t, b)
	require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())

	for _, phase := range []string{"FETCHSOURCE", "BUILD", "PUSH"} {
		span := got.GetTiming()[phase]
		require.NotNil(t, span, phase)
		assert.False(t, span.GetEndTime().AsTime().Before(span.GetStartTime().AsTime()), phase)
	}

	for i, s := range got.GetSteps() {
		require.NotNil(t, s.GetPullTiming(), i)
		require.NotNil(t, s.GetTiming(), i)
		assert.False(t, s.GetTiming().GetStartTime().AsTime().Before(s.GetPullTiming().GetEndTime().AsTime()), i)
		assert.False(t, s.GetTiming().GetEndTime().AsTime().Before(s.GetTiming().GetStartTime().AsTime()), i)
	}
}

func TestNoFetchOrPushPhaseWithoutSourceOrImages(t *testing.T) {
	t.Parallel()

	e := newEnv(t, map[string]behavior{"x": exits(0)})

	got := e.run(t, buildOf(step("x")))
	require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus())
	assert.Contains(t, got.GetTiming(), "BUILD")
	assert.NotContains(t, got.GetTiming(), "FETCHSOURCE")
	assert.NotContains(t, got.GetTiming(), "PUSH")
}

func TestArtifactsImagesPushed(t *testing.T) {
	t.Parallel()

	const (
		a = "us-docker.pkg.dev/p/r/a"
		c = "us-docker.pkg.dev/p/r/c"
	)

	e := newEnv(t, map[string]behavior{"x": func(_ context.Context, spec container.Spec, _ io.Writer) int {
		host, _, _ := strings.Cut(spec.Mounts[0], ":")

		return exitCode(os.WriteFile(filepath.Join(host, "out.txt"), []byte("o"), 0o600))
	}})

	b := buildOf(step("x"))
	b.Images = []string{a}
	b.Artifacts = &cloudbuildpb.Artifacts{
		Images:  []string{a, c},
		Objects: &cloudbuildpb.Artifacts_ArtifactObjects{Location: "gs://out/p", Paths: []string{"out.txt"}},
	}

	got := e.run(t, b)
	require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())

	names := make([]string, 0, 2)
	for _, im := range got.GetResults().GetImages() {
		names = append(names, im.GetName())
	}

	assert.Equal(t, []string{a, c}, names)
	assert.Len(t, e.rt.pushes(), 2)

	timing := got.GetArtifacts().GetObjects().GetTiming()
	require.NotNil(t, timing)
	assert.Equal(t, got.GetResults().GetArtifactTiming().GetEndTime().AsTime(), timing.GetEndTime().AsTime())
}
