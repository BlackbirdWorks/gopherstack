package gcpcloudbuild

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

// Keys of Build.timing.
const (
	phaseFetchSource = "FETCHSOURCE"
	phaseBuild       = "BUILD"
	phasePush        = "PUSH"
	digestProbe      = 5 * time.Second
	outputFile       = "output"
)

// setPhase records the span of a build phase from start until now.
func (r *runner) setPhase(name string, start *timestamppb.Timestamp) {
	end := r.b.ts()

	r.b.update(r.ref, func(pb *cloudbuildpb.Build) {
		if pb.Timing == nil {
			pb.Timing = map[string]*cloudbuildpb.TimeSpan{}
		}

		pb.Timing[name] = &cloudbuildpb.TimeSpan{StartTime: start, EndTime: end}
	})
}

// markPulled records how long starting the container took (image pull included) as the step's pull_timing and
// starts the step's own timing after it.
func (r *runner) markPulled(i int, start *timestamppb.Timestamp) {
	end := r.b.ts()

	r.b.update(r.ref, func(pb *cloudbuildpb.Build) {
		step := pb.GetSteps()[i]
		step.PullTiming = &cloudbuildpb.TimeSpan{StartTime: start, EndTime: end}

		if step.GetTiming() != nil {
			step.Timing.StartTime = end
		}
	})
}

// imageDigest reports the digest of a step's builder image: the pinned digest in the reference, else the runtime's
// local image digest; empty when neither is known.
func (r *runner) imageDigest(ctx context.Context, image string) string {
	if _, d, ok := strings.Cut(image, "@"); ok {
		return d
	}

	dg, ok := r.rt.(container.ImageDigester)
	if !ok {
		return ""
	}

	ctx, cancel := context.WithTimeout(ctx, digestProbe)
	defer cancel()

	d, err := dg.ImageDigest(ctx, image)
	if err != nil {
		return ""
	}

	return d
}

func (r *runner) outputDir(i int) (string, error) {
	dir := filepath.Join(r.auxDir(), "outputs", "step-"+strconv.Itoa(i))

	if err := os.MkdirAll(dir, workDirPerm); err != nil {
		return "", err //nolint:wrapcheck // reported as the step failure
	}

	if err := os.Chmod(dir, workDirPerm); err != nil {
		return "", err //nolint:wrapcheck // reported as the step failure
	}

	return dir, nil
}

// collectOutput keeps the first 50KB a step wrote to $BUILDER_OUTPUT/output.
func (r *runner) collectOutput(i int) {
	f, err := os.Open(filepath.Join(r.auxDir(), "outputs", "step-"+strconv.Itoa(i), outputFile)) //nolint:gosec // build-owned dir
	if err != nil {
		return
	}

	defer func() { _ = f.Close() }()

	data, err := io.ReadAll(io.LimitReader(f, maxStepOutput))
	if err == nil {
		r.stepOutputs[i] = data
	}
}

// addStepResults fills results.build_step_images and build_step_outputs, in step order, when any step has one.
func (r *runner) addStepResults(res *cloudbuildpb.Results) {
	if slices.ContainsFunc(r.stepImages, func(s string) bool { return s != "" }) {
		res.BuildStepImages = slices.Clone(r.stepImages)
	}

	if slices.ContainsFunc(r.stepOutputs, func(b []byte) bool { return len(b) > 0 }) {
		res.BuildStepOutputs = make([][]byte, len(r.stepOutputs))
		for i, o := range r.stepOutputs {
			res.BuildStepOutputs[i] = slices.Clone(o)
		}
	}
}

// imagesToPush is images plus artifacts.images, without duplicates.
func (r *runner) imagesToPush() []string {
	out := slices.Clone(r.build.GetImages())

	for _, img := range r.build.GetArtifacts().GetImages() {
		if !slices.Contains(out, img) {
			out = append(out, img)
		}
	}

	return out
}
