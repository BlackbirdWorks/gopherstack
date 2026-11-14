package gcprun_test

import (
	"testing"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeBuilder struct {
	build  *cloudbuildpb.Build
	parent string
}

func (f *fakeBuilder) SubmitBuild(parent string, b *cloudbuildpb.Build) (*longrunningpb.Operation, error) {
	f.parent, f.build = parent, b

	return &longrunningpb.Operation{Name: "operations/build/test-project/abc"}, nil
}

func submitReq() *runpb.SubmitBuildRequest {
	return &runpb.SubmitBuildRequest{
		Parent: parent,
		Source: &runpb.SubmitBuildRequest_StorageSource{StorageSource: &runpb.StorageSource{
			Bucket: "src", Object: "app.tar.gz", Generation: 7,
		}},
		ImageUri:  "us-central1-docker.pkg.dev/test-project/repo/app:v1",
		BuildType: &runpb.SubmitBuildRequest_DockerBuild_{DockerBuild: &runpb.SubmitBuildRequest_DockerBuild{}},
		Tags:      []string{"t1"},
	}
}

func TestSubmitBuild_Docker(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	fb := &fakeBuilder{}
	e.handler.Backend.SetBuilder(fb)

	resp, err := e.builds.SubmitBuild(t.Context(), submitReq())
	require.NoError(t, err)
	assert.Equal(t, "operations/build/test-project/abc", resp.GetBuildOperation().GetName())

	assert.Equal(t, parent, fb.parent)
	src := fb.build.GetSource().GetStorageSource()
	assert.Equal(t, "src", src.GetBucket())
	assert.Equal(t, "app.tar.gz", src.GetObject())
	assert.Equal(t, int64(7), src.GetGeneration())
	assert.Equal(t, []string{"us-central1-docker.pkg.dev/test-project/repo/app:v1"}, fb.build.GetImages())
	assert.Equal(t, []string{"t1"}, fb.build.GetTags())
	require.Len(t, fb.build.GetSteps(), 1)
	assert.Equal(t, "gcr.io/cloud-builders/docker", fb.build.GetSteps()[0].GetName())
	assert.Equal(t, []string{"build", "-t", submitReq().GetImageUri(), "."}, fb.build.GetSteps()[0].GetArgs())
}

func TestSubmitBuild_Buildpacks(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	fb := &fakeBuilder{}
	e.handler.Backend.SetBuilder(fb)

	req := submitReq()
	req.BuildType = &runpb.SubmitBuildRequest_BuildpackBuild{BuildpackBuild: &runpb.SubmitBuildRequest_BuildpacksBuild{
		FunctionTarget:       "handler",
		EnvironmentVariables: map[string]string{"B": "2", "A": "1"},
		BaseImage:            "example/builder:1",
	}}
	req.WorkerPool = parent + "/workerPools/cb"

	_, err := e.builds.SubmitBuild(t.Context(), req)
	require.NoError(t, err)

	step := fb.build.GetSteps()[0]
	assert.Equal(t, "gcr.io/k8s-skaffold/pack", step.GetName())
	assert.Equal(t, []string{
		"build", req.GetImageUri(), "--builder", "example/builder:1", "--network", "cloudbuild",
		"--env", "GOOGLE_FUNCTION_TARGET=handler", "--env", "A=1", "--env", "B=2",
	}, step.GetArgs())
	assert.Equal(t, parent+"/workerPools/cb", fb.build.GetOptions().GetPool().GetName())
}

func TestSubmitBuild_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod  func(*runpb.SubmitBuildRequest)
		name string
		code codes.Code
	}{
		{func(r *runpb.SubmitBuildRequest) { r.Parent = "projects/p" }, "bad parent", codes.InvalidArgument},
		{func(r *runpb.SubmitBuildRequest) { r.Source = nil }, "no source", codes.InvalidArgument},
		{func(r *runpb.SubmitBuildRequest) { r.ImageUri = "" }, "no image", codes.InvalidArgument},
		{func(r *runpb.SubmitBuildRequest) { r.BuildType = nil }, "no build type", codes.InvalidArgument},
		{func(r *runpb.SubmitBuildRequest) { r.MachineType = "N1" }, "machine type", codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			e.handler.Backend.SetBuilder(&fakeBuilder{})

			req := submitReq()
			tt.mod(req)

			_, err := e.builds.SubmitBuild(t.Context(), req)
			assert.Equal(t, tt.code, status.Code(err))
		})
	}
}

func TestSubmitBuild_NoBuilder(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)

	_, err := e.builds.SubmitBuild(t.Context(), submitReq())
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}
