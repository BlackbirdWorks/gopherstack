package gcprun

import (
	"maps"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	dockerBuilderImage    = "gcr.io/cloud-builders/docker"
	buildpacksPackImage   = "gcr.io/k8s-skaffold/pack"
	defaultBuildpackImage = "gcr.io/buildpacks/builder:latest"
	buildTimeout          = time.Hour
	machineTypeHighCPU    = "E2_HIGHCPU_8"
)

// Builder submits a Cloud Build build on behalf of Cloud Run; it is wired to the Cloud Build emulator.
type Builder interface {
	SubmitBuild(parent string, build *cloudbuildpb.Build) (*longrunningpb.Operation, error)
}

// SetBuilder wires Cloud Build for SubmitBuild; nil disables it.
func (b *InMemoryBackend) SetBuilder(bl Builder) {
	b.mu.Lock("SetBuilder")
	defer b.mu.Unlock()

	b.builder = bl
}

func validateSubmitBuild(in *runpb.SubmitBuildRequest) error {
	if src := in.GetStorageSource(); src == nil || src.GetBucket() == "" || src.GetObject() == "" {
		return errInvalid("source.storage_source with a bucket and an object is required.")
	}

	if in.GetImageUri() == "" || !strings.Contains(in.GetImageUri(), "/") {
		return errInvalid("image_uri must be an Artifact Registry image URI.")
	}

	if mt := in.GetMachineType(); mt != "" && mt != machineTypeHighCPU {
		return errInvalid("machine_type %q is not supported; only %s is.", mt, machineTypeHighCPU)
	}

	if in.GetBuildType() == nil {
		return errInvalid("One of build_type.docker_build or build_type.buildpack_build is required.")
	}

	return nil
}

func buildpackArgs(in *runpb.SubmitBuildRequest) []string {
	bp := in.GetBuildpackBuild()
	builder := bp.GetBaseImage()

	if builder == "" {
		builder = defaultBuildpackImage
	}

	args := []string{"build", in.GetImageUri(), "--builder", builder, "--network", "cloudbuild"}

	if bp.GetFunctionTarget() != "" {
		args = append(args, "--env", "GOOGLE_FUNCTION_TARGET="+bp.GetFunctionTarget())
	}

	if bp.GetProjectDescriptor() != "" {
		args = append(args, "--descriptor", bp.GetProjectDescriptor())
	}

	if bp.GetCacheImageUri() != "" {
		args = append(args, "--cache-image", bp.GetCacheImageUri())
	}

	for _, k := range slices.Sorted(maps.Keys(bp.GetEnvironmentVariables())) {
		args = append(args, "--env", k+"="+bp.GetEnvironmentVariables()[k])
	}

	return args
}

// cloudBuildOf maps the Run build request to the Cloud Build build that performs it.
func cloudBuildOf(in *runpb.SubmitBuildRequest) *cloudbuildpb.Build {
	src := in.GetStorageSource()

	storage := &cloudbuildpb.StorageSource{
		Bucket: src.GetBucket(), Object: src.GetObject(), Generation: src.GetGeneration(),
	}

	build := &cloudbuildpb.Build{
		Source:         &cloudbuildpb.Source{Source: &cloudbuildpb.Source_StorageSource{StorageSource: storage}},
		Images:         []string{in.GetImageUri()},
		ServiceAccount: in.GetServiceAccount(),
		Tags:           in.GetTags(),
		Timeout:        durationpb.New(buildTimeout),
	}

	if in.GetBuildpackBuild() != nil {
		build.Steps = []*cloudbuildpb.BuildStep{{Name: buildpacksPackImage, Args: buildpackArgs(in)}}
	} else {
		build.Steps = []*cloudbuildpb.BuildStep{{
			Name: dockerBuilderImage, Args: []string{"build", "-t", in.GetImageUri(), "."},
		}}
	}

	switch {
	case in.GetWorkerPool() != "":
		build.Options = &cloudbuildpb.BuildOptions{
			Pool: &cloudbuildpb.BuildOptions_PoolOption{Name: in.GetWorkerPool()},
		}
	case in.GetMachineType() != "":
		build.Options = &cloudbuildpb.BuildOptions{MachineType: cloudbuildpb.BuildOptions_E2_HIGHCPU_8}
	}

	return build
}

// SubmitBuild runs the build through the Cloud Build emulator and returns its operation.
func (b *InMemoryBackend) SubmitBuild(in *runpb.SubmitBuildRequest) (*runpb.SubmitBuildResponse, error) {
	if _, err := parseParent(in.GetParent()); err != nil {
		return nil, err
	}

	if err := validateSubmitBuild(in); err != nil {
		return nil, err
	}

	b.mu.RLock("SubmitBuild")
	builder := b.builder
	b.mu.RUnlock()

	if builder == nil {
		return nil, status.Error(codes.FailedPrecondition, "Cloud Build is not available in this emulator.")
	}

	op, err := builder.SubmitBuild(in.GetParent(), cloudBuildOf(in))
	if err != nil {
		return nil, err
	}

	return &runpb.SubmitBuildResponse{BuildOperation: op}, nil
}
