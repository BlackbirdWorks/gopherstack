package build //nolint:revive // mirrors upstream docker package layout

import "io"

// ImageBuildOptions models the subset of Docker build options used in this repo.
type ImageBuildOptions struct {
	Dockerfile string
	Tags       []string
	NoCache    bool
	PullParent bool
	Remove     bool
}

// ImageBuildResponse models the subset of the build response used in this repo.
type ImageBuildResponse struct {
	Body io.ReadCloser
}
