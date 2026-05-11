package image //nolint:revive // mirrors upstream docker package layout

import (
	compatfilters "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/filters"
	mobyimage "github.com/moby/moby/api/types/image"
)

// Summary aliases the Moby image summary type.
type Summary = mobyimage.Summary

// DeleteResponse aliases the Moby image delete response type.
type DeleteResponse = mobyimage.DeleteResponse

// PullOptions models the subset of image pull options used in this repo.
type PullOptions struct {
	All bool
}

// ListOptions models the subset of image list options used in this repo.
type ListOptions struct {
	Filters    compatfilters.Args
	All        bool
	Identity   bool
	Manifests  bool
	SharedSize bool
}

// RemoveOptions models the subset of image remove options used in this repo.
type RemoveOptions struct {
	Force         bool
	PruneChildren bool
}
