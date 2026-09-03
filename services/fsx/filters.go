package fsx

import "slices"

// filterNameFileSystemID is shared across every FSx filter enum that
// includes it (types.FilterName, types.SnapshotFilterName,
// types.VolumeFilterName, types.StorageVirtualMachineFilterName,
// types.DataRepositoryTaskFilterName, types.S3AccessPointAttachmentsFilterName).
const filterNameFileSystemID = "file-system-id"

// wireFilter is the shared {Name, Values} shape every FSx Describe* filter
// uses on the wire (types.Filter, types.SnapshotFilter, types.VolumeFilter,
// types.StorageVirtualMachineFilter, types.DataRepositoryTaskFilter,
// types.S3AccessPointAttachmentsFilter all share this JSON shape, differing
// only in which Name values each operation documents as supported).
type wireFilter struct {
	Name   string   `json:"Name"`
	Values []string `json:"Values,omitempty"`
}

// matchesFilters reports whether valueOf(name) is present in a filter's
// Values for every filter in filters whose Name valueOf recognizes (an
// unrecognized filter Name is ignored, matching AWS's per-op "supported
// names" behavior for a documented-but-unimplemented name rather than
// rejecting the request). Values within one filter are ORed; filters
// across different names are ANDed.
func matchesFilters(filters []wireFilter, valueOf func(name string) (string, bool)) bool {
	for _, f := range filters {
		got, ok := valueOf(f.Name)
		if !ok {
			continue
		}

		if !slices.Contains(f.Values, got) {
			return false
		}
	}

	return true
}
