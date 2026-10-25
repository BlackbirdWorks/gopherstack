package gcpworkflows

import (
	"cmp"
	"slices"

	"google.golang.org/genproto/googleapis/cloud/location"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultLocationsPage = 100
	locationLabel        = "cloud.googleapis.com/region"
)

// regions are the locations Workflows is offered in.
func regions() []string {
	return []string{
		"asia-east1", "asia-east2", "asia-northeast1", "asia-northeast2", "asia-northeast3", "asia-south1",
		"asia-south2", "asia-southeast1", "asia-southeast2", "australia-southeast1", "australia-southeast2",
		"europe-central2", "europe-north1", "europe-southwest1", "europe-west1", "europe-west2", "europe-west3",
		"europe-west4", "europe-west6", "europe-west8", "europe-west9", "europe-west10", "europe-west12",
		"me-central1", "me-central2", "me-west1", "northamerica-northeast1", "northamerica-northeast2",
		"southamerica-east1", "southamerica-west1", "us-central1", "us-east1", "us-east4", "us-east5",
		"us-south1", "us-west1", "us-west2", "us-west3", "us-west4",
	}
}

func knownLocation(id string) bool { return slices.Contains(regions(), id) }

func buildLocation(project, id string) *location.Location {
	return &location.Location{
		Name:        locationName(project, id),
		LocationId:  id,
		DisplayName: id,
		Labels:      map[string]string{locationLabel: id},
	}
}

// GetLocation returns a Workflows location.
func (b *InMemoryBackend) GetLocation(name string) (*location.Location, error) {
	project, loc, err := parseParent(name)
	if err != nil {
		return nil, err
	}

	if !knownLocation(loc) {
		return nil, errNotFound("Location %q not found.", name)
	}

	return buildLocation(project, loc), nil
}

// ListLocations lists the Workflows locations.
func (b *InMemoryBackend) ListLocations(in *location.ListLocationsRequest) (*location.ListLocationsResponse, error) {
	ids, ok := parseRes(in.GetName(), "projects")
	if !ok {
		return nil, errInvalid("Invalid name %q: want projects/{project}.", in.GetName())
	}

	if in.GetFilter() != "" {
		return nil, errInvalid("filter is not supported for locations.")
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	all := make([]*location.Location, 0, len(regions()))
	for _, l := range regions() {
		all = append(all, buildLocation(ids[0], l))
	}

	slices.SortFunc(all, func(a, b *location.Location) int { return cmp.Compare(a.GetName(), b.GetName()) })

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultLocationsPage)

	return &location.ListLocationsResponse{Locations: pg.Data, NextPageToken: pg.Next}, nil
}
