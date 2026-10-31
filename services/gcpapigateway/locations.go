package gcpapigateway

import (
	"cmp"
	"slices"

	"google.golang.org/genproto/googleapis/cloud/location"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func locationIDs() []string { return append([]string{globalLocation}, gatewayRegions...) }

func buildLocation(project, id string) *location.Location {
	return &location.Location{
		Name: locationName(project, id), LocationId: id, DisplayName: id,
		Labels: map[string]string{"cloud.googleapis.com/region": id},
	}
}

// GetLocation returns the global location or a gateway region.
func (b *InMemoryBackend) GetLocation(name string) (*location.Location, error) {
	project, loc, err := parseParent(name)
	if err != nil {
		return nil, err
	}

	if !slices.Contains(locationIDs(), loc) {
		return nil, errNotFound("Location %q not found.", name)
	}

	return buildLocation(project, loc), nil
}

// ListLocations lists the global location and the gateway regions.
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

	all := make([]*location.Location, 0, len(locationIDs()))
	for _, l := range locationIDs() {
		all = append(all, buildLocation(ids[0], l))
	}

	slices.SortFunc(all, func(a, c *location.Location) int { return cmp.Compare(a.GetName(), c.GetName()) })

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultListPage)

	return &location.ListLocationsResponse{Locations: pg.Data, NextPageToken: pg.Next}, nil
}
