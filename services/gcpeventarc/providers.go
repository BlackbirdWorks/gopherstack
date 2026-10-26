package gcpeventarc

import (
	"cmp"
	"slices"
	"strings"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"google.golang.org/genproto/googleapis/cloud/location"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const locationLabel = "cloud.googleapis.com/location"

func checkProviderLocation(parent string) (string, string, error) {
	project, loc, err := parseParent(parent)
	if err != nil {
		return "", "", err
	}

	if loc != wildcardLocation && !knownLocation(loc) {
		return "", "", errInvalid("Location %q is not supported by Eventarc.", loc)
	}

	return project, loc, nil
}

// GetProvider returns an event provider of the catalog.
func (b *InMemoryBackend) GetProvider(name string) (*eventarcpb.Provider, error) {
	project, loc, id, err := parseChild(name, "providers")
	if err != nil {
		return nil, err
	}

	if !knownLocation(loc) {
		return nil, errInvalid("Location %q is not supported by Eventarc.", loc)
	}

	if !slices.Contains(providerIDs(), id) {
		return nil, errNotFound("Provider %q not found.", name)
	}

	return buildProvider(project, loc, id), nil
}

// ListProviders lists the event providers; filter matches a substring of name or display_name.
func (b *InMemoryBackend) ListProviders(
	in *eventarcpb.ListProvidersRequest,
) (*eventarcpb.ListProvidersResponse, error) {
	project, loc, err := checkProviderLocation(in.GetParent())
	if err != nil {
		return nil, err
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	if loc == wildcardLocation {
		loc = globalLocation
	}

	keep, err := providerFilter(in.GetFilter())
	if err != nil {
		return nil, err
	}

	all := make([]*eventarcpb.Provider, 0, len(providerIDs()))

	for _, id := range providerIDs() {
		if p := buildProvider(project, loc, id); keep(p) {
			all = append(all, p)
		}
	}

	cmpFn, err := orderFunc(in.GetOrderBy(), func(p *eventarcpb.Provider, field string) (string, bool) {
		switch field {
		case fieldName:
			return p.GetName(), true
		case "display_name", "displayName":
			return p.GetDisplayName(), true
		}

		return "", false
	})
	if err != nil {
		return nil, err
	}

	slices.SortStableFunc(all, cmpFn)

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultListPage)

	return &eventarcpb.ListProvidersResponse{Providers: pg.Data, NextPageToken: pg.Next}, nil
}

// providerFilter understands `name:"x"`, `displayName:"x"` and `display_name:"x"` (substring, case-insensitive).
func providerFilter(f string) (func(*eventarcpb.Provider) bool, error) {
	f = strings.TrimSpace(f)
	if f == "" {
		return func(*eventarcpb.Provider) bool { return true }, nil
	}

	field, val, ok := strings.Cut(f, ":")
	if !ok {
		return nil, errInvalid("Unsupported filter %q: use name:\"text\" or displayName:\"text\".", f)
	}

	val = strings.ToLower(strings.Trim(strings.TrimSpace(val), `"`))

	switch strings.TrimSpace(field) {
	case fieldName:
		return func(p *eventarcpb.Provider) bool { return strings.Contains(strings.ToLower(p.GetName()), val) }, nil
	case "displayName", "display_name":
		return func(p *eventarcpb.Provider) bool { return strings.Contains(strings.ToLower(p.GetDisplayName()), val) }, nil
	default:
		return nil, errInvalid("Unsupported filter field %q: use name or displayName.", field)
	}
}

// GetLocation describes one Eventarc location.
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

// ListLocations lists the Eventarc locations.
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

	all := make([]*location.Location, 0, len(locations()))

	for _, l := range locations() {
		all = append(all, buildLocation(ids[0], l))
	}

	slices.SortFunc(all, func(a, b *location.Location) int { return cmp.Compare(a.GetName(), b.GetName()) })

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultListPage)

	return &location.ListLocationsResponse{Locations: pg.Data, NextPageToken: pg.Next}, nil
}

func buildLocation(project, id string) *location.Location {
	return &location.Location{
		Name:        locationName(project, id),
		LocationId:  id,
		DisplayName: id,
		Labels:      map[string]string{locationLabel: id},
	}
}
