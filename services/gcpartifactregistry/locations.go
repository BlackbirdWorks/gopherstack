package gcpartifactregistry

import (
	"context"
	"slices"
	"strings"

	"google.golang.org/genproto/googleapis/cloud/location"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// locationCatalogue is the list at docs.cloud.google.com/artifact-registry/docs/repositories/repo-locations.
type catalogueEntry struct{ id, display string }

var locationCatalogue = []catalogueEntry{ //nolint:gochecknoglobals // immutable catalogue
	{"northamerica-northeast1", "Montréal"},
	{"northamerica-northeast2", "Toronto"},
	{"northamerica-south1", "Queretaro"},
	{"us-central1", "Iowa"},
	{"us-east1", "South Carolina"},
	{"us-east4", "Northern Virginia"},
	{"us-east5", "Columbus"},
	{"us-south1", "Dallas"},
	{"us-west1", "Oregon"},
	{"us-west2", "Los Angeles"},
	{"us-west3", "Salt Lake City"},
	{"us-west4", "Las Vegas"},
	{"southamerica-east1", "São Paulo"},
	{"southamerica-west1", "Santiago"},
	{"europe-central2", "Warsaw"},
	{"europe-north1", "Finland"},
	{"europe-north2", "Stockholm"},
	{"europe-southwest1", "Madrid"},
	{"europe-west1", "Belgium"},
	{"europe-west2", "London"},
	{"europe-west3", "Frankfurt"},
	{"europe-west4", "Netherlands"},
	{"europe-west6", "Zürich"},
	{"europe-west8", "Milan"},
	{"europe-west9", "Paris"},
	{"europe-west10", "Berlin"},
	{"europe-west12", "Turin"},
	{"me-central1", "Doha"},
	{"me-central2", "Dammam"},
	{"me-west1", "Tel Aviv"},
	{"asia-east1", "Taiwan"},
	{"asia-east2", "Hong Kong"},
	{"asia-northeast1", "Tokyo"},
	{"asia-northeast2", "Osaka"},
	{"asia-northeast3", "Seoul"},
	{"asia-south1", "Mumbai"},
	{"asia-south2", "Delhi"},
	{"asia-southeast1", "Singapore"},
	{"asia-southeast2", "Jakarta"},
	{"asia-southeast3", "Bangkok"},
	{"australia-southeast1", "Sydney"},
	{"australia-southeast2", "Melbourne"},
	{"africa-south1", "Johannesburg"},
	{"asia", "Data centers in Asia"},
	{"europe", "Data centers in the European Union"},
	{"us", "Data centers in the United States"},
}

func buildLocation(project string, idx int) *location.Location {
	l := locationCatalogue[idx]
	loc := &location.Location{
		Name: "projects/" + project + "/locations/" + l.id, LocationId: l.id, DisplayName: l.display,
	}

	if strings.Contains(l.id, "-") {
		loc.Labels = map[string]string{"cloud.googleapis.com/region": l.id}
	}

	return loc
}

type locationsServer struct {
	location.UnimplementedLocationsServer
}

func (locationsServer) GetLocation(
	_ context.Context, in *location.GetLocationRequest,
) (*location.Location, error) {
	ref, err := parseParent(in.GetName())
	if err != nil {
		return nil, err
	}

	idx := slices.IndexFunc(locationCatalogue, func(l catalogueEntry) bool { return l.id == ref.location })
	if idx < 0 {
		return nil, errNotFound("Location %q not found.", in.GetName())
	}

	return buildLocation(ref.project, idx), nil
}

func (locationsServer) ListLocations(
	_ context.Context, in *location.ListLocationsRequest,
) (*location.ListLocationsResponse, error) {
	p := strings.Split(in.GetName(), "/")
	if len(p) != 2 || p[0] != segProjects || p[1] == "" {
		return nil, errInvalid("Invalid name %q: want projects/{project}.", in.GetName())
	}

	if in.GetFilter() != "" {
		return nil, errInvalid("filter is not supported for locations.")
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	all := make([]*location.Location, 0, len(locationCatalogue))
	for i := range locationCatalogue {
		all = append(all, buildLocation(p[1], i))
	}

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultPage)

	return &location.ListLocationsResponse{Locations: pg.Data, NextPageToken: pg.Next}, nil
}
