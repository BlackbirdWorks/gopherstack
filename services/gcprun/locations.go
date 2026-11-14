package gcprun

import (
	"context"
	"slices"
	"strings"

	"google.golang.org/genproto/googleapis/cloud/location"
)

const regionLabel = "cloud.googleapis.com/region"

// regions returns the locations the emulator's other GCP services offer (Cloud Scheduler's catalog), id then name.
func regions() [][2]string {
	return [][2]string{
		{"africa-south1", "Johannesburg"}, {"asia-east1", "Taiwan"}, {"asia-east2", "Hong Kong"},
		{"asia-northeast1", "Tokyo"}, {"asia-northeast2", "Osaka"}, {"asia-northeast3", "Seoul"},
		{"asia-south1", "Mumbai"}, {"asia-south2", "Delhi"}, {"asia-southeast1", "Singapore"},
		{"asia-southeast2", "Jakarta"}, {"australia-southeast1", "Sydney"}, {"australia-southeast2", "Melbourne"},
		{"europe-central2", "Warsaw"}, {"europe-north1", "Finland"}, {"europe-southwest1", "Madrid"},
		{"europe-west1", "Belgium"}, {"europe-west10", "Berlin"}, {"europe-west12", "Turin"},
		{"europe-west2", "London"}, {"europe-west3", "Frankfurt"}, {"europe-west4", "Netherlands"},
		{"europe-west6", "Zurich"}, {"europe-west8", "Milan"}, {"europe-west9", "Paris"},
		{"me-central1", "Doha"}, {"me-central2", "Dammam"}, {"me-west1", "Tel Aviv"},
		{"northamerica-northeast1", "Montréal"}, {"northamerica-northeast2", "Toronto"},
		{"southamerica-east1", "São Paulo"}, {"southamerica-west1", "Santiago"},
		{"us-central1", "Iowa"}, {"us-east1", "South Carolina"}, {"us-east4", "Northern Virginia"},
		{"us-east5", "Columbus"}, {"us-south1", "Dallas"}, {"us-west1", "Oregon"},
		{"us-west2", "Los Angeles"}, {"us-west3", "Salt Lake City"}, {"us-west4", "Las Vegas"},
	}
}

type locationsServer struct {
	location.UnimplementedLocationsServer
}

func locationOf(project string, r [2]string) *location.Location {
	return &location.Location{
		Name:        "projects/" + project + "/locations/" + r[0],
		LocationId:  r[0],
		DisplayName: r[1],
		Labels:      map[string]string{regionLabel: r[0]},
	}
}

func projectOfName(name string) (string, bool) {
	p := strings.Split(strings.TrimPrefix(name, "/"), "/")
	if len(p) != namePairSize || p[0] != "projects" || p[1] == "" {
		return "", false
	}

	return p[1], true
}

func (*locationsServer) ListLocations(
	_ context.Context, in *location.ListLocationsRequest,
) (*location.ListLocationsResponse, error) {
	project, ok := projectOfName(in.GetName())
	if !ok {
		return nil, errInvalid("Invalid name %q: want projects/{project}.", in.GetName())
	}

	if in.GetFilter() != "" {
		return nil, errInvalid("filter is not supported.")
	}

	cat := regions()
	all := make([]*location.Location, 0, len(cat))

	for _, r := range cat {
		all = append(all, locationOf(project, r))
	}

	slices.SortFunc(all, func(a, b *location.Location) int { return strings.Compare(a.GetName(), b.GetName()) })

	items, next, err := paginate(all, in.GetPageToken(), in.GetPageSize())
	if err != nil {
		return nil, err
	}

	return &location.ListLocationsResponse{Locations: items, NextPageToken: next}, nil
}

func (*locationsServer) GetLocation(_ context.Context, in *location.GetLocationRequest) (*location.Location, error) {
	n, err := parseName(in.GetName())
	if err != nil {
		return nil, err
	}

	for _, r := range regions() {
		if r[0] == n.location {
			return locationOf(n.project, r), nil
		}
	}

	return nil, errNotFoundMsg("Location %q is not supported.", in.GetName())
}
