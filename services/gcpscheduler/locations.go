package gcpscheduler

import (
	"slices"
	"strings"

	"google.golang.org/genproto/googleapis/cloud/location"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const regionLabel = "cloud.googleapis.com/region"

// regions are the Cloud Scheduler locations, id then display name.
var regions = [][2]string{ //nolint:gochecknoglobals // immutable catalog
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

func locationOf(project string, r [2]string) *location.Location {
	return &location.Location{
		Name:        "projects/" + project + "/locations/" + r[0],
		LocationId:  r[0],
		DisplayName: r[1],
		Labels:      map[string]string{regionLabel: r[0]},
	}
}

// ListLocations lists the supported locations of a project.
func ListLocations(req *location.ListLocationsRequest) ([]*location.Location, string, error) {
	name := strings.TrimPrefix(req.GetName(), "/")
	if err := parseProject(name); err != nil {
		return nil, "", err
	}

	if req.GetFilter() != "" {
		return nil, "", errInvalid("filter is not supported.")
	}

	if req.GetPageSize() < 0 {
		return nil, "", errInvalid("page_size must not be negative.")
	}

	project := strings.Split(name, "/")[1]
	all := make([]*location.Location, 0, len(regions))

	for _, r := range regions {
		all = append(all, locationOf(project, r))
	}

	slices.SortFunc(all, func(a, b *location.Location) int { return strings.Compare(a.GetName(), b.GetName()) })

	pg := page.New(all, req.GetPageToken(), int(req.GetPageSize()), defaultPageSize)

	return pg.Data, pg.Next, nil
}

// GetLocation returns one supported location.
func GetLocation(req *location.GetLocationRequest) (*location.Location, error) {
	if err := parseLocation(req.GetName()); err != nil {
		return nil, err
	}

	p := strings.Split(req.GetName(), "/")

	for _, r := range regions {
		if r[0] == p[3] {
			return locationOf(p[1], r), nil
		}
	}

	return nil, errNotFound("Location %q is not supported.", req.GetName())
}
