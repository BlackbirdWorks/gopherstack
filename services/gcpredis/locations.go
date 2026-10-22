package gcpredis

import (
	"slices"
	"strings"

	"google.golang.org/genproto/googleapis/cloud/location"
	"google.golang.org/protobuf/types/known/anypb"

	"cloud.google.com/go/redis/apiv1/redispb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	regionLabel     = "cloud.googleapis.com/region"
	defaultPageSize = 100
)

var zoneSuffixes = []string{"a", "b", "c"} //nolint:gochecknoglobals // immutable catalog

// regions are the Memorystore for Redis locations, id then display name.
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

func regionKnown(id string) bool {
	return slices.ContainsFunc(regions, func(r [2]string) bool { return r[0] == id })
}

// zonesOf lists the zones of a region.
func zonesOf(region string) []string {
	out := make([]string, 0, len(zoneSuffixes))
	for _, s := range zoneSuffixes {
		out = append(out, region+"-"+s)
	}

	return out
}

// zoneIn reports whether zone belongs to region.
func zoneIn(zone, region string) bool {
	return slices.Contains(zonesOf(region), zone)
}

func locationOf(project string, r [2]string) *location.Location {
	zones := make(map[string]*redispb.ZoneMetadata, len(zoneSuffixes))
	for _, z := range zonesOf(r[0]) {
		zones[z] = &redispb.ZoneMetadata{}
	}

	meta, _ := anypb.New(&redispb.LocationMetadata{AvailableZones: zones})

	return &location.Location{
		Name:        "projects/" + project + "/locations/" + r[0],
		LocationId:  r[0],
		DisplayName: r[1],
		Labels:      map[string]string{regionLabel: r[0]},
		Metadata:    meta,
	}
}

// ListLocations lists the supported locations of a project.
func ListLocations(req *location.ListLocationsRequest) ([]*location.Location, string, error) {
	name := strings.TrimPrefix(req.GetName(), "/")

	p := strings.Split(name, "/")
	if len(p) != 2 || p[0] != "projects" || p[1] == "" {
		return nil, "", errInvalid("Invalid resource name %q.", name)
	}

	if req.GetFilter() != "" {
		return nil, "", errInvalid("filter is not supported.")
	}

	if req.GetPageSize() < 0 {
		return nil, "", errInvalid("page_size must not be negative.")
	}

	if err := page.ValidateToken(req.GetPageToken()); err != nil {
		return nil, "", errInvalid("Invalid page_token.")
	}

	all := make([]*location.Location, 0, len(regions))
	for _, r := range regions {
		all = append(all, locationOf(p[1], r))
	}

	slices.SortFunc(all, func(a, b *location.Location) int { return strings.Compare(a.GetName(), b.GetName()) })

	pg := page.New(all, req.GetPageToken(), int(req.GetPageSize()), defaultPageSize)

	return pg.Data, pg.Next, nil
}

// GetLocation returns one supported location.
func GetLocation(req *location.GetLocationRequest) (*location.Location, error) {
	n, err := parseParent(req.GetName())
	if err != nil {
		return nil, err
	}

	for _, r := range regions {
		if r[0] == n.location {
			return locationOf(n.project, r), nil
		}
	}

	return nil, errNotFound("Location %q is not supported.", req.GetName())
}
