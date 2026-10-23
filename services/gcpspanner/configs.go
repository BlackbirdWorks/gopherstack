package gcpspanner

import (
	"slices"
	"strings"

	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	usEast1         = "us-east1"
	usCentral1      = "us-central1"
	europeWest1     = "europe-west1"
	europeWest4     = "europe-west4"
	asiaNortheast1  = "asia-northeast1"
	emulatorConfig  = "emulator-config"
	defaultPageSize = 100
	regionalPrefix  = "regional-"
)

// regionalConfigs are the Google-managed regional instance configurations.
var regionalConfigs = []string{ //nolint:gochecknoglobals // immutable catalog
	"asia-east1", "asia-east2", asiaNortheast1, "asia-northeast2", "asia-northeast3", "asia-south1",
	"asia-south2", "asia-southeast1", "asia-southeast2", "australia-southeast1", "australia-southeast2",
	"europe-central2", "europe-north1", "europe-southwest1", europeWest1, "europe-west2", "europe-west3",
	europeWest4, "europe-west6", "europe-west8", "europe-west9", "me-central1", "me-west1",
	"northamerica-northeast1", "northamerica-northeast2", "southamerica-east1", "southamerica-west1",
	usCentral1, usEast1, "us-east4", "us-east5", "us-south1", "us-west1", "us-west2", "us-west3", "us-west4",
}

var multiRegionConfigs = map[string][]string{ //nolint:gochecknoglobals // immutable catalog
	"nam3":          {"us-east4", usEast1, usCentral1},
	"nam6":          {usCentral1, usEast1, "us-central2"},
	"nam-eur-asia1": {usCentral1, "us-central2", europeWest1, asiaNortheast1},
	"eur3":          {europeWest1, europeWest4, "europe-north1"},
	"eur5":          {"europe-west2", europeWest1, europeWest4},
	"asia1":         {asiaNortheast1, "asia-northeast2", "asia-northeast3"},
}

func regionalConfig(project, region string) *instancepb.InstanceConfig {
	reps := make([]*instancepb.ReplicaInfo, 0, 3) //nolint:mnd // regional configs have three replicas
	for range 3 {
		reps = append(reps, &instancepb.ReplicaInfo{
			Location: region, Type: instancepb.ReplicaInfo_READ_WRITE, DefaultLeaderLocation: len(reps) == 0,
		})
	}

	return &instancepb.InstanceConfig{
		Name:          configName(project, regionalPrefix+region),
		DisplayName:   region,
		ConfigType:    instancepb.InstanceConfig_GOOGLE_MANAGED,
		Replicas:      reps,
		LeaderOptions: []string{region},
		State:         instancepb.InstanceConfig_READY,
	}
}

func multiRegionConfig(project, id string, regions []string) *instancepb.InstanceConfig {
	reps := make([]*instancepb.ReplicaInfo, 0, len(regions))
	for i, r := range regions {
		reps = append(reps, &instancepb.ReplicaInfo{
			Location: r, Type: instancepb.ReplicaInfo_READ_WRITE, DefaultLeaderLocation: i == 0,
		})
	}

	return &instancepb.InstanceConfig{
		Name:          configName(project, id),
		DisplayName:   id,
		ConfigType:    instancepb.InstanceConfig_GOOGLE_MANAGED,
		Replicas:      reps,
		LeaderOptions: slices.Clone(regions),
		State:         instancepb.InstanceConfig_READY,
	}
}

func emulatorInstanceConfig(project string) *instancepb.InstanceConfig {
	return &instancepb.InstanceConfig{
		Name:        configName(project, emulatorConfig),
		DisplayName: "Emulator Instance Config",
		ConfigType:  instancepb.InstanceConfig_GOOGLE_MANAGED,
		Replicas: []*instancepb.ReplicaInfo{
			{Location: "emulator-location", Type: instancepb.ReplicaInfo_READ_WRITE, DefaultLeaderLocation: true},
		},
		LeaderOptions: []string{"emulator-location"},
		State:         instancepb.InstanceConfig_READY,
	}
}

// allConfigs returns the catalog for a project, sorted by name.
func allConfigs(project string) []*instancepb.InstanceConfig {
	out := make([]*instancepb.InstanceConfig, 0, 1+len(regionalConfigs)+len(multiRegionConfigs))
	out = append(out, emulatorInstanceConfig(project))

	for _, r := range regionalConfigs {
		out = append(out, regionalConfig(project, r))
	}

	for id, regions := range multiRegionConfigs {
		out = append(out, multiRegionConfig(project, id, regions))
	}

	slices.SortFunc(out, func(a, b *instancepb.InstanceConfig) int { return strings.Compare(a.GetName(), b.GetName()) })

	return out
}

func lookupConfig(project, id string) (*instancepb.InstanceConfig, bool) {
	for _, c := range allConfigs(project) {
		if c.GetName() == configName(project, id) {
			return c, true
		}
	}

	return nil, false
}

// ListInstanceConfigs lists the Google-managed configurations of a project.
func (b *InMemoryBackend) ListInstanceConfigs(
	in *instancepb.ListInstanceConfigsRequest,
) (*instancepb.ListInstanceConfigsResponse, error) {
	project, err := parseProject(in.GetParent())
	if err != nil {
		return nil, err
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	if err = page.ValidateToken(in.GetPageToken()); err != nil {
		return nil, errInvalid("Invalid page_token.")
	}

	pg := page.New(allConfigs(project), in.GetPageToken(), int(in.GetPageSize()), defaultPageSize)

	return &instancepb.ListInstanceConfigsResponse{InstanceConfigs: pg.Data, NextPageToken: pg.Next}, nil
}

// GetInstanceConfig returns one configuration.
func (b *InMemoryBackend) GetInstanceConfig(name string) (*instancepb.InstanceConfig, error) {
	parts := strings.Split(name, "/")
	if len(parts) != instanceParts || parts[0] != projectsSegment || parts[2] != "instanceConfigs" {
		return nil, errInvalid("Invalid instance config name %q.", name)
	}

	cfg, ok := lookupConfig(parts[1], parts[3])
	if !ok {
		return nil, errNotFound("Instance config not found: %s", name)
	}

	return cloneConfig(cfg), nil
}

func cloneConfig(c *instancepb.InstanceConfig) *instancepb.InstanceConfig {
	out, _ := proto.Clone(c).(*instancepb.InstanceConfig)

	return out
}
