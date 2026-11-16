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

func regionalConfig(project, region string) *instancepb.InstanceConfig {
	return buildConfig(project, regionalPrefix+region, region, def(rw(region, regionalReplicas, true)),
		regionalOptional[region])
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

	for id, d := range multiRegionConfigs {
		out = append(out, buildConfig(project, id, id, d.replicas, d.optional))
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

	pg := page.New(b.projectConfigs(project), in.GetPageToken(), int(in.GetPageSize()), defaultPageSize)

	return &instancepb.ListInstanceConfigsResponse{InstanceConfigs: pg.Data, NextPageToken: pg.Next}, nil
}

// GetInstanceConfig returns one configuration.
func (b *InMemoryBackend) GetInstanceConfig(name string) (*instancepb.InstanceConfig, error) {
	parts := strings.Split(name, "/")
	if len(parts) != instanceParts || parts[0] != projectsSegment || parts[2] != instanceConfigsSegment {
		return nil, errInvalid("Invalid instance config name %q.", name)
	}

	cfg, ok := b.lookupConfigName(name)
	if !ok {
		return nil, errNotFound("Instance config not found: %s", name)
	}

	return cloneConfig(cfg), nil
}

func cloneConfig(c *instancepb.InstanceConfig) *instancepb.InstanceConfig {
	out, _ := proto.Clone(c).(*instancepb.InstanceConfig)

	return out
}

// projectConfigs is the Google-managed catalog plus the project's user-managed configurations, sorted by name.
func (b *InMemoryBackend) projectConfigs(project string) []*instancepb.InstanceConfig {
	out := allConfigs(project)
	prefix := configName(project, "")

	b.mu.RLock("projectConfigs")
	for name, c := range b.userConfigs {
		if strings.HasPrefix(name, prefix) {
			out = append(out, cloneIC(c))
		}
	}
	b.mu.RUnlock()

	slices.SortFunc(out, func(a, c *instancepb.InstanceConfig) int { return strings.Compare(a.GetName(), c.GetName()) })

	return out
}
