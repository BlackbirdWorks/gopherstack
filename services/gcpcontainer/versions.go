//nolint:staticcheck // legacy cluster fields are deprecated in the proto but still served
package gcpcontainer

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"cloud.google.com/go/container/apiv1/containerpb"
)

const (
	v1343  = "1.34.3-gke.1051000"
	v1337  = "1.33.7-gke.1044000"
	v13211 = "1.32.11-gke.1090000"

	versionParts     = 3
	latestVersion    = "latest"
	maxNodeSkew      = 2
	defaultImageType = "COS_CONTAINERD"
)

type channelSpec struct {
	def      string
	target   string
	versions []string
	channel  containerpb.ReleaseChannel_Channel
}

// channels is a documented representative version catalog per release channel, newest first. The emulator
// does not track Google's rollout; the shape (channel -> default, valid and upgrade-target versions) matches the API.
func channels() []channelSpec {
	return []channelSpec{
		{
			channel: containerpb.ReleaseChannel_RAPID, def: "1.35.0-gke.1233000", target: "1.35.1-gke.1000000",
			versions: []string{"1.35.1-gke.1000000", "1.35.0-gke.1233000", v1343},
		},
		{
			channel: containerpb.ReleaseChannel_REGULAR, def: "1.34.2-gke.1008000", target: v1343,
			versions: []string{v1343, "1.34.2-gke.1008000", v1337},
		},
		{
			channel: containerpb.ReleaseChannel_STABLE, def: "1.33.6-gke.1021000", target: v1337,
			versions: []string{v1337, "1.33.6-gke.1021000", v13211},
		},
		{
			channel: containerpb.ReleaseChannel_EXTENDED, def: "1.32.10-gke.1049000", target: v13211,
			versions: []string{v13211, "1.32.10-gke.1049000", "1.31.14-gke.1084000"},
		},
	}
}

func channelByEnum(c containerpb.ReleaseChannel_Channel) (channelSpec, bool) {
	for _, s := range channels() {
		if s.channel == c {
			return s, true
		}
	}

	return channelSpec{}, false
}

func defaultVersion() string {
	s, _ := channelByEnum(containerpb.ReleaseChannel_REGULAR)

	return s.def
}

// versionKey orders "1.M.P-gke.B" versions.
func versionKey(v string) [4]int {
	var k [4]int

	core, build, _ := strings.Cut(v, "-gke.")
	for i, p := range strings.Split(core, ".") {
		if i < versionParts {
			k[i], _ = strconv.Atoi(p)
		}
	}

	k[3], _ = strconv.Atoi(build)

	return k
}

func compareVersions(a, b string) int {
	ka, kb := versionKey(a), versionKey(b)

	return slices.Compare(ka[:], kb[:])
}

func sortedDesc(in []string) []string {
	out := slices.Clone(in)
	slices.SortFunc(out, func(a, b string) int { return -compareVersions(a, b) })

	return slices.Compact(out)
}

func validMasterVersions() []string {
	var all []string
	for _, s := range channels() {
		all = append(all, s.versions...)
	}

	return sortedDesc(all)
}

func validNodeVersions() []string {
	return sortedDesc(append(validMasterVersions(), "1.30.14-gke.1050000", "1.29.15-gke.1039000"))
}

func validImageTypes() []string {
	return []string{"COS_CONTAINERD", "UBUNTU_CONTAINERD", "WINDOWS_LTSC_CONTAINERD", "WINDOWS_SAC_CONTAINERD"}
}

// pickVersion resolves a request against candidates (newest first): "", "latest", an exact version or a
// "1.M" / "1.M.P" prefix.
func pickVersion(req string, candidates []string, def string) (string, bool) {
	switch req {
	case "":
		return def, def != ""
	case latestVersion:
		return candidates[0], true
	}

	if slices.Contains(candidates, req) {
		return req, true
	}

	for _, c := range candidates {
		if strings.HasPrefix(c, req+".") || strings.HasPrefix(c, req+"-") {
			return c, true
		}
	}

	return "", false
}

func resolveMasterVersion(req string, channel containerpb.ReleaseChannel_Channel) (string, error) {
	if spec, ok := channelByEnum(channel); ok {
		v, found := pickVersion(req, spec.versions, spec.def)
		if !found {
			return "", errInvalid("initial_cluster_version",
				"Master version %q is unsupported in the %s release channel.", req, channel)
		}

		return v, nil
	}

	v, found := pickVersion(req, validMasterVersions(), defaultVersion())
	if !found {
		return "", errInvalid("initial_cluster_version", "Master version %q is unsupported.", req)
	}

	return v, nil
}

func resolveNodeVersion(req, master string) (string, error) {
	if req == "" {
		return master, nil
	}

	v, found := pickVersion(req, validNodeVersions(), "")
	if !found {
		return "", errInvalid("version", "Node version %q is unsupported.", req)
	}

	if err := checkSkew(v, master); err != nil {
		return "", err
	}

	return v, nil
}

func minor(v string) int { return versionKey(v)[1] }

func checkSkew(node, master string) error {
	if d := minor(master) - minor(node); d < 0 || d > maxNodeSkew {
		return errInvalid(
			"version",
			"Node version %s must be the master version %s or at most %d minor versions older.",
			node,
			master,
			maxNodeSkew,
		)
	}

	return nil
}

func serverConfig() *containerpb.ServerConfig {
	cfg := &containerpb.ServerConfig{
		DefaultClusterVersion: defaultVersion(),
		ValidNodeVersions:     validNodeVersions(),
		DefaultImageType:      defaultImageType,
		ValidImageTypes:       validImageTypes(),
		ValidMasterVersions:   validMasterVersions(),
	}

	for _, s := range channels() {
		cfg.Channels = append(cfg.Channels, &containerpb.ServerConfig_ReleaseChannelConfig{
			Channel: s.channel, DefaultVersion: s.def, ValidVersions: slices.Clone(s.versions),
			UpgradeTargetVersion: s.target,
		})
	}

	slices.SortFunc(cfg.GetChannels(), func(a, b *containerpb.ServerConfig_ReleaseChannelConfig) int {
		return cmp.Compare(a.GetChannel(), b.GetChannel())
	})

	return cfg
}

// GetServerConfig returns the supported versions and image types for a location.
func (b *InMemoryBackend) GetServerConfig(req *containerpb.GetServerConfigRequest) (*containerpb.ServerConfig, error) {
	project, location, err := resolveLocation(req.GetName(), req.GetProjectId(), req.GetZone())
	if err != nil {
		return nil, err
	}

	if !validLocation(location) {
		return nil, errNotFound(fmt.Sprintf("projects/%s/locations/%s", project, location))
	}

	return serverConfig(), nil
}
