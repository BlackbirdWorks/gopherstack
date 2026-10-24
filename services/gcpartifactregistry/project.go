package gcpartifactregistry

import (
	"strings"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"google.golang.org/protobuf/proto"
)

const (
	maxPullPercent = 100
	vpcscLen       = 5
)

func checkSettingsName(name string) error {
	p := strings.Split(name, "/")
	if len(p) != segLocs+1 || p[0] != segProjects || p[1] == "" || p[2] != "projectSettings" {
		return errInvalid("Invalid project settings name %q.", name)
	}

	return nil
}

func checkVPCSCName(name string) error {
	p := strings.Split(name, "/")
	if len(p) != vpcscLen || p[0] != segProjects || p[segLocs] != segLocations || p[1] == "" || p[3] == "" ||
		p[3] == allLocs || p[4] != "vpcscConfig" {
		return errInvalid("Invalid VPC-SC config name %q.", name)
	}

	return nil
}

func cloneSettings(s *artifactregistrypb.ProjectSettings) *artifactregistrypb.ProjectSettings {
	out, _ := proto.Clone(s).(*artifactregistrypb.ProjectSettings)

	return out
}

// GetProjectSettings returns a project's settings; a project that never changed them has the defaults.
func (b *InMemoryBackend) GetProjectSettings(name string) (*artifactregistrypb.ProjectSettings, error) {
	if err := checkSettingsName(name); err != nil {
		return nil, err
	}

	b.mu.RLock("GetProjectSettings")
	defer b.mu.RUnlock()

	if s, ok := b.settings[name]; ok {
		return cloneSettings(s), nil
	}

	return &artifactregistrypb.ProjectSettings{
		Name:                   name,
		LegacyRedirectionState: artifactregistrypb.ProjectSettings_REDIRECTION_FROM_GCR_IO_DISABLED,
	}, nil
}

// UpdateProjectSettings applies legacy_redirection_state and pull_percent.
func (b *InMemoryBackend) UpdateProjectSettings(
	in *artifactregistrypb.UpdateProjectSettingsRequest,
) (*artifactregistrypb.ProjectSettings, error) {
	name := in.GetProjectSettings().GetName()

	cur, err := b.GetProjectSettings(name)
	if err != nil {
		return nil, err
	}

	paths := maskPaths(in.GetUpdateMask())
	if err = checkPaths(paths, []string{"legacy_redirection_state", "pull_percent"}); err != nil {
		return nil, err
	}

	src := in.GetProjectSettings()

	if wants(paths, "legacy_redirection_state") && (len(paths) > 0 || src.GetLegacyRedirectionState() != 0) {
		state := src.GetLegacyRedirectionState()
		if _, ok := artifactregistrypb.ProjectSettings_RedirectionState_name[int32(state)]; !ok ||
			state == artifactregistrypb.ProjectSettings_REDIRECTION_STATE_UNSPECIFIED {
			return nil, errInvalid("legacy_redirection_state must be a known redirection state.")
		}

		cur.LegacyRedirectionState = state
	}

	if wants(paths, "pull_percent") && (len(paths) > 0 || src.GetPullPercent() != 0) {
		if src.GetPullPercent() < 0 || src.GetPullPercent() > maxPullPercent {
			return nil, errInvalid("pull_percent must be between 0 and %d.", maxPullPercent)
		}

		cur.PullPercent = src.GetPullPercent()
	}

	b.mu.Lock("UpdateProjectSettings")
	b.settings[name] = cur
	b.mu.Unlock()

	return cloneSettings(cur), nil
}

// GetVPCSCConfig returns a location's VPC-SC policy, ALLOW until it is changed.
func (b *InMemoryBackend) GetVPCSCConfig(name string) (*artifactregistrypb.VPCSCConfig, error) {
	if err := checkVPCSCName(name); err != nil {
		return nil, err
	}

	b.mu.RLock("GetVPCSCConfig")
	defer b.mu.RUnlock()

	if c, ok := b.vpcsc[name]; ok {
		out, _ := proto.Clone(c).(*artifactregistrypb.VPCSCConfig)

		return out, nil
	}

	return &artifactregistrypb.VPCSCConfig{Name: name, VpcscPolicy: artifactregistrypb.VPCSCConfig_ALLOW}, nil
}

// UpdateVPCSCConfig sets vpcsc_policy.
func (b *InMemoryBackend) UpdateVPCSCConfig(
	in *artifactregistrypb.UpdateVPCSCConfigRequest,
) (*artifactregistrypb.VPCSCConfig, error) {
	name := in.GetVpcscConfig().GetName()

	cur, err := b.GetVPCSCConfig(name)
	if err != nil {
		return nil, err
	}

	paths := maskPaths(in.GetUpdateMask())
	if err = checkPaths(paths, []string{"vpcsc_policy"}); err != nil {
		return nil, err
	}

	policy := in.GetVpcscConfig().GetVpcscPolicy()

	if wants(paths, "vpcsc_policy") && (len(paths) > 0 || policy != 0) {
		if policy != artifactregistrypb.VPCSCConfig_ALLOW && policy != artifactregistrypb.VPCSCConfig_DENY {
			return nil, errInvalid("vpcsc_policy must be ALLOW or DENY.")
		}

		cur.VpcscPolicy = policy
	}

	b.mu.Lock("UpdateVPCSCConfig")
	b.vpcsc[name] = cur
	b.mu.Unlock()

	out, _ := proto.Clone(cur).(*artifactregistrypb.VPCSCConfig)

	return out, nil
}
