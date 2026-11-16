package gcpspanner

import (
	"encoding/hex"
	"regexp"
	"slices"
	"strings"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	percentDone          = 100
	maxUserConfigs       = 64
	customConfigPrefix   = "custom-"
	maxConfigIDLen       = 64
	minConfigIDLen       = 2
	configOperationsPart = "/operations/"
)

var customConfigIDRe = regexp.MustCompile(`^custom-[-a-z0-9]*[a-z0-9]$`)

func (b *InMemoryBackend) nextEtagString() string { return hex.EncodeToString(b.nextEtag()) }

func progress(start, end *timestamppb.Timestamp) *instancepb.OperationProgress {
	return &instancepb.OperationProgress{ProgressPercent: percentDone, StartTime: start, EndTime: end}
}

func cloneIC(c *instancepb.InstanceConfig) *instancepb.InstanceConfig {
	out, _ := proto.Clone(c).(*instancepb.InstanceConfig)

	return out
}

// lookupConfigName returns a Google-managed or user-managed configuration.
func (b *InMemoryBackend) lookupConfigName(name string) (*instancepb.InstanceConfig, bool) {
	b.mu.RLock("lookupConfigName")
	defer b.mu.RUnlock()

	return b.lookupConfigLocked(name)
}

func (b *InMemoryBackend) lookupConfigLocked(name string) (*instancepb.InstanceConfig, bool) {
	parts := strings.Split(name, "/")
	if len(parts) != instanceParts || parts[0] != projectsSegment || parts[2] != instanceConfigsSegment {
		return nil, false
	}

	if c, ok := lookupConfig(parts[1], parts[3]); ok {
		return c, true
	}

	c, ok := b.userConfigs[name]
	if !ok {
		return nil, false
	}

	return cloneIC(c), true
}

// configGoneLocked reports an instance configuration that was deleted after the caller resolved it.
func (b *InMemoryBackend) configGoneLocked(name string) error {
	if _, ok := b.lookupConfigLocked(name); !ok {
		return errNotFound("Instance config not found: %s", name)
	}

	return nil
}

func replicaKey(r *instancepb.ReplicaInfo) string {
	return r.GetType().String() + "/" + r.GetLocation()
}

func countReplicas(rs []*instancepb.ReplicaInfo) map[string]int {
	out := map[string]int{}
	for _, r := range rs {
		out[replicaKey(r)]++
	}

	return out
}

// validateCustomReplicas requires all base replicas plus at least one optional replica and nothing else.
func validateCustomReplicas(base *instancepb.InstanceConfig, replicas []*instancepb.ReplicaInfo) error {
	got := countReplicas(replicas)
	want := countReplicas(base.GetReplicas())
	opt := countReplicas(base.GetOptionalReplicas())
	extra := 0

	for k, n := range got {
		extra += max(n-want[k], 0)

		if n < want[k] {
			return errInvalid("replicas must include every replica of %s.", base.GetName())
		}

		if n > want[k] && n-want[k] > opt[k] {
			return errInvalid("Replica %s is not among the optional_replicas of %s.", k, base.GetName())
		}
	}

	for k, n := range want {
		if got[k] < n {
			return errInvalid("replicas must include every replica of %s.", base.GetName())
		}
	}

	if extra == 0 {
		return errInvalid("replicas must add at least one of the optional_replicas of %s.", base.GetName())
	}

	return nil
}

func validateLabels(labels map[string]string) error {
	if len(labels) > maxLabels {
		return errInvalid("At most %d labels are allowed.", maxLabels)
	}

	return nil
}

func (b *InMemoryBackend) buildUserConfig(
	project, id string, in *instancepb.InstanceConfig,
) (*instancepb.InstanceConfig, error) {
	if len(id) < minConfigIDLen || len(id) > maxConfigIDLen || !customConfigIDRe.MatchString(id) {
		return nil, errInvalid("instance_config_id %q must match custom-[-a-z0-9]*[a-z0-9] and be 2-64 characters.", id)
	}

	if in == nil {
		return nil, errInvalid("instance_config is required.")
	}

	name := configName(project, id)
	if in.GetName() != "" && in.GetName() != name {
		return nil, errInvalid("instance_config.name must be %s.", name)
	}

	if err := validateLabels(in.GetLabels()); err != nil {
		return nil, err
	}

	if in.GetBaseConfig() == "" {
		return nil, errInvalid("instance_config.base_config is required.")
	}

	if !strings.HasPrefix(in.GetBaseConfig(), configName(project, "")) {
		return nil, errInvalid("base_config must be a configuration of project %s.", project)
	}

	base, ok := b.lookupConfigName(in.GetBaseConfig())
	if !ok {
		return nil, errNotFound("Instance config not found: %s", in.GetBaseConfig())
	}

	if base.GetConfigType() != instancepb.InstanceConfig_GOOGLE_MANAGED {
		return nil, errInvalid("base_config must be a Google-managed configuration.")
	}

	if err := validateCustomReplicas(base, in.GetReplicas()); err != nil {
		return nil, err
	}

	display := in.GetDisplayName()
	if display == "" {
		display = id
	}

	return &instancepb.InstanceConfig{
		Name: name, DisplayName: display, ConfigType: instancepb.InstanceConfig_USER_MANAGED,
		Replicas: in.GetReplicas(), BaseConfig: in.GetBaseConfig(), Labels: in.GetLabels(),
		LeaderOptions: leaderOptions(in.GetReplicas()), State: instancepb.InstanceConfig_READY,
	}, nil
}

// CreateInstanceConfig creates a user-managed configuration from a Google-managed base.
func (b *InMemoryBackend) CreateInstanceConfig(
	in *instancepb.CreateInstanceConfigRequest,
) (*longrunningpb.Operation, error) {
	project, err := parseProject(in.GetParent())
	if err != nil {
		return nil, err
	}

	cfg, err := b.buildUserConfig(project, in.GetInstanceConfigId(), in.GetInstanceConfig())
	if err != nil {
		return nil, err
	}

	start := b.ts()

	if !in.GetValidateOnly() {
		b.mu.Lock("CreateInstanceConfig")
		if _, dup := b.userConfigs[cfg.GetName()]; dup {
			b.mu.Unlock()

			return nil, errExists("Instance config already exists: %s", cfg.GetName())
		}

		if len(b.userConfigs) >= maxUserConfigs {
			b.mu.Unlock()

			return nil, errExhausted(
				"At most %d user-managed instance configs can exist in the emulator.",
				maxUserConfigs,
			)
		}

		cfg.Etag = b.nextEtagString()
		b.userConfigs[cfg.GetName()] = cloneIC(cfg)
		b.mu.Unlock()
	}

	meta := &instancepb.CreateInstanceConfigMetadata{InstanceConfig: cfg, Progress: progress(start, b.ts())}

	return b.finished(opName(cfg.GetName(), ""), meta, cfg)
}

func updatableConfigPath(p string) bool {
	name, _, _ := strings.Cut(p, ".")

	return name == fieldDisplayName || name == "labels"
}

// UpdateInstanceConfig changes display_name and labels of a user-managed configuration.
func (b *InMemoryBackend) UpdateInstanceConfig(
	in *instancepb.UpdateInstanceConfigRequest,
) (*longrunningpb.Operation, error) {
	pb := in.GetInstanceConfig()
	if pb == nil || pb.GetName() == "" {
		return nil, errInvalid("instance_config.name is required.")
	}

	paths := in.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		return nil, errInvalid("update_mask is required.")
	}

	if i := slices.IndexFunc(paths, func(p string) bool { return !updatableConfigPath(p) }); i >= 0 {
		return nil, errInvalid("Update mask path %q is not an updatable field: only display_name and labels.", paths[i])
	}

	if err := validateLabels(pb.GetLabels()); err != nil {
		return nil, err
	}

	start := b.ts()

	b.mu.Lock("UpdateInstanceConfig")
	cur, ok := b.userConfigs[pb.GetName()]

	if !ok {
		b.mu.Unlock()

		return nil, b.missingConfigErr(pb.GetName())
	}

	if pb.GetEtag() != "" && pb.GetEtag() != cur.GetEtag() {
		b.mu.Unlock()

		return nil, errEtag(pb.GetName())
	}

	next := cloneIC(cur)
	if maskHas(paths, "display_name") {
		next.DisplayName = pb.GetDisplayName()
	}

	if maskHas(paths, "labels") {
		next.Labels = pb.GetLabels()
	}

	if !in.GetValidateOnly() {
		next.Etag = b.nextEtagString()
		b.userConfigs[next.GetName()] = cloneIC(next)
	}
	b.mu.Unlock()

	meta := &instancepb.UpdateInstanceConfigMetadata{InstanceConfig: next, Progress: progress(start, b.ts())}

	return b.finished(opName(next.GetName(), ""), meta, next)
}

// missingConfigErr distinguishes an unknown configuration from a Google-managed one.
func (b *InMemoryBackend) missingConfigErr(name string) error {
	if c, ok := b.lookupGoogleConfig(name); ok {
		return errInvalid(
			"Instance config %s is Google-managed; only user-managed configs can be changed.",
			c.GetName(),
		)
	}

	return errNotFound("Instance config not found: %s", name)
}

func (b *InMemoryBackend) lookupGoogleConfig(name string) (*instancepb.InstanceConfig, bool) {
	parts := strings.Split(name, "/")
	if len(parts) != instanceParts || parts[0] != projectsSegment || parts[2] != instanceConfigsSegment {
		return nil, false
	}

	return lookupConfig(parts[1], parts[3])
}

// DeleteInstanceConfig deletes a user-managed configuration that no instance uses.
func (b *InMemoryBackend) DeleteInstanceConfig(in *instancepb.DeleteInstanceConfigRequest) (*emptypb.Empty, error) {
	b.mu.Lock("DeleteInstanceConfig")
	defer b.mu.Unlock()

	cur, ok := b.userConfigs[in.GetName()]
	if !ok {
		return nil, b.missingConfigErr(in.GetName())
	}

	if in.GetEtag() != "" && in.GetEtag() != cur.GetEtag() {
		return nil, errEtag(in.GetName())
	}

	if b.configInUseLocked(in.GetName()) {
		return nil, errPrecondition("Instance config %s is used by an instance or instance partition.", in.GetName())
	}

	if !in.GetValidateOnly() {
		delete(b.userConfigs, in.GetName())
	}

	return &emptypb.Empty{}, nil
}

func (b *InMemoryBackend) configInUseLocked(name string) bool {
	for _, rec := range b.instances {
		if rec.pb.GetConfig() == name {
			return true
		}

		for _, p := range rec.parts {
			if p.GetConfig() == name {
				return true
			}
		}
	}

	return false
}

// ListInstanceConfigOperations lists the operations of user-managed configurations of a project.
func (b *InMemoryBackend) ListInstanceConfigOperations(
	in *instancepb.ListInstanceConfigOperationsRequest,
) (*instancepb.ListInstanceConfigOperationsResponse, error) {
	project, err := parseProject(in.GetParent())
	if err != nil {
		return nil, err
	}

	ops, err := b.listOps(configName(project, customConfigPrefix), in.GetFilter(), in.GetPageSize(), in.GetPageToken())
	if err != nil {
		return nil, err
	}

	return &instancepb.ListInstanceConfigOperationsResponse{Operations: ops.Data, NextPageToken: ops.Next}, nil
}

// listOps pages the filtered operations under prefix.
func (b *InMemoryBackend) listOps(
	prefix, filter string,
	size int32,
	token string,
) (page.Page[*longrunningpb.Operation], error) {
	var zero page.Page[*longrunningpb.Operation]

	if size < 0 {
		return zero, errInvalid("page_size must not be negative.")
	}

	if err := page.ValidateToken(token); err != nil {
		return zero, errInvalid("Invalid page_token.")
	}

	var matched []*longrunningpb.Operation

	for _, o := range b.ops.Under(prefix) {
		if strings.Contains(strings.TrimPrefix(o.GetName(), prefix), configOperationsPart) {
			matched = append(matched, o)
		}
	}

	all, err := filterOps(matched, filter)
	if err != nil {
		return zero, err
	}

	return page.New(all, token, int(size), defaultOpsPage), nil
}
