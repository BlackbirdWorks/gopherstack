package vpclattice

import (
	"context"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// resolveTargetGroupID resolves a target group identifier to an ID.
func (b *InMemoryBackend) resolveTargetGroupID(identifier string) (string, bool) {
	if b.targetGroups.Has(identifier) {
		return identifier, true
	}
	for _, tg := range b.targetGroups.All() {
		if tg.ARN == identifier {
			return tg.ID, true
		}
	}

	return "", false
}

// ------- TargetGroup operations -------

// CreateTargetGroup creates a target group.
func (b *InMemoryBackend) CreateTargetGroup(
	ctx context.Context,
	name, tgType string,
	config *TargetGroupConfig,
	tags map[string]string,
) (*TargetGroup, error) {
	if name == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("CreateTargetGroup")
	defer b.mu.Unlock()

	if len(b.tgsByName.Get(name)) > 0 {
		return nil, ErrAlreadyExists
	}

	config = withTargetGroupDefaults(tgType, config)

	now := time.Now().UTC()
	id := newID(idPrefixTargetGroup)
	region := b.regionFor(ctx)
	tgARN := arn.Build(arnService, region, b.accountID, resourceTargetGroup+"/"+id)

	tg := &storedTargetGroup{
		ARN:           tgARN,
		ID:            id,
		Name:          name,
		Type:          tgType,
		Status:        tgStatusActive,
		Config:        config,
		Tags:          copyTags(tags),
		CreatedAt:     now,
		LastUpdatedAt: now,
		Region:        region,
	}

	b.targetGroups.Put(tg)
	b.targets[id] = make([]*storedTarget, 0)
	b.tags[tgARN] = copyTags(tags)

	return tg.toTargetGroup(), nil
}

// GetTargetGroup returns a target group.
func (b *InMemoryBackend) GetTargetGroup(tgID string) (*TargetGroup, error) {
	b.mu.RLock("GetTargetGroup")
	defer b.mu.RUnlock()

	id, ok := b.resolveTargetGroupID(tgID)
	if !ok {
		return nil, ErrNotFound
	}

	tg, _ := b.targetGroups.Get(id)

	return tg.toTargetGroup(), nil
}

// UpdateTargetGroup updates a target group's health check config.
func (b *InMemoryBackend) UpdateTargetGroup(
	tgID string,
	healthCheck *HealthCheckConfig,
) (*TargetGroup, error) {
	b.mu.Lock("UpdateTargetGroup")
	defer b.mu.Unlock()

	id, ok := b.resolveTargetGroupID(tgID)
	if !ok {
		return nil, ErrNotFound
	}

	tg, _ := b.targetGroups.Get(id)
	if tg.Config == nil {
		tg.Config = &TargetGroupConfig{}
	}

	if healthCheck != nil {
		tg.Config.HealthCheck = withHealthCheckDefaults(healthCheck, tg.Config.Port)
	}

	tg.LastUpdatedAt = time.Now().UTC()

	return tg.toTargetGroup(), nil
}

// DeleteTargetGroup deletes a target group. Real AWS rejects the delete with
// ConflictException while the target group is still referenced by a
// listener rule (or, per the doc comment, while creation is still in
// progress -- not applicable here, since this backend's Create paths are
// synchronous) -- see the DeleteTargetGroup doc comment in
// aws-sdk-go-v2/service/vpclattice's api_op_DeleteTargetGroup.go.
func (b *InMemoryBackend) DeleteTargetGroup(tgID string) error {
	b.mu.Lock("DeleteTargetGroup")
	defer b.mu.Unlock()

	id, ok := b.resolveTargetGroupID(tgID)
	if !ok {
		return ErrNotFound
	}

	tg, _ := b.targetGroups.Get(id)

	if b.targetGroupInUse(id, tg.ARN) {
		return ErrDependencyConflict
	}

	b.targetGroups.Delete(id)
	delete(b.targets, id)
	delete(b.tags, tg.ARN)

	return nil
}

// targetGroupInUse reports whether any listener rule forwards to the given
// target group (matched by ID or ARN, since clients may specify either as
// targetGroupIdentifier). Listener default actions are covered too, since
// CreateListener materializes a listener's default action as its default
// rule -- see createDefaultRule.
func (b *InMemoryBackend) targetGroupInUse(id, arn string) bool {
	for _, r := range b.rules.All() {
		if r.Action == nil {
			continue
		}

		for _, wtg := range r.Action.ForwardTargetGroups {
			if wtg.TargetGroupID == id || wtg.TargetGroupID == arn {
				return true
			}
		}
	}

	return false
}

// ListTargetGroups lists target groups with optional filters.
//
// ListTargetGroupsInput models maxResults, nextToken, targetGroupType and
// vpcIdentifier query parameters -- there is no serviceArn filter on this
// operation (aws-sdk-go-v2/service/vpclattice@v1.25.5 api_op_ListTargetGroups.go).
func (b *InMemoryBackend) ListTargetGroups(
	ctx context.Context,
	tgType, vpcID string,
	maxResults int32,
	nextToken string,
) ([]*TargetGroupSummary, string, error) {
	b.mu.RLock("ListTargetGroups")
	defer b.mu.RUnlock()

	region := b.regionFor(ctx)
	all := make([]*TargetGroupSummary, 0, b.targetGroups.Len())

	for _, tg := range b.targetGroups.All() {
		if tg.Region != region {
			continue
		}

		if tgType != "" && tg.Type != tgType {
			continue
		}

		if vpcID != "" && (tg.Config == nil || tg.Config.VpcID != vpcID) {
			continue
		}

		all = append(all, tg.toSummary())
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}

// withTargetGroupDefaults fills the defaults documented on types.TargetGroupConfig (types.go:1010-1045).
func withTargetGroupDefaults(tgType string, in *TargetGroupConfig) *TargetGroupConfig {
	if in == nil {
		return nil
	}

	cfg := *in

	switch tgType {
	case "LAMBDA":
		if cfg.LambdaEventStructureVersion == "" {
			cfg.LambdaEventStructureVersion = "V1"
		}

		return &cfg
	case "IP":
		if cfg.IPAddressType == "" {
			cfg.IPAddressType = "IPV4"
		}
	}

	if cfg.ProtocolVersion == "" {
		cfg.ProtocolVersion = "HTTP1"
	}

	if cfg.Port == 0 {
		switch cfg.Protocol {
		case "HTTP":
			cfg.Port = 80
		case protocolHTTPS:
			cfg.Port = 443
		}
	}

	if cfg.HealthCheck != nil {
		cfg.HealthCheck = withHealthCheckDefaults(cfg.HealthCheck, cfg.Port)
	}

	return &cfg
}

// withHealthCheckDefaults fills the defaults documented on types.HealthCheckConfig (types.go:255-295).
func withHealthCheckDefaults(in *HealthCheckConfig, targetPort int32) *HealthCheckConfig {
	hc := *in

	if hc.HealthCheckIntervalSeconds == 0 {
		hc.HealthCheckIntervalSeconds = 30
	}

	if hc.HealthCheckTimeoutSeconds == 0 {
		hc.HealthCheckTimeoutSeconds = 5
	}

	if hc.HealthyThresholdCount == 0 {
		hc.HealthyThresholdCount = 5
	}

	if hc.UnhealthyThresholdCount == 0 {
		hc.UnhealthyThresholdCount = 2
	}

	if hc.Path == "" {
		hc.Path = "/"
	}

	if hc.Protocol == "" {
		hc.Protocol = "HTTP"
	}

	if hc.Port == 0 {
		hc.Port = targetPort
	}

	return &hc
}
