package bedrockagent

import (
	"context"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	vpcStatusCreating = "CREATING"
	vpcStatusCreated  = "CREATED"
	vpcStatusDeleting = "DELETING"

	vpcProtocolHTTP     = "HTTP"
	vpcProtocolHTTPS    = "HTTPS"
	vpcResolutionPublic = "PUBLIC"
	vpcResolutionInVPC  = "IN_VPC"

	maxPort               = 65535
	vpcConfigurationIDLen = 10
)

// VpcConfigurationConfig is the CreateVpcConfiguration request.
type VpcConfigurationConfig struct {
	Name           string
	Description    string
	VpcID          string
	ResourceTarget string
	Protocol       string
	ResolutionMode string
	HostHeader     string
	TLSServerName  string
	ClientToken    string
	SubnetIDs      []string
	Port           int32
}

// VpcConfiguration is a knowledge base VPC connection. Status is derived at
// read time from CreatedAt/DeleteStartedAt and the lifecycle delay.
type VpcConfiguration struct {
	CreatedAt          time.Time
	UpdatedAt          time.Time
	DeleteStartedAt    time.Time
	VpcConfigurationID string
	KnowledgeBaseID    string
	Status             string
	Name               string
	Description        string
	VpcID              string
	ResourceTarget     string
	Protocol           string
	ResolutionMode     string
	HostHeader         string
	TLSServerName      string
	ClientToken        string
	SubnetIDs          []string
	Port               int32
}

func vpcConfigurationKeyFn(v *VpcConfiguration) string {
	return v.KnowledgeBaseID + "/" + v.VpcConfigurationID
}

func vpcConfigurationByKBKeyFn(v *VpcConfiguration) string { return v.KnowledgeBaseID }

func validateVpcConfiguration(cfg VpcConfigurationConfig) error {
	if cfg.VpcID == "" {
		return fmt.Errorf("%w: vpcId is required", ErrValidation)
	}

	if len(cfg.SubnetIDs) == 0 {
		return fmt.Errorf("%w: subnetIds must contain at least one subnet", ErrValidation)
	}

	if cfg.Port < 1 || cfg.Port > maxPort {
		return fmt.Errorf("%w: port must be between 1 and %d", ErrValidation, maxPort)
	}

	if err := validateEnum("protocol", cfg.Protocol, []string{vpcProtocolHTTP, vpcProtocolHTTPS}); err != nil {
		return err
	}

	if cfg.Protocol == "" {
		return fmt.Errorf("%w: protocol is required", ErrValidation)
	}

	resolution := []string{vpcResolutionPublic, vpcResolutionInVPC}
	if err := validateEnum("resolutionMode", cfg.ResolutionMode, resolution); err != nil {
		return err
	}

	if cfg.ResolutionMode == "" {
		return fmt.Errorf("%w: resolutionMode is required", ErrValidation)
	}

	if cfg.Protocol == vpcProtocolHTTPS && cfg.TLSServerName == "" {
		return fmt.Errorf("%w: tlsServerName is required when protocol is HTTPS", ErrValidation)
	}

	if cfg.TLSServerName != "" && !validHostname(strings.TrimPrefix(cfg.TLSServerName, "*.")) {
		return fmt.Errorf("%w: tlsServerName must be a hostname without a port", ErrValidation)
	}

	return validateResourceTarget(cfg.ResourceTarget)
}

func validHostname(h string) bool {
	if h == "" || len(h) > 253 || strings.ContainsAny(h, ":/ @*") {
		return false
	}

	return !strings.HasPrefix(h, ".") && !strings.HasSuffix(h, ".")
}

// validateResourceTarget accepts a private IPv4 address or a DNS name and
// rejects the target kinds CreateVpcConfiguration documents as unsupported.
func validateResourceTarget(target string) error {
	if target == "" {
		return fmt.Errorf("%w: resourceTarget is required", ErrValidation)
	}

	addr, err := netip.ParseAddr(target)
	if err != nil {
		if strings.EqualFold(target, "localhost") || !validHostname(target) {
			return fmt.Errorf(
				"%w: resourceTarget %q is not a private IPv4 address or domain name",
				ErrValidation,
				target,
			)
		}

		return nil
	}

	if !addr.Is4() || !addr.IsPrivate() || addr.IsLoopback() || addr.IsLinkLocalUnicast() ||
		addr.IsUnspecified() || addr.IsMulticast() {
		return fmt.Errorf("%w: resourceTarget %q must be a private IPv4 address", ErrValidation, target)
	}

	return nil
}

// vpcViewLocked copies v with its time-derived status. Caller holds b.mu.
func (b *InMemoryBackend) vpcViewLocked(v *VpcConfiguration) *VpcConfiguration {
	cp := *v
	cp.SubnetIDs = slices.Clone(v.SubnetIDs)
	delay := time.Duration(b.lifecycleDelay.Load())

	switch {
	case !v.DeleteStartedAt.IsZero():
		cp.Status = vpcStatusDeleting
	case delay > 0 && time.Since(v.CreatedAt) < delay:
		cp.Status = vpcStatusCreating
	default:
		cp.Status = vpcStatusCreated
	}

	return &cp
}

func (b *InMemoryBackend) vpcGoneLocked(v *VpcConfiguration) bool {
	if v.DeleteStartedAt.IsZero() {
		return false
	}

	delay := time.Duration(b.lifecycleDelay.Load())

	return delay <= 0 || time.Since(v.DeleteStartedAt) >= delay
}

// purgeGoneVpcLocked drops configurations whose deletion has completed.
func (b *InMemoryBackend) purgeGoneVpcLocked(kbID string) {
	for _, v := range slices.Clone(b.vpcConfigurationsByKB.Get(kbID)) {
		if b.vpcGoneLocked(v) {
			b.vpcConfigurations.Delete(vpcConfigurationKeyFn(v))
		}
	}
}

func (b *InMemoryBackend) findVpcLocked(kbID, id string) (*VpcConfiguration, error) {
	if !b.knowledgeBases.Has(kbID) {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	b.purgeGoneVpcLocked(kbID)

	v, ok := b.vpcConfigurations.Get(kbID + "/" + id)
	if !ok {
		return nil, fmt.Errorf("%w: vpc configuration %q not found", ErrNotFound, id)
	}

	return v, nil
}

// CreateVpcConfiguration registers a VPC connection for a knowledge base. A
// repeated ClientToken returns the existing configuration.
func (b *InMemoryBackend) CreateVpcConfiguration(
	_ context.Context, kbID string, cfg VpcConfigurationConfig,
) (*VpcConfiguration, error) {
	if err := validateVpcConfiguration(cfg); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateVpcConfiguration")
	defer b.mu.Unlock()

	if !b.knowledgeBases.Has(kbID) {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	b.purgeGoneVpcLocked(kbID)

	if prior := findByClientToken(b.vpcConfigurations, cfg.ClientToken,
		func(v *VpcConfiguration) string { return v.ClientToken },
		func(v *VpcConfiguration) bool { return v.KnowledgeBaseID == kbID },
	); prior != nil {
		return b.vpcViewLocked(prior), nil
	}

	now := time.Now().UTC()
	v := &VpcConfiguration{
		VpcConfigurationID: awsid.NewN("", vpcConfigurationIDLen),
		KnowledgeBaseID:    kbID,
		Name:               cfg.Name,
		Description:        cfg.Description,
		VpcID:              cfg.VpcID,
		SubnetIDs:          slices.Clone(cfg.SubnetIDs),
		ResourceTarget:     cfg.ResourceTarget,
		Port:               cfg.Port,
		Protocol:           cfg.Protocol,
		ResolutionMode:     cfg.ResolutionMode,
		HostHeader:         cfg.HostHeader,
		TLSServerName:      cfg.TLSServerName,
		ClientToken:        cfg.ClientToken,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	b.vpcConfigurations.Put(v)

	out := b.vpcViewLocked(v)
	out.Status = vpcStatusCreating

	return out, nil
}

// GetVpcConfiguration returns one VPC configuration.
func (b *InMemoryBackend) GetVpcConfiguration(_ context.Context, kbID, id string) (*VpcConfiguration, error) {
	b.mu.Lock("GetVpcConfiguration")
	defer b.mu.Unlock()

	v, err := b.findVpcLocked(kbID, id)
	if err != nil {
		return nil, err
	}

	return b.vpcViewLocked(v), nil
}

// ListVpcConfigurations lists a knowledge base's VPC configurations, oldest first.
func (b *InMemoryBackend) ListVpcConfigurations(
	_ context.Context, kbID, statusFilter string, maxResults int, nextToken string,
) ([]*VpcConfiguration, string, error) {
	if statusFilter != "" && !slices.Contains(vpcStatuses(), statusFilter) {
		return nil, "", fmt.Errorf("%w: invalid status %q", ErrValidation, statusFilter)
	}

	b.mu.Lock("ListVpcConfigurations")
	defer b.mu.Unlock()

	if !b.knowledgeBases.Has(kbID) {
		return nil, "", fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	b.purgeGoneVpcLocked(kbID)

	views := make(map[string]*VpcConfiguration)

	var ordered []*VpcConfiguration

	for _, v := range b.vpcConfigurationsByKB.Get(kbID) {
		view := b.vpcViewLocked(v)
		if statusFilter != "" && view.Status != statusFilter {
			continue
		}

		views[view.VpcConfigurationID] = view
		ordered = append(ordered, view)
	}

	slices.SortFunc(ordered, func(a, c *VpcConfiguration) int {
		if d := a.CreatedAt.Compare(c.CreatedAt); d != 0 {
			return d
		}

		return strings.Compare(a.VpcConfigurationID, c.VpcConfigurationID)
	})

	ids := make([]string, len(ordered))
	for i, v := range ordered {
		ids[i] = v.VpcConfigurationID
	}

	ids, next := paginate(ids, nextToken, maxResults)

	out := make([]*VpcConfiguration, 0, len(ids))
	for _, id := range ids {
		out = append(out, views[id])
	}

	return out, next, nil
}

func vpcStatuses() []string {
	return []string{vpcStatusCreating, vpcStatusCreated, vpcStatusDeleting, "CREATE_FAILED", "DELETE_FAILED"}
}

// DeleteVpcConfiguration starts deletion; the configuration then reads as
// DELETING until the lifecycle delay elapses, after which it is gone.
func (b *InMemoryBackend) DeleteVpcConfiguration(_ context.Context, kbID, id string) (*VpcConfiguration, error) {
	b.mu.Lock("DeleteVpcConfiguration")
	defer b.mu.Unlock()

	v, err := b.findVpcLocked(kbID, id)
	if err != nil {
		return nil, err
	}

	if v.DeleteStartedAt.IsZero() {
		v.DeleteStartedAt = time.Now().UTC()
		v.UpdatedAt = v.DeleteStartedAt
	}

	return b.vpcViewLocked(v), nil
}
