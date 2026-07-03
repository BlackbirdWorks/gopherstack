package cloudfront

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const metricDisabled = "Disabled"

// ---------------------------------------------------------------------------
// Error sentinels
// ---------------------------------------------------------------------------

// ErrTrustStoreNotFound is returned when a trust store does not exist.
var ErrTrustStoreNotFound = awserr.New("NoSuchTrustStore", awserr.ErrNotFound)

// ErrStreamingDistributionNotFound is returned when a streaming distribution does not exist.
var ErrStreamingDistributionNotFound = awserr.New("NoSuchStreamingDistribution", awserr.ErrNotFound)

// ErrStreamingDistributionNotDisabled is returned when deleting a streaming distribution
// that is still enabled.
var ErrStreamingDistributionNotDisabled = awserr.New("StreamingDistributionNotDisabled", awserr.ErrConflict)

// ---------------------------------------------------------------------------
// TrustStore
// ---------------------------------------------------------------------------

// TrustStore represents a CloudFront trust store.
type TrustStore struct {
	ID      string `json:"id"`
	ARN     string `json:"arn"`
	Name    string `json:"name"`
	Comment string `json:"comment,omitempty"`
	ETag    string `json:"etag"`
}

func (b *InMemoryBackend) trustStoreARN(id string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:trust-store/%s", b.accountID, id)
}

func (b *InMemoryBackend) CreateTrustStore(name, comment string) (*TrustStore, error) {
	b.mu.Lock("CreateTrustStore")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}
	id := generateID()
	ts := &TrustStore{
		ID:      id,
		ARN:     b.trustStoreARN(id),
		Name:    name,
		Comment: comment,
		ETag:    uuid.NewString(),
	}
	b.trustStores[id] = ts
	cp := *ts

	return &cp, nil
}

func (b *InMemoryBackend) GetTrustStore(id string) (*TrustStore, error) {
	b.mu.RLock("GetTrustStore")
	defer b.mu.RUnlock()

	ts, ok := b.trustStores[id]
	if !ok {
		return nil, ErrTrustStoreNotFound
	}
	cp := *ts

	return &cp, nil
}

func (b *InMemoryBackend) ListTrustStores() []*TrustStore {
	b.mu.RLock("ListTrustStores")
	defer b.mu.RUnlock()

	out := make([]*TrustStore, 0, len(b.trustStores))
	for _, ts := range b.trustStores {
		cp := *ts
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

func (b *InMemoryBackend) UpdateTrustStore(id, comment string) (*TrustStore, error) {
	b.mu.Lock("UpdateTrustStore")
	defer b.mu.Unlock()

	ts, ok := b.trustStores[id]
	if !ok {
		return nil, ErrTrustStoreNotFound
	}
	if comment != "" {
		ts.Comment = comment
	}
	ts.ETag = uuid.NewString()
	cp := *ts

	return &cp, nil
}

func (b *InMemoryBackend) DeleteTrustStore(id string) error {
	b.mu.Lock("DeleteTrustStore")
	defer b.mu.Unlock()

	if _, ok := b.trustStores[id]; !ok {
		return ErrTrustStoreNotFound
	}
	delete(b.trustStores, id)

	return nil
}

// ---------------------------------------------------------------------------
// StreamingDistribution
// ---------------------------------------------------------------------------

// StreamingDistributionS3Origin models the S3Origin element of a StreamingDistributionConfig.
type StreamingDistributionS3Origin struct {
	DomainName           string `json:"domainName,omitempty"`
	OriginAccessIdentity string `json:"originAccessIdentity,omitempty"`
}

// StreamingDistributionTrustedSigners models the TrustedSigners element of a
// StreamingDistributionConfig.
type StreamingDistributionTrustedSigners struct {
	Items   []string `json:"items,omitempty"`
	Enabled bool     `json:"enabled"`
}

// StreamingDistributionConfig models the mutable configuration of a streaming distribution.
type StreamingDistributionConfig struct {
	CallerReference string                              `json:"callerReference"`
	Comment         string                              `json:"comment,omitempty"`
	PriceClass      string                              `json:"priceClass,omitempty"`
	S3Origin        StreamingDistributionS3Origin       `json:"s3Origin"`
	Aliases         []string                            `json:"aliases,omitempty"`
	TrustedSigners  StreamingDistributionTrustedSigners `json:"trustedSigners"`
	Enabled         bool                                `json:"enabled"`
}

// StreamingDistribution represents a CloudFront RTMP streaming distribution.
type StreamingDistribution struct {
	Tags             map[string]string           `json:"tags,omitempty"`
	ARN              string                      `json:"arn"`
	DomainName       string                      `json:"domainName"`
	Status           string                      `json:"status"`
	ETag             string                      `json:"etag"`
	ID               string                      `json:"id"`
	LastModifiedTime string                      `json:"lastModifiedTime,omitempty"`
	RawConfig        []byte                      `json:"rawConfig,omitempty"`
	Config           StreamingDistributionConfig `json:"config"`
}

func (b *InMemoryBackend) streamingDistributionARN(id string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:streaming-distribution/%s", b.accountID, id)
}

// copyStreamingDistribution returns a deep copy of a StreamingDistribution.
// Must be called with the lock held.
func (b *InMemoryBackend) copyStreamingDistribution(sd *StreamingDistribution) *StreamingDistribution {
	cp := *sd
	cp.Config.Aliases = append([]string(nil), sd.Config.Aliases...)
	cp.Config.TrustedSigners.Items = append([]string(nil), sd.Config.TrustedSigners.Items...)
	cp.RawConfig = append([]byte(nil), sd.RawConfig...)

	if sd.Tags != nil {
		cp.Tags = make(map[string]string, len(sd.Tags))
		maps.Copy(cp.Tags, sd.Tags)
	}

	return &cp
}

// CreateStreamingDistribution creates a new RTMP streaming distribution.
// If a streaming distribution with the same non-empty CallerReference already exists, it
// is returned without creating a duplicate (idempotent).
func (b *InMemoryBackend) CreateStreamingDistribution(
	cfg StreamingDistributionConfig,
	rawConfig []byte,
) (*StreamingDistribution, error) {
	b.mu.Lock("CreateStreamingDistribution")
	defer b.mu.Unlock()

	if cfg.CallerReference != "" {
		if existingID, ok := b.streamingDistributionCallerRefs[cfg.CallerReference]; ok {
			return b.copyStreamingDistribution(b.streamingDistributions[existingID]), nil
		}
	}

	id := generateID()
	sd := &StreamingDistribution{
		ID:               id,
		ARN:              b.streamingDistributionARN(id),
		DomainName:       strings.ToLower(id) + ".cloudfront.net",
		Status:           statusDeployed,
		ETag:             uuid.NewString(),
		LastModifiedTime: time.Now().UTC().Format(time.RFC3339),
		Config:           cfg,
		RawConfig:        rawConfig,
		Tags:             make(map[string]string),
	}
	b.streamingDistributions[id] = sd
	b.streamingDistributionARNs[sd.ARN] = id

	if cfg.CallerReference != "" {
		b.streamingDistributionCallerRefs[cfg.CallerReference] = id
	}

	return b.copyStreamingDistribution(sd), nil
}

// GetStreamingDistribution returns a streaming distribution by ID.
func (b *InMemoryBackend) GetStreamingDistribution(id string) (*StreamingDistribution, error) {
	b.mu.RLock("GetStreamingDistribution")
	defer b.mu.RUnlock()

	sd, ok := b.streamingDistributions[id]
	if !ok {
		return nil, fmt.Errorf("%w: streaming distribution %s not found", ErrStreamingDistributionNotFound, id)
	}

	return b.copyStreamingDistribution(sd), nil
}

// ListStreamingDistributions returns all streaming distributions sorted by ID.
func (b *InMemoryBackend) ListStreamingDistributions() []*StreamingDistribution {
	b.mu.RLock("ListStreamingDistributions")
	defer b.mu.RUnlock()

	out := make([]*StreamingDistribution, 0, len(b.streamingDistributions))
	for _, sd := range b.streamingDistributions {
		out = append(out, b.copyStreamingDistribution(sd))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

// UpdateStreamingDistribution updates an existing streaming distribution's config.
// CallerReference is immutable and is preserved from the existing config.
func (b *InMemoryBackend) UpdateStreamingDistribution(
	id string,
	cfg StreamingDistributionConfig,
	rawConfig []byte,
) (*StreamingDistribution, error) {
	b.mu.Lock("UpdateStreamingDistribution")
	defer b.mu.Unlock()

	sd, ok := b.streamingDistributions[id]
	if !ok {
		return nil, fmt.Errorf("%w: streaming distribution %s not found", ErrStreamingDistributionNotFound, id)
	}

	cfg.CallerReference = sd.Config.CallerReference
	sd.Config = cfg
	sd.RawConfig = rawConfig
	sd.ETag = uuid.NewString()
	sd.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)

	return b.copyStreamingDistribution(sd), nil
}

// DeleteStreamingDistribution deletes a streaming distribution by ID.
// The streaming distribution must be disabled first, mirroring AWS's requirement.
func (b *InMemoryBackend) DeleteStreamingDistribution(id string) error {
	b.mu.Lock("DeleteStreamingDistribution")
	defer b.mu.Unlock()

	sd, ok := b.streamingDistributions[id]
	if !ok {
		return fmt.Errorf("%w: streaming distribution %s not found", ErrStreamingDistributionNotFound, id)
	}

	if sd.Config.Enabled {
		return fmt.Errorf(
			"%w: streaming distribution %s must be disabled before it can be deleted",
			ErrStreamingDistributionNotDisabled, id,
		)
	}

	delete(b.streamingDistributionARNs, sd.ARN)
	delete(b.streamingDistributionCallerRefs, sd.Config.CallerReference)
	delete(b.streamingDistributions, id)

	return nil
}

// ---------------------------------------------------------------------------
// MonitoringSubscription (per-distribution)
// ---------------------------------------------------------------------------

// MonitoringSubscription represents real-time metrics settings for a distribution.
type MonitoringSubscription struct {
	RealtimeMetricsSubscriptionStatus string `json:"realtimeMetricsSubscriptionStatus"`
}

func (b *InMemoryBackend) CreateMonitoringSubscription(distributionID string, enabled bool) error {
	b.mu.Lock("CreateMonitoringSubscription")
	defer b.mu.Unlock()

	status := metricDisabled
	if enabled {
		status = "Enabled"
	}
	b.monitoringSubscriptions[distributionID] = &MonitoringSubscription{
		RealtimeMetricsSubscriptionStatus: status,
	}

	return nil
}

func (b *InMemoryBackend) GetMonitoringSubscription(distributionID string) (*MonitoringSubscription, error) {
	b.mu.RLock("GetMonitoringSubscription")
	defer b.mu.RUnlock()

	ms, ok := b.monitoringSubscriptions[distributionID]
	if !ok {
		return &MonitoringSubscription{RealtimeMetricsSubscriptionStatus: "Disabled"}, nil
	}
	cp := *ms

	return &cp, nil
}

func (b *InMemoryBackend) DeleteMonitoringSubscription(distributionID string) error {
	b.mu.Lock("DeleteMonitoringSubscription")
	defer b.mu.Unlock()

	delete(b.monitoringSubscriptions, distributionID)

	return nil
}

// ---------------------------------------------------------------------------
// ResourcePolicy
// ---------------------------------------------------------------------------

// resourcePolicyEntry stores a CloudFront resource policy.
type resourcePolicyEntry struct {
	Policy string
}

func (b *InMemoryBackend) PutResourcePolicy(resourceARN, policy string) error {
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	b.resourcePolicies[resourceARN] = &resourcePolicyEntry{Policy: policy}

	return nil
}

func (b *InMemoryBackend) GetResourcePolicy(resourceARN string) (string, error) {
	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	e, ok := b.resourcePolicies[resourceARN]
	if !ok {
		return "{}", nil // return empty policy if not set
	}

	return e.Policy, nil
}

func (b *InMemoryBackend) DeleteResourcePolicy(resourceARN string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	delete(b.resourcePolicies, resourceARN)

	return nil
}

// ---------------------------------------------------------------------------
// ConnectionGroup extra operations (Get/List/Update/Delete)
// ---------------------------------------------------------------------------

func (b *InMemoryBackend) GetConnectionGroup(id string) (*ConnectionGroup, error) {
	b.mu.RLock("GetConnectionGroup")
	defer b.mu.RUnlock()

	cg, ok := b.connectionGroups[id]
	if !ok {
		return nil, ErrConnectionGroupNotFound
	}
	cp := *cg

	return &cp, nil
}

func (b *InMemoryBackend) GetConnectionGroupByRoutingEndpoint(endpoint string) (*ConnectionGroup, error) {
	b.mu.RLock("GetConnectionGroupByRoutingEndpoint")
	defer b.mu.RUnlock()

	// Simple: match by name or ID (endpoint = name in our simplified model).
	for _, cg := range b.connectionGroups {
		if cg.Name == endpoint || cg.ID == endpoint {
			cp := *cg

			return &cp, nil
		}
	}

	return nil, ErrConnectionGroupNotFound
}

func (b *InMemoryBackend) ListConnectionGroups() []*ConnectionGroup {
	b.mu.RLock("ListConnectionGroups")
	defer b.mu.RUnlock()

	out := make([]*ConnectionGroup, 0, len(b.connectionGroups))
	for _, cg := range b.connectionGroups {
		cp := *cg
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

func (b *InMemoryBackend) UpdateConnectionGroup(id, comment string) (*ConnectionGroup, error) {
	b.mu.Lock("UpdateConnectionGroup")
	defer b.mu.Unlock()

	cg, ok := b.connectionGroups[id]
	if !ok {
		return nil, ErrConnectionGroupNotFound
	}
	if comment != "" {
		cg.Comment = comment
	}
	cp := *cg

	return &cp, nil
}

func (b *InMemoryBackend) DeleteConnectionGroup(id string) error {
	b.mu.Lock("DeleteConnectionGroup")
	defer b.mu.Unlock()

	if _, ok := b.connectionGroups[id]; !ok {
		return ErrConnectionGroupNotFound
	}
	delete(b.connectionGroups, id)

	return nil
}

// ---------------------------------------------------------------------------
// ConnectionFunction extra operations (Get/Describe/List/Update/Delete/Publish/Test)
// ---------------------------------------------------------------------------

func (b *InMemoryBackend) GetConnectionFunction(id string) (*ConnectionFunction, error) {
	b.mu.RLock("GetConnectionFunction")
	defer b.mu.RUnlock()

	fn, ok := b.connectionFunctions[id]
	if !ok {
		return nil, ErrConnectionFunctionNotFound
	}
	cp := *fn

	return &cp, nil
}

func (b *InMemoryBackend) ListConnectionFunctions() []*ConnectionFunction {
	b.mu.RLock("ListConnectionFunctions")
	defer b.mu.RUnlock()

	out := make([]*ConnectionFunction, 0, len(b.connectionFunctions))
	for _, fn := range b.connectionFunctions {
		cp := *fn
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

func (b *InMemoryBackend) UpdateConnectionFunction(id, comment string) (*ConnectionFunction, error) {
	b.mu.Lock("UpdateConnectionFunction")
	defer b.mu.Unlock()

	fn, ok := b.connectionFunctions[id]
	if !ok {
		return nil, ErrConnectionFunctionNotFound
	}
	if comment != "" {
		fn.Comment = comment
	}
	cp := *fn

	return &cp, nil
}

func (b *InMemoryBackend) DeleteConnectionFunction(id string) error {
	b.mu.Lock("DeleteConnectionFunction")
	defer b.mu.Unlock()

	if _, ok := b.connectionFunctions[id]; !ok {
		return ErrConnectionFunctionNotFound
	}
	delete(b.connectionFunctions, id)

	return nil
}

// ---------------------------------------------------------------------------
// AnycastIPList extra operations (Get/List/Update/Delete)
// ---------------------------------------------------------------------------

func (b *InMemoryBackend) GetAnycastIPList(id string) (*AnycastIPList, error) {
	b.mu.RLock("GetAnycastIPList")
	defer b.mu.RUnlock()

	list, ok := b.anycastIPLists[id]
	if !ok {
		return nil, ErrAnycastIPListNotFound
	}
	cp := *list

	return &cp, nil
}

func (b *InMemoryBackend) ListAnycastIPLists() []*AnycastIPList {
	b.mu.RLock("ListAnycastIPLists")
	defer b.mu.RUnlock()

	out := make([]*AnycastIPList, 0, len(b.anycastIPLists))
	for _, list := range b.anycastIPLists {
		cp := *list
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

func (b *InMemoryBackend) UpdateAnycastIPList(id string, ipCount int32) (*AnycastIPList, error) {
	b.mu.Lock("UpdateAnycastIPList")
	defer b.mu.Unlock()

	list, ok := b.anycastIPLists[id]
	if !ok {
		return nil, ErrAnycastIPListNotFound
	}
	if ipCount > 0 {
		list.IPCount = ipCount
	}
	cp := *list

	return &cp, nil
}

func (b *InMemoryBackend) DeleteAnycastIPList(id string) error {
	b.mu.Lock("DeleteAnycastIPList")
	defer b.mu.Unlock()

	if _, ok := b.anycastIPLists[id]; !ok {
		return ErrAnycastIPListNotFound
	}
	delete(b.anycastIPLists, id)

	return nil
}

// ---------------------------------------------------------------------------
// ListDistributionsBy* helpers
// ---------------------------------------------------------------------------

// ListDistributionsByWebACLID returns distribution IDs associated with a web ACL.
func (b *InMemoryBackend) ListDistributionsByWebACLID(webACLID string) []*Distribution {
	b.mu.RLock("ListDistributionsByWebACLID")
	defer b.mu.RUnlock()

	var out []*Distribution
	for distID, wID := range b.distributionWebACLs {
		if wID == webACLID {
			if d, ok := b.distributions[distID]; ok {
				cp := *d
				out = append(out, &cp)
			}
		}
	}

	return out
}

// ListDistributionsByCachePolicyID returns distributions that reference a cache policy.
func (b *InMemoryBackend) ListDistributionsByCachePolicyID(policyID string) []*Distribution {
	b.mu.RLock("ListDistributionsByCachePolicyID")
	defer b.mu.RUnlock()

	return b.distributionsByPolicyID(b.distributionCachePolicies, policyID)
}

// ListDistributionsByOriginRequestPolicyID returns distributions that reference an origin request policy.
func (b *InMemoryBackend) ListDistributionsByOriginRequestPolicyID(policyID string) []*Distribution {
	b.mu.RLock("ListDistributionsByOriginRequestPolicyID")
	defer b.mu.RUnlock()

	return b.distributionsByPolicyID(b.distributionOriginRequestPolicies, policyID)
}

// ListDistributionsByResponseHeadersPolicyID returns distributions that reference a response headers policy.
func (b *InMemoryBackend) ListDistributionsByResponseHeadersPolicyID(policyID string) []*Distribution {
	b.mu.RLock("ListDistributionsByResponseHeadersPolicyID")
	defer b.mu.RUnlock()

	return b.distributionsByPolicyID(b.distributionResponseHeadersPolicies, policyID)
}

// ListDistributionsByRealtimeLogConfigARN returns distributions that reference a realtime log config.
func (b *InMemoryBackend) ListDistributionsByRealtimeLogConfigARN(arn string) []*Distribution {
	b.mu.RLock("ListDistributionsByRealtimeLogConfigARN")
	defer b.mu.RUnlock()

	return b.distributionsByPolicyID(b.distributionRealtimeLogConfigs, arn)
}

// distributionsByPolicyID is a helper that filters distributions by a policy ID from an index map.
// Must be called with RLock held.
func (b *InMemoryBackend) distributionsByPolicyID(index map[string]string, policyID string) []*Distribution {
	var out []*Distribution
	for distID, pid := range index {
		if pid == policyID {
			if d, ok := b.distributions[distID]; ok {
				cp := *d
				out = append(out, &cp)
			}
		}
	}

	return out
}

// SetDistributionCachePolicyID records a distribution→cache policy association.
func (b *InMemoryBackend) SetDistributionCachePolicyID(distID, policyID string) {
	b.mu.Lock("SetDistributionCachePolicyID")
	defer b.mu.Unlock()

	if policyID == "" {
		delete(b.distributionCachePolicies, distID)
	} else {
		b.distributionCachePolicies[distID] = policyID
	}
}

// SetDistributionOriginRequestPolicyID records a distribution→origin request policy association.
func (b *InMemoryBackend) SetDistributionOriginRequestPolicyID(distID, policyID string) {
	b.mu.Lock("SetDistributionOriginRequestPolicyID")
	defer b.mu.Unlock()

	if policyID == "" {
		delete(b.distributionOriginRequestPolicies, distID)
	} else {
		b.distributionOriginRequestPolicies[distID] = policyID
	}
}

// SetDistributionResponseHeadersPolicyID records a distribution→response headers policy association.
func (b *InMemoryBackend) SetDistributionResponseHeadersPolicyID(distID, policyID string) {
	b.mu.Lock("SetDistributionResponseHeadersPolicyID")
	defer b.mu.Unlock()

	if policyID == "" {
		delete(b.distributionResponseHeadersPolicies, distID)
	} else {
		b.distributionResponseHeadersPolicies[distID] = policyID
	}
}

// SetDistributionRealtimeLogConfigARN records a distribution→realtime log config association.
func (b *InMemoryBackend) SetDistributionRealtimeLogConfigARN(distID, arn string) {
	b.mu.Lock("SetDistributionRealtimeLogConfigARN")
	defer b.mu.Unlock()

	if arn == "" {
		delete(b.distributionRealtimeLogConfigs, distID)
	} else {
		b.distributionRealtimeLogConfigs[distID] = arn
	}
}
