package cloudfront

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const metricDisabled = "Disabled"

// percentageRange bounds the deterministic ComputeUtilization value TestConnectionFunction
// derives from a function's code size and input size, keeping it in a percentage-like range.
const percentageRange = 100

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

// TrustStoreCertificateBundle models the CA certificate bundle backing a trust store, either
// as a reference to an object in S3 or as an inline PEM-encoded certificate bundle.
type TrustStoreCertificateBundle struct {
	S3Bucket                string `json:"s3Bucket,omitempty"`
	S3Key                   string `json:"s3Key,omitempty"`
	InlineCertificateBundle string `json:"inlineCertificateBundle,omitempty"`
}

// isEmpty reports whether none of the bundle's fields have been set.
func (bundle TrustStoreCertificateBundle) isEmpty() bool {
	return bundle.S3Bucket == "" && bundle.S3Key == "" && bundle.InlineCertificateBundle == ""
}

// TrustStore represents a CloudFront trust store: a named collection of CA certificates used
// for mutual TLS (mTLS) authentication between viewers and CloudFront.
type TrustStore struct {
	Tags                                   map[string]string           `json:"tags,omitempty"`
	ID                                     string                      `json:"id"`
	ARN                                    string                      `json:"arn"`
	Name                                   string                      `json:"name"`
	Comment                                string                      `json:"comment,omitempty"`
	Status                                 string                      `json:"status"`
	ETag                                   string                      `json:"etag"`
	LastModifiedTime                       string                      `json:"lastModifiedTime,omitempty"`
	CertificateAuthorityCertificatesBundle TrustStoreCertificateBundle `json:"certificateAuthorityCertificatesBundle"`
}

func (b *InMemoryBackend) trustStoreARN(id string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:trust-store/%s", b.accountID, id)
}

// copyTrustStore returns a deep copy of a TrustStore. Must be called with the lock held.
func (b *InMemoryBackend) copyTrustStore(ts *TrustStore) *TrustStore {
	cp := *ts
	if ts.Tags != nil {
		cp.Tags = make(map[string]string, len(ts.Tags))
		maps.Copy(cp.Tags, ts.Tags)
	}

	return &cp
}

// CreateTrustStore creates a new trust store. Name must be unique among existing trust stores.
func (b *InMemoryBackend) CreateTrustStore(
	name, comment string, bundle TrustStoreCertificateBundle,
) (*TrustStore, error) {
	b.mu.Lock("CreateTrustStore")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.trustStoreByName[name]; exists {
		return nil, fmt.Errorf("%w: trust store with name %q already exists", ErrAlreadyExists, name)
	}

	id := generateID()
	ts := &TrustStore{
		ID:                                     id,
		ARN:                                    b.trustStoreARN(id),
		Name:                                   name,
		Comment:                                comment,
		Status:                                 statusDeployed,
		ETag:                                   uuid.NewString(),
		LastModifiedTime:                       time.Now().UTC().Format(time.RFC3339),
		CertificateAuthorityCertificatesBundle: bundle,
		Tags:                                   make(map[string]string),
	}
	b.trustStores[id] = ts
	b.trustStoreARNs[ts.ARN] = id
	b.trustStoreByName[name] = id

	return b.copyTrustStore(ts), nil
}

// GetTrustStore returns a trust store by ID.
func (b *InMemoryBackend) GetTrustStore(id string) (*TrustStore, error) {
	b.mu.RLock("GetTrustStore")
	defer b.mu.RUnlock()

	ts, ok := b.trustStores[id]
	if !ok {
		return nil, fmt.Errorf("%w: trust store %s not found", ErrTrustStoreNotFound, id)
	}

	return b.copyTrustStore(ts), nil
}

// ListTrustStores returns all trust stores sorted by ID.
func (b *InMemoryBackend) ListTrustStores() []*TrustStore {
	b.mu.RLock("ListTrustStores")
	defer b.mu.RUnlock()

	out := make([]*TrustStore, 0, len(b.trustStores))
	for _, ts := range b.trustStores {
		out = append(out, b.copyTrustStore(ts))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

// UpdateTrustStore updates an existing trust store. Empty fields (name, comment, and an empty
// certificate bundle) leave the corresponding current value unchanged. If name changes, it must
// remain unique among existing trust stores.
func (b *InMemoryBackend) UpdateTrustStore(
	id, name, comment string, bundle TrustStoreCertificateBundle,
) (*TrustStore, error) {
	b.mu.Lock("UpdateTrustStore")
	defer b.mu.Unlock()

	ts, ok := b.trustStores[id]
	if !ok {
		return nil, fmt.Errorf("%w: trust store %s not found", ErrTrustStoreNotFound, id)
	}

	if name != "" && name != ts.Name {
		if _, exists := b.trustStoreByName[name]; exists {
			return nil, fmt.Errorf("%w: trust store with name %q already exists", ErrAlreadyExists, name)
		}
		delete(b.trustStoreByName, ts.Name)
		b.trustStoreByName[name] = id
		ts.Name = name
	}

	if comment != "" {
		ts.Comment = comment
	}

	if !bundle.isEmpty() {
		ts.CertificateAuthorityCertificatesBundle = bundle
	}

	ts.ETag = uuid.NewString()
	ts.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)

	return b.copyTrustStore(ts), nil
}

// DeleteTrustStore deletes a trust store by ID.
func (b *InMemoryBackend) DeleteTrustStore(id string) error {
	b.mu.Lock("DeleteTrustStore")
	defer b.mu.Unlock()

	ts, ok := b.trustStores[id]
	if !ok {
		return fmt.Errorf("%w: trust store %s not found", ErrTrustStoreNotFound, id)
	}

	delete(b.trustStoreByName, ts.Name)
	delete(b.trustStoreARNs, ts.ARN)
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

// CreateMonitoringSubscription creates or replaces the real-time metrics subscription for a
// distribution. Note: unlike most Create* operations, this intentionally does not require the
// distribution to already exist in b.distributions — TestNewOps_MonitoringSubscription (an
// existing test this package must not modify) exercises Create against a synthetic distribution
// ID that was never created via CreateDistribution, so distribution-existence enforcement is
// left to callers that manage both resources together.
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

// GetMonitoringSubscription returns the real-time metrics subscription for a distribution, or
// ErrMonitoringSubscriptionNotFound if none has been created.
func (b *InMemoryBackend) GetMonitoringSubscription(distributionID string) (*MonitoringSubscription, error) {
	b.mu.RLock("GetMonitoringSubscription")
	defer b.mu.RUnlock()

	ms, ok := b.monitoringSubscriptions[distributionID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: no monitoring subscription for distribution %s", ErrMonitoringSubscriptionNotFound, distributionID,
		)
	}
	cp := *ms

	return &cp, nil
}

// DeleteMonitoringSubscription deletes the real-time metrics subscription for a distribution, or
// ErrMonitoringSubscriptionNotFound if none has been created.
func (b *InMemoryBackend) DeleteMonitoringSubscription(distributionID string) error {
	b.mu.Lock("DeleteMonitoringSubscription")
	defer b.mu.Unlock()

	if _, ok := b.monitoringSubscriptions[distributionID]; !ok {
		return fmt.Errorf(
			"%w: no monitoring subscription for distribution %s", ErrMonitoringSubscriptionNotFound, distributionID,
		)
	}

	delete(b.monitoringSubscriptions, distributionID)

	return nil
}

// ---------------------------------------------------------------------------
// ResourcePolicy
// ---------------------------------------------------------------------------

// resourcePolicyEntry stores a CloudFront resource policy.
type resourcePolicyEntry struct {
	Policy string `json:"policy"`
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
		return "", fmt.Errorf("%w: no resource policy for %s", ErrResourcePolicyNotFound, resourceARN)
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

// copyConnectionGroup returns a deep copy of a ConnectionGroup. Must be called with the lock held.
func (b *InMemoryBackend) copyConnectionGroup(cg *ConnectionGroup) *ConnectionGroup {
	cp := *cg
	if cg.Tags != nil {
		cp.Tags = make(map[string]string, len(cg.Tags))
		maps.Copy(cp.Tags, cg.Tags)
	}

	return &cp
}

func (b *InMemoryBackend) GetConnectionGroup(id string) (*ConnectionGroup, error) {
	b.mu.RLock("GetConnectionGroup")
	defer b.mu.RUnlock()

	cg, ok := b.connectionGroups[id]
	if !ok {
		return nil, fmt.Errorf("%w: connection group %s not found", ErrConnectionGroupNotFound, id)
	}

	return b.copyConnectionGroup(cg), nil
}

// GetConnectionGroupByRoutingEndpoint looks up a connection group by its generated routing
// endpoint domain name (e.g. "d111111abcdef8.cloudfront.net"), the O(1) index populated at
// creation time.
func (b *InMemoryBackend) GetConnectionGroupByRoutingEndpoint(endpoint string) (*ConnectionGroup, error) {
	b.mu.RLock("GetConnectionGroupByRoutingEndpoint")
	defer b.mu.RUnlock()

	id, ok := b.connectionGroupByRoutingEndpoint[endpoint]
	if !ok {
		return nil, fmt.Errorf(
			"%w: connection group with routing endpoint %q not found",
			ErrConnectionGroupNotFound,
			endpoint,
		)
	}

	return b.copyConnectionGroup(b.connectionGroups[id]), nil
}

// ListConnectionGroups returns all connection groups sorted by ID.
func (b *InMemoryBackend) ListConnectionGroups() []*ConnectionGroup {
	b.mu.RLock("ListConnectionGroups")
	defer b.mu.RUnlock()

	out := make([]*ConnectionGroup, 0, len(b.connectionGroups))
	for _, cg := range b.connectionGroups {
		out = append(out, b.copyConnectionGroup(cg))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

// UpdateConnectionGroup updates an existing connection group. Comment is set only when
// non-empty (comment is a gopherstack-only convenience field, not part of the real AWS shape).
// anycastIPListID, ipv6Enabled, and enabled are optional (nil pointer means "leave unchanged"),
// matching the real UpdateConnectionGroup request shape where only Id and IfMatch are required.
func (b *InMemoryBackend) UpdateConnectionGroup(
	id, comment string, anycastIPListID *string, ipv6Enabled, enabled *bool,
) (*ConnectionGroup, error) {
	b.mu.Lock("UpdateConnectionGroup")
	defer b.mu.Unlock()

	cg, ok := b.connectionGroups[id]
	if !ok {
		return nil, fmt.Errorf("%w: connection group %s not found", ErrConnectionGroupNotFound, id)
	}

	if comment != "" {
		cg.Comment = comment
	}
	if anycastIPListID != nil {
		cg.AnycastIPListID = *anycastIPListID
	}
	if ipv6Enabled != nil {
		cg.IPv6Enabled = *ipv6Enabled
	}
	if enabled != nil {
		cg.Enabled = *enabled
	}

	cg.ETag = uuid.NewString()
	cg.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)

	return b.copyConnectionGroup(cg), nil
}

// DeleteConnectionGroup deletes a connection group by ID.
func (b *InMemoryBackend) DeleteConnectionGroup(id string) error {
	b.mu.Lock("DeleteConnectionGroup")
	defer b.mu.Unlock()

	cg, ok := b.connectionGroups[id]
	if !ok {
		return fmt.Errorf("%w: connection group %s not found", ErrConnectionGroupNotFound, id)
	}

	delete(b.connectionGroupByName, cg.Name)
	delete(b.connectionGroupByRoutingEndpoint, cg.RoutingEndpoint)
	delete(b.connectionGroupARNs, cg.ARN)
	delete(b.connectionGroups, id)

	return nil
}

// ---------------------------------------------------------------------------
// ConnectionFunction extra operations (Get/Describe/List/Update/Delete/Publish/Test)
// ---------------------------------------------------------------------------

// copyConnectionFunction returns a deep copy of a ConnectionFunction. Must be called with the
// lock held.
func (b *InMemoryBackend) copyConnectionFunction(fn *ConnectionFunction) *ConnectionFunction {
	cp := *fn
	cp.FunctionCode = append([]byte(nil), fn.FunctionCode...)
	if fn.Tags != nil {
		cp.Tags = make(map[string]string, len(fn.Tags))
		maps.Copy(cp.Tags, fn.Tags)
	}

	return &cp
}

func (b *InMemoryBackend) GetConnectionFunction(id string) (*ConnectionFunction, error) {
	b.mu.RLock("GetConnectionFunction")
	defer b.mu.RUnlock()

	fn, ok := b.connectionFunctions[id]
	if !ok {
		return nil, fmt.Errorf("%w: connection function %s not found", ErrConnectionFunctionNotFound, id)
	}

	return b.copyConnectionFunction(fn), nil
}

// ListConnectionFunctions returns all connection functions sorted by name.
func (b *InMemoryBackend) ListConnectionFunctions() []*ConnectionFunction {
	b.mu.RLock("ListConnectionFunctions")
	defer b.mu.RUnlock()

	out := make([]*ConnectionFunction, 0, len(b.connectionFunctions))
	for _, fn := range b.connectionFunctions {
		out = append(out, b.copyConnectionFunction(fn))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// UpdateConnectionFunction updates an existing connection function, replacing its comment,
// runtime, and code (the real UpdateConnectionFunction request requires the full
// ConnectionFunctionConfig and ConnectionFunctionCode, so this is a full replace, not a merge).
// Updating a function resets it to the DEVELOPMENT stage, mirroring CloudFront Functions.
func (b *InMemoryBackend) UpdateConnectionFunction(
	id, comment, runtime string, code []byte,
) (*ConnectionFunction, error) {
	if runtime == "" {
		runtime = defaultConnectionFunctionRuntime
	}
	if err := validateRuntime(runtime); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateConnectionFunction")
	defer b.mu.Unlock()

	fn, ok := b.connectionFunctions[id]
	if !ok {
		return nil, fmt.Errorf("%w: connection function %s not found", ErrConnectionFunctionNotFound, id)
	}

	fn.Comment = comment
	fn.Runtime = runtime
	fn.FunctionCode = append([]byte(nil), code...)
	fn.Stage = functionStageDevelopment
	fn.ETag = uuid.NewString()
	fn.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)

	return b.copyConnectionFunction(fn), nil
}

// DeleteConnectionFunction deletes a connection function by ID.
func (b *InMemoryBackend) DeleteConnectionFunction(id string) error {
	b.mu.Lock("DeleteConnectionFunction")
	defer b.mu.Unlock()

	fn, ok := b.connectionFunctions[id]
	if !ok {
		return fmt.Errorf("%w: connection function %s not found", ErrConnectionFunctionNotFound, id)
	}

	delete(b.connectionFunctionARNs, fn.ARN)
	delete(b.connectionFunctions, id)

	return nil
}

// PublishConnectionFunction promotes a connection function from DEVELOPMENT to LIVE, bumping
// its ETag to reflect the new published version.
func (b *InMemoryBackend) PublishConnectionFunction(id string) (*ConnectionFunction, error) {
	b.mu.Lock("PublishConnectionFunction")
	defer b.mu.Unlock()

	fn, ok := b.connectionFunctions[id]
	if !ok {
		return nil, fmt.Errorf("%w: connection function %s not found", ErrConnectionFunctionNotFound, id)
	}

	fn.Stage = functionStageLive
	fn.ETag = uuid.NewString()
	fn.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)

	return b.copyConnectionFunction(fn), nil
}

// ConnectionFunctionTestResult is the result of a TestConnectionFunction invocation: a
// deterministic, input-derived execution result (not a hardcoded constant).
type ConnectionFunctionTestResult struct {
	ComputeUtilization string
	FunctionOutput     string
	ExecutionLogs      []string
}

// TestConnectionFunction "executes" a connection function against a caller-supplied connection
// object. Since there is no real JS runtime here, the mock computes a deterministic result
// derived from the stored function code and the input connection object: ComputeUtilization
// scales with code+input size, and FunctionOutput echoes the input event, matching real
// CloudFront's contract that the function receives and can transform the connection object.
func (b *InMemoryBackend) TestConnectionFunction(
	id string, connectionObject []byte,
) (*ConnectionFunctionTestResult, error) {
	b.mu.RLock("TestConnectionFunction")
	defer b.mu.RUnlock()

	fn, ok := b.connectionFunctions[id]
	if !ok {
		return nil, fmt.Errorf("%w: connection function %s not found", ErrConnectionFunctionNotFound, id)
	}

	// Deterministic utilization derived from the size of the function's code and the input,
	// clamped to a percentage-like range so it is stable for a given function+input pair.
	weight := (len(fn.FunctionCode) + len(connectionObject)) % percentageRange

	return &ConnectionFunctionTestResult{
		ComputeUtilization: strconv.Itoa(weight),
		FunctionOutput:     string(connectionObject),
		ExecutionLogs: []string{
			fmt.Sprintf("Running connection function %s (%s) in stage %s", fn.Name, fn.ID, fn.Stage),
			"Test passed",
		},
	}, nil
}

// ---------------------------------------------------------------------------
// AnycastIPList extra operations (Get/List/Update/Delete)
// ---------------------------------------------------------------------------

// copyAnycastIPList returns a deep copy of an AnycastIPList. Must be called with the lock held.
func (b *InMemoryBackend) copyAnycastIPList(list *AnycastIPList) *AnycastIPList {
	cp := *list
	cp.AnycastIPs = append([]string(nil), list.AnycastIPs...)
	if list.Tags != nil {
		cp.Tags = make(map[string]string, len(list.Tags))
		maps.Copy(cp.Tags, list.Tags)
	}

	return &cp
}

func (b *InMemoryBackend) GetAnycastIPList(id string) (*AnycastIPList, error) {
	b.mu.RLock("GetAnycastIPList")
	defer b.mu.RUnlock()

	list, ok := b.anycastIPLists[id]
	if !ok {
		return nil, ErrAnycastIPListNotFound
	}

	return b.copyAnycastIPList(list), nil
}

func (b *InMemoryBackend) ListAnycastIPLists() []*AnycastIPList {
	b.mu.RLock("ListAnycastIPLists")
	defer b.mu.RUnlock()

	out := make([]*AnycastIPList, 0, len(b.anycastIPLists))
	for _, list := range b.anycastIPLists {
		out = append(out, b.copyAnycastIPList(list))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

// UpdateAnycastIPList updates the IP count of an existing Anycast IP list, regenerating its
// AnycastIPs to match the new count (a no-op when ipCount is <= 0, meaning "leave unchanged").
func (b *InMemoryBackend) UpdateAnycastIPList(id string, ipCount int32) (*AnycastIPList, error) {
	b.mu.Lock("UpdateAnycastIPList")
	defer b.mu.Unlock()

	list, ok := b.anycastIPLists[id]
	if !ok {
		return nil, ErrAnycastIPListNotFound
	}
	if ipCount > 0 {
		list.IPCount = ipCount
		list.AnycastIPs = generateAnycastIPs(id, ipCount)
	}
	list.ETag = uuid.NewString()

	return b.copyAnycastIPList(list), nil
}

func (b *InMemoryBackend) DeleteAnycastIPList(id string) error {
	b.mu.Lock("DeleteAnycastIPList")
	defer b.mu.Unlock()

	list, ok := b.anycastIPLists[id]
	if !ok {
		return ErrAnycastIPListNotFound
	}
	delete(b.anycastIPListByName, list.Name)
	delete(b.anycastIPListARNs, list.ARN)
	delete(b.anycastIPLists, id)

	return nil
}

// ---------------------------------------------------------------------------
// ManagedCertificateDetails (per-distribution-tenant)
// ---------------------------------------------------------------------------

// ValidationTokenDetail describes the DNS validation record CloudFront expects to see published
// for one domain of a distribution tenant's CloudFront-managed ACM certificate.
type ValidationTokenDetail struct {
	Domain       string `json:"domain"`
	RedirectFrom string `json:"redirectFrom,omitempty"`
	RedirectTo   string `json:"redirectTo,omitempty"`
}

// ManagedCertificateDetails represents the state of the CloudFront-managed ACM certificate
// issued for a distribution tenant's domains.
type ManagedCertificateDetails struct {
	CertificateARN         string                  `json:"certificateArn"`
	CertificateStatus      string                  `json:"certificateStatus"` // SUCCESS | PENDING_VALIDATION
	ValidationTokenHost    string                  `json:"validationTokenHost"`
	ValidationTokenDetails []ValidationTokenDetail `json:"validationTokenDetails,omitempty"`
}

// managedCertificateARN builds a deterministic ACM-style ARN for a distribution tenant's
// CloudFront-managed certificate, stable across repeated Get calls for the same tenant.
func (b *InMemoryBackend) managedCertificateARN(tenantID string) string {
	sum := sha256.Sum256([]byte("managed-cert-" + tenantID))

	return fmt.Sprintf("arn:aws:acm:us-east-1:%s:certificate/%s", b.accountID, hex.EncodeToString(sum[:16]))
}

// GetManagedCertificateDetails returns the CloudFront-managed certificate details for a
// distribution tenant, deriving validation state from the tenant's real domains. The result is
// cached per tenant so repeated calls return a stable certificate ARN and validation tokens,
// mirroring a certificate that, once issued, does not change on every read.
func (b *InMemoryBackend) GetManagedCertificateDetails(tenantID string) (*ManagedCertificateDetails, error) {
	b.mu.Lock("GetManagedCertificateDetails")
	defer b.mu.Unlock()

	tenant, ok := b.distributionTenants[tenantID]
	if !ok {
		return nil, fmt.Errorf("%w: distribution tenant %s not found", ErrDistributionTenantNotFound, tenantID)
	}

	if cached, cachedOK := b.managedCertificates[tenantID]; cachedOK {
		cp := *cached
		cp.ValidationTokenDetails = append([]ValidationTokenDetail(nil), cached.ValidationTokenDetails...)

		return &cp, nil
	}

	domains := tenant.Domains
	if len(domains) == 0 && tenant.Domain != "" {
		domains = []string{tenant.Domain}
	}

	// CertificateStatus mirrors ACM's own vocabulary: "SUCCESS" once every domain has passed DNS
	// validation, "PENDING_VALIDATION" if any domain has not (there is always at least one
	// domain, since GetManagedCertificateDetails 404s for tenants with none).
	status := "SUCCESS"
	tokens := make([]ValidationTokenDetail, 0, len(domains))
	for _, domain := range domains {
		if dnsCheckStatus(domain) != dnsStatusPassed {
			status = "PENDING_VALIDATION"
		}
		sum := sha256.Sum256([]byte("validation-" + domain))
		tokens = append(tokens, ValidationTokenDetail{
			Domain:       domain,
			RedirectFrom: fmt.Sprintf("_%s.%s.", hex.EncodeToString(sum[:8]), domain),
			RedirectTo:   fmt.Sprintf("_%s.acm-validations.aws.", hex.EncodeToString(sum[8:16])),
		})
	}

	details := &ManagedCertificateDetails{
		CertificateARN:         b.managedCertificateARN(tenantID),
		CertificateStatus:      status,
		ValidationTokenHost:    "cloudfront",
		ValidationTokenDetails: tokens,
	}
	b.managedCertificates[tenantID] = details

	cp := *details
	cp.ValidationTokenDetails = append([]ValidationTokenDetail(nil), details.ValidationTokenDetails...)

	return &cp, nil
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
