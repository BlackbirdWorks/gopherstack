package ec2

import (
	"fmt"
)

// VerifiedAccessPolicy holds the Verified Access policy document and enabled
// status associated with either a Verified Access endpoint or group.
type VerifiedAccessPolicy struct {
	PolicyDocument string `json:"policyDocument,omitempty"`
	PolicyEnabled  bool   `json:"policyEnabled,omitempty"`
}

// VerifiedAccessLogCloudWatchLogs holds the CloudWatch Logs destination
// configuration for a Verified Access instance's logging configuration.
type VerifiedAccessLogCloudWatchLogs struct {
	LogGroup string `json:"logGroup,omitempty"`
	Enabled  bool   `json:"enabled,omitempty"`
}

// VerifiedAccessLogKinesisDataFirehose holds the Kinesis Data Firehose
// destination configuration for a Verified Access instance's logging
// configuration.
type VerifiedAccessLogKinesisDataFirehose struct {
	DeliveryStream string `json:"deliveryStream,omitempty"`
	Enabled        bool   `json:"enabled,omitempty"`
}

// VerifiedAccessLogS3 holds the Amazon S3 destination configuration for a
// Verified Access instance's logging configuration.
type VerifiedAccessLogS3 struct {
	BucketName  string `json:"bucketName,omitempty"`
	BucketOwner string `json:"bucketOwner,omitempty"`
	Prefix      string `json:"prefix,omitempty"`
	Enabled     bool   `json:"enabled,omitempty"`
}

// VerifiedAccessLogOptions is the set of logging destinations that can be
// configured for a Verified Access instance.
type VerifiedAccessLogOptions struct {
	CloudWatchLogs      *VerifiedAccessLogCloudWatchLogs      `json:"cloudWatchLogs,omitempty"`
	KinesisDataFirehose *VerifiedAccessLogKinesisDataFirehose `json:"kinesisDataFirehose,omitempty"`
	S3                  *VerifiedAccessLogS3                  `json:"s3,omitempty"`
	LogVersion          string                                `json:"logVersion,omitempty"`
	IncludeTrustContext bool                                  `json:"includeTrustContext,omitempty"`
}

// VerifiedAccessInstanceLoggingConfig is the logging configuration stored for
// a Verified Access instance.
type VerifiedAccessInstanceLoggingConfig struct {
	VerifiedAccessInstanceID string                   `json:"verifiedAccessInstanceId,omitempty"`
	AccessLogs               VerifiedAccessLogOptions `json:"accessLogs,omitzero"`
}

// VerifiedAccessEndpointTarget describes a resolved network target for a
// Verified Access endpoint.
type VerifiedAccessEndpointTarget struct {
	VerifiedAccessEndpointID              string `json:"verifiedAccessEndpointId,omitempty"`
	VerifiedAccessEndpointTargetIPAddress string `json:"verifiedAccessEndpointTargetIpAddress,omitempty"`
	VerifiedAccessEndpointTargetDNS       string `json:"verifiedAccessEndpointTargetDns,omitempty"`
}

// GetVerifiedAccessEndpointPolicy returns the stored policy for a Verified
// Access endpoint. If no policy has ever been set, the zero-value policy
// (disabled, no document) is returned, matching AWS's default state.
func (b *InMemoryBackend) GetVerifiedAccessEndpointPolicy(id string) (*VerifiedAccessPolicy, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: VerifiedAccessEndpointId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetVerifiedAccessEndpointPolicy")
	defer b.mu.RUnlock()

	if _, ok := b.verifiedAccessEndpoints.Get(id); !ok {
		return nil, fmt.Errorf("%w: %s", ErrVerifiedAccessEndpointNotFound, id)
	}

	if pol, ok := b.verifiedAccessEndpointPolicies[id]; ok {
		cp := *pol

		return &cp, nil
	}

	return &VerifiedAccessPolicy{}, nil
}

// ModifyVerifiedAccessEndpointPolicy sets the policy document/status for a
// Verified Access endpoint and returns the resulting stored policy.
func (b *InMemoryBackend) ModifyVerifiedAccessEndpointPolicy(
	id string,
	policyEnabled bool,
	policyDocument string,
) (*VerifiedAccessPolicy, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: VerifiedAccessEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVerifiedAccessEndpointPolicy")
	defer b.mu.Unlock()

	if _, ok := b.verifiedAccessEndpoints.Get(id); !ok {
		return nil, fmt.Errorf("%w: %s", ErrVerifiedAccessEndpointNotFound, id)
	}

	pol := &VerifiedAccessPolicy{PolicyEnabled: policyEnabled, PolicyDocument: policyDocument}
	b.verifiedAccessEndpointPolicies[id] = pol
	cp := *pol

	return &cp, nil
}

// GetVerifiedAccessGroupPolicy returns the stored policy for a Verified
// Access group. If no policy has ever been set, the zero-value policy
// (disabled, no document) is returned, matching AWS's default state.
func (b *InMemoryBackend) GetVerifiedAccessGroupPolicy(id string) (*VerifiedAccessPolicy, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: VerifiedAccessGroupId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetVerifiedAccessGroupPolicy")
	defer b.mu.RUnlock()

	if _, ok := b.verifiedAccessGroups.Get(id); !ok {
		return nil, fmt.Errorf("%w: %s", ErrVerifiedAccessGroupNotFound, id)
	}

	if pol, ok := b.verifiedAccessGroupPolicies[id]; ok {
		cp := *pol

		return &cp, nil
	}

	return &VerifiedAccessPolicy{}, nil
}

// ModifyVerifiedAccessGroupPolicy sets the policy document/status for a
// Verified Access group and returns the resulting stored policy.
func (b *InMemoryBackend) ModifyVerifiedAccessGroupPolicy(
	id string,
	policyEnabled bool,
	policyDocument string,
) (*VerifiedAccessPolicy, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: VerifiedAccessGroupId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVerifiedAccessGroupPolicy")
	defer b.mu.Unlock()

	if _, ok := b.verifiedAccessGroups.Get(id); !ok {
		return nil, fmt.Errorf("%w: %s", ErrVerifiedAccessGroupNotFound, id)
	}

	pol := &VerifiedAccessPolicy{PolicyEnabled: policyEnabled, PolicyDocument: policyDocument}
	b.verifiedAccessGroupPolicies[id] = pol
	cp := *pol

	return &cp, nil
}

// DescribeVerifiedAccessInstanceLoggingConfigurations returns the logging
// configuration for the given instance IDs, or for every instance when ids
// is empty. Instances without an explicit logging configuration report the
// zero-value configuration (all destinations disabled), matching AWS.
func (b *InMemoryBackend) DescribeVerifiedAccessInstanceLoggingConfigurations(
	ids []string,
) []*VerifiedAccessInstanceLoggingConfig {
	b.mu.RLock("DescribeVerifiedAccessInstanceLoggingConfigurations")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	var out []*VerifiedAccessInstanceLoggingConfig

	for _, vai := range b.verifiedAccessInstances.All() {
		instanceID := verifiedAccessInstancesKeyFn(vai)
		if len(filter) > 0 && !filter[instanceID] {
			continue
		}

		if cfg, ok := b.verifiedAccessInstanceLoggingConfigs.Get(instanceID); ok {
			cp := *cfg
			out = append(out, &cp)

			continue
		}

		out = append(out, &VerifiedAccessInstanceLoggingConfig{VerifiedAccessInstanceID: instanceID})
	}

	return out
}

// ModifyVerifiedAccessInstanceLoggingConfiguration sets the logging
// configuration for a Verified Access instance and returns the resulting
// stored configuration.
func (b *InMemoryBackend) ModifyVerifiedAccessInstanceLoggingConfiguration(
	instanceID string,
	accessLogs VerifiedAccessLogOptions,
) (*VerifiedAccessInstanceLoggingConfig, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: VerifiedAccessInstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyVerifiedAccessInstanceLoggingConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.verifiedAccessInstances.Get(instanceID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrVerifiedAccessInstanceNotFound, instanceID)
	}

	cfg := &VerifiedAccessInstanceLoggingConfig{
		VerifiedAccessInstanceID: instanceID,
		AccessLogs:               accessLogs,
	}
	b.verifiedAccessInstanceLoggingConfigs.Put(cfg)
	cp := *cfg

	return &cp, nil
}

// GetVerifiedAccessEndpointTargets returns the resolved network targets for a
// Verified Access endpoint. Only "cidr" (network-CIDR) endpoints have
// resolvable targets; since no real network-level target resolution is
// modelled, this returns the correct empty AWS shape for every endpoint.
func (b *InMemoryBackend) GetVerifiedAccessEndpointTargets(id string) ([]*VerifiedAccessEndpointTarget, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: VerifiedAccessEndpointId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetVerifiedAccessEndpointTargets")
	defer b.mu.RUnlock()

	if _, ok := b.verifiedAccessEndpoints.Get(id); !ok {
		return nil, fmt.Errorf("%w: %s", ErrVerifiedAccessEndpointNotFound, id)
	}

	return nil, nil
}

// VerifiedAccessInstanceClientConfiguration is the exported client
// configuration for a Verified Access instance.
type VerifiedAccessInstanceClientConfiguration struct {
	VerifiedAccessInstanceID string
	Region                   string
	Version                  string
}

// ExportVerifiedAccessInstanceClientConfiguration returns the generated
// client configuration for a Verified Access instance. Device trust provider
// types and OpenVPN configurations are not modelled (no device-trust-provider
// subtype or OpenVPN backing is stored), so they are correctly reported as
// empty; the user trust provider's OIDC configuration is likewise unmodelled
// and omitted.
func (b *InMemoryBackend) ExportVerifiedAccessInstanceClientConfiguration(
	instanceID string,
) (*VerifiedAccessInstanceClientConfiguration, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: VerifiedAccessInstanceId is required", ErrInvalidParameter)
	}

	b.mu.RLock("ExportVerifiedAccessInstanceClientConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.verifiedAccessInstances.Get(instanceID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrVerifiedAccessInstanceNotFound, instanceID)
	}

	return &VerifiedAccessInstanceClientConfiguration{
		VerifiedAccessInstanceID: instanceID,
		Region:                   b.Region,
		Version:                  "1",
	}, nil
}
