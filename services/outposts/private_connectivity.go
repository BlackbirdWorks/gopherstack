package outposts

import (
	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	privateConnectivityEnabled = "ENABLED"
	maxSubnetsPerVPC           = 1
)

func (c *PrivateConnectivityConfig) clone() *PrivateConnectivityConfig {
	cp := *c
	cp.VPCs = make([]VpcInformation, len(c.VPCs))

	for i, v := range c.VPCs {
		v.SubnetIDs = cloneStrs(v.SubnetIDs)
		cp.VPCs[i] = v
	}

	return &cp
}

func validateVpcInformation(vpcs []VpcInformation) error {
	if len(vpcs) == 0 {
		return validationError("VpcInformationList must contain at least one entry")
	}

	for _, v := range vpcs {
		if len(v.SubnetIDs) > maxSubnetsPerVPC {
			return validationError("only one subnet per VPC is supported")
		}
	}

	return nil
}

// CreatePrivateConnectivityConfig enables private connectivity for an Outpost.
func (b *InMemoryBackend) CreatePrivateConnectivityConfig(
	idOrARN string, vpcs []VpcInformation,
) (string, *PrivateConnectivityConfig, error) {
	if err := validateVpcInformation(vpcs); err != nil {
		return "", nil, err
	}

	b.mu.Lock("CreatePrivateConnectivityConfig")
	defer b.mu.Unlock()

	o, ok := b.resolveOutpostLocked(idOrARN)
	if !ok {
		return "", nil, notFoundError(resourceOutpost, idOrARN)
	}

	if _, exists := b.privateConnectivity.Get(o.ID); exists {
		return "", nil, conflictErrorWithResource(
			ResourceTypeOutpost, o.ID, "private connectivity is already configured for outpost "+o.ID)
	}

	cfg := &PrivateConnectivityConfig{
		OutpostID: o.ID,
		RoleARN: arn.Build("iam", "", b.accountID,
			"role/aws-service-role/outposts.amazonaws.com/AWSServiceRoleForOutposts_"+o.ID),
		Status: privateConnectivityEnabled,
		VPCs:   vpcs,
	}
	b.privateConnectivity.Put(cfg)

	return o.ID, cfg.clone(), nil
}

// GetPrivateConnectivityConfig returns the private connectivity config of an Outpost.
func (b *InMemoryBackend) GetPrivateConnectivityConfig(idOrARN string) (*PrivateConnectivityConfig, error) {
	b.mu.RLock("GetPrivateConnectivityConfig")
	defer b.mu.RUnlock()

	o, ok := b.resolveOutpostLocked(idOrARN)
	if !ok {
		return nil, notFoundError(resourceOutpost, idOrARN)
	}

	cfg, ok := b.privateConnectivity.Get(o.ID)
	if !ok {
		return nil, notFoundError(resourcePrivateConnectivity, o.ID)
	}

	return cfg.clone(), nil
}
