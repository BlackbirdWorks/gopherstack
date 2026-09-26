package opensearch

import (
	"fmt"
	"sort"
	"time"
)

// ServerlessVpcEndpoint represents an OpenSearch Serverless-managed
// interface VPC endpoint (opensearchserverless@v1.34.4 types.VpcEndpointDetail,
// types.go:1038-1070). This is a distinct resource from the classic-domain
// VpcEndpoint (vpc_endpoints.go): the classic model has no Name/CreatedDate
// fields (real types.VpcEndpoint, opensearch@v1.75.4, has neither), which
// the real AOSS VpcEndpointDetail requires -- so AOSS gets its own store
// rather than overloading the classic one. BatchGetVpcEndpoint
// (handler_serverless_account.go) reads both, preferring this one.
type ServerlessVpcEndpoint struct {
	FailureCode      string   `json:"failureCode,omitempty"`
	FailureMessage   string   `json:"failureMessage,omitempty"`
	ID               string   `json:"id"`
	Name             string   `json:"name"`
	Status           string   `json:"status"`
	VpcID            string   `json:"vpcId,omitempty"`
	SubnetIDs        []string `json:"subnetIds,omitempty"`
	SecurityGroupIDs []string `json:"securityGroupIds,omitempty"`
	CreatedDate      float64  `json:"createdDate"`
	LastModifiedDate float64  `json:"lastModifiedDate"`
}

func slVpcEndpointKeyFn(v *ServerlessVpcEndpoint) string { return v.ID }

// CreateServerlessVpcEndpoint creates a new AOSS-managed interface VPC endpoint.
func (b *InMemoryBackend) CreateServerlessVpcEndpoint(
	name, vpcID string, subnetIDs, securityGroupIDs []string,
) (*ServerlessVpcEndpoint, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	if len(subnetIDs) == 0 {
		return nil, fmt.Errorf("%w: SubnetIds is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateServerlessVpcEndpoint")
	defer b.mu.Unlock()

	for _, ep := range b.slVpcEndpoints.All() {
		if ep.Name == name {
			return nil, fmt.Errorf("%w: VPC endpoint %s already exists", ErrApplicationAlreadyExists, name)
		}
	}

	b.slVpcEndpointCounter++
	id := fmt.Sprintf("vpce-aoss-%d", b.slVpcEndpointCounter)
	now := float64(time.Now().Unix())

	ep := &ServerlessVpcEndpoint{
		ID:               id,
		Name:             name,
		VpcID:            vpcID,
		SubnetIDs:        subnetIDs,
		SecurityGroupIDs: securityGroupIDs,
		Status:           pkgStateActive,
		CreatedDate:      now,
		LastModifiedDate: now,
	}
	b.slVpcEndpoints.Put(ep)

	cp := *ep

	return &cp, nil
}

// applyServerlessVpcEndpointSetDelta returns current with remove entries
// dropped and add entries applied, deduplicated and sorted for a
// deterministic wire order.
func applyServerlessVpcEndpointSetDelta(current, add, remove []string) []string {
	set := make(map[string]bool, len(current)+len(add))
	for _, v := range current {
		set[v] = true
	}

	for _, v := range remove {
		delete(set, v)
	}

	for _, v := range add {
		set[v] = true
	}

	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}

	sort.Strings(out)

	return out
}

// UpdateServerlessVpcEndpoint applies subnet/security-group additions and
// removals to an existing VPC endpoint.
func (b *InMemoryBackend) UpdateServerlessVpcEndpoint(
	id string,
	addSubnetIDs, removeSubnetIDs, addSecurityGroupIDs, removeSecurityGroupIDs []string,
) (*ServerlessVpcEndpoint, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: Id is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateServerlessVpcEndpoint")
	defer b.mu.Unlock()

	ep, ok := b.slVpcEndpoints.Get(id)
	if !ok {
		// UpdateVpcEndpoint's declared exceptions (deserializers.go
		// awsAwsjson10_deserializeOpErrorUpdateVpcEndpoint) are ConflictException/
		// InternalServerException/ValidationException only -- no
		// ResourceNotFoundException -- so an unknown Id surfaces as
		// ValidationException, not a 404.
		return nil, fmt.Errorf("%w: VPC endpoint %s not found", ErrInvalidParameter, id)
	}

	ep.SubnetIDs = applyServerlessVpcEndpointSetDelta(ep.SubnetIDs, addSubnetIDs, removeSubnetIDs)
	ep.SecurityGroupIDs = applyServerlessVpcEndpointSetDelta(
		ep.SecurityGroupIDs, addSecurityGroupIDs, removeSecurityGroupIDs,
	)
	ep.LastModifiedDate = float64(time.Now().Unix())

	cp := *ep

	return &cp, nil
}

// DeleteServerlessVpcEndpoint removes a VPC endpoint by ID.
func (b *InMemoryBackend) DeleteServerlessVpcEndpoint(id string) (*ServerlessVpcEndpoint, error) {
	b.mu.Lock("DeleteServerlessVpcEndpoint")
	defer b.mu.Unlock()

	ep, ok := b.slVpcEndpoints.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: VPC endpoint %s not found", ErrApplicationNotFound, id)
	}

	cp := *ep
	cp.Status = statusDeleting
	b.slVpcEndpoints.Delete(id)

	return &cp, nil
}

// ListServerlessVpcEndpoints returns every AOSS-native VPC endpoint,
// optionally filtered by status, sorted by ID for deterministic output.
func (b *InMemoryBackend) ListServerlessVpcEndpoints(statusFilter string) []*ServerlessVpcEndpoint {
	b.mu.RLock("ListServerlessVpcEndpoints")
	defer b.mu.RUnlock()

	out := make([]*ServerlessVpcEndpoint, 0, b.slVpcEndpoints.Len())

	for _, ep := range b.slVpcEndpoints.All() {
		if statusFilter != "" && ep.Status != statusFilter {
			continue
		}

		cp := *ep
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}
