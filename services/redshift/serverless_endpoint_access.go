package redshift

import (
	"fmt"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---------------------------------------------------------------------------
// Serverless VPC endpoint access (Create/Get/List/Update/DeleteEndpointAccess)
// ---------------------------------------------------------------------------

// CreateEndpointAccessSL creates a Redshift Serverless managed VPC endpoint.
// endpointName, subnetIDs and workgroupName are all required on the real
// CreateEndpointAccessRequest (confirmed against service-2.json).
func (b *InMemoryBackend) CreateEndpointAccessSL(
	endpointName, workgroupName, ownerAccount string, subnetIDs, vpcSecurityGroupIDs []string,
) (*ServerlessEndpointAccess, error) {
	if endpointName == "" {
		return nil, fmt.Errorf("%w: endpointName is required", ErrServerlessValidation)
	}

	b.mu.Lock("CreateEndpointAccessSL")
	defer b.mu.Unlock()

	if _, exists := b.slEndpointAccesses.Get(endpointName); exists {
		return nil, fmt.Errorf("%w: endpoint %q already exists", ErrEndpointAccessSLAlreadyExists, endpointName)
	}

	wg, ok := b.slWorkgroups.Get(workgroupName)
	if !ok {
		return nil, fmt.Errorf("%w: workgroup %q not found", ErrWorkgroupNotFound, workgroupName)
	}

	ep := &ServerlessEndpointAccess{
		EndpointCreateTime:  time.Now(),
		EndpointArn:         arn.Build("redshift-serverless", b.region, b.accountID, "endpoint/"+endpointName),
		EndpointName:        endpointName,
		EndpointStatus:      slStatusAvailable,
		OwnerAccount:        ownerAccount,
		WorkgroupName:       wg.WorkgroupName,
		SubnetIDs:           cloneStrings(subnetIDs),
		VpcSecurityGroupIDs: cloneStrings(vpcSecurityGroupIDs),
		Port:                slServerlessPort,
		Address: fmt.Sprintf(
			"%s.%s.%s.redshift-serverless.amazonaws.com", endpointName, randomHex(slEndpointHexBytes), b.region,
		),
	}
	b.slEndpointAccesses.Put(ep)
	b.slEndpointAccessIdx.insert(endpointName)

	return cloneEndpointAccessSL(ep), nil
}

// GetEndpointAccessSL returns a serverless VPC endpoint by name.
func (b *InMemoryBackend) GetEndpointAccessSL(endpointName string) (*ServerlessEndpointAccess, error) {
	b.mu.RLock("GetEndpointAccessSL")
	defer b.mu.RUnlock()

	ep, ok := b.slEndpointAccesses.Get(endpointName)
	if !ok {
		return nil, fmt.Errorf("%w: endpoint %q not found", ErrEndpointAccessSLNotFound, endpointName)
	}

	return cloneEndpointAccessSL(ep), nil
}

// ListEndpointAccessSL returns serverless VPC endpoints, optionally filtered
// by workgroupName/ownerAccount. ListEndpointAccessRequest's vpcId filter is
// deliberately not accepted -- this backend never derives a real vpcId for
// any endpoint (see ServerlessEndpointAccess's VpcEndpoint doc comment), so
// there is nothing honest to filter against.
//
//nolint:dupl // pagination pattern is structurally identical across serverless resource types
func (b *InMemoryBackend) ListEndpointAccessSL(
	workgroupName, ownerAccount string, maxResults int, nextToken string,
) ([]*ServerlessEndpointAccess, string) {
	b.mu.RLock("ListEndpointAccessSL")
	defer b.mu.RUnlock()

	keys := b.slEndpointAccessIdx.ordered()
	list := make([]*ServerlessEndpointAccess, 0, len(keys))

	for _, name := range keys {
		ep, ok := b.slEndpointAccesses.Get(name)
		if !ok {
			continue
		}

		if workgroupName != "" && ep.WorkgroupName != workgroupName {
			continue
		}

		if ownerAccount != "" && ep.OwnerAccount != ownerAccount {
			continue
		}

		list = append(list, cloneEndpointAccessSL(ep))
	}

	if maxResults <= 0 {
		maxResults = serverlessDefaultPageSize()
	}

	startIdx := 0
	if nextToken != "" {
		if n, err := strconv.Atoi(nextToken); err == nil {
			startIdx = n
		}
	}

	if startIdx >= len(list) {
		return []*ServerlessEndpointAccess{}, ""
	}

	end := startIdx + maxResults
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// UpdateEndpointAccessSL updates a serverless VPC endpoint. Real
// UpdateEndpointAccessInput only supports changing vpcSecurityGroupIds
// (confirmed against service-2.json) -- there is no subnetIds parameter.
func (b *InMemoryBackend) UpdateEndpointAccessSL(
	endpointName string, vpcSecurityGroupIDs []string,
) (*ServerlessEndpointAccess, error) {
	b.mu.Lock("UpdateEndpointAccessSL")
	defer b.mu.Unlock()

	ep, ok := b.slEndpointAccesses.Get(endpointName)
	if !ok {
		return nil, fmt.Errorf("%w: endpoint %q not found", ErrEndpointAccessSLNotFound, endpointName)
	}

	if vpcSecurityGroupIDs != nil {
		ep.VpcSecurityGroupIDs = cloneStrings(vpcSecurityGroupIDs)
	}

	return cloneEndpointAccessSL(ep), nil
}

// DeleteEndpointAccessSL deletes a serverless VPC endpoint and returns the
// deleted object -- DeleteEndpointAccessResponse carries an "endpoint"
// member (confirmed against service-2.json), unlike DeleteResourcePolicy/
// DeleteCustomDomainAssociation's zero-member responses.
func (b *InMemoryBackend) DeleteEndpointAccessSL(endpointName string) (*ServerlessEndpointAccess, error) {
	b.mu.Lock("DeleteEndpointAccessSL")
	defer b.mu.Unlock()

	ep, ok := b.slEndpointAccesses.Get(endpointName)
	if !ok {
		return nil, fmt.Errorf("%w: endpoint %q not found", ErrEndpointAccessSLNotFound, endpointName)
	}

	cp := cloneEndpointAccessSL(ep)
	b.slEndpointAccesses.Delete(endpointName)
	b.slEndpointAccessIdx.remove(endpointName)

	return cp, nil
}

func cloneEndpointAccessSL(ep *ServerlessEndpointAccess) *ServerlessEndpointAccess {
	cp := *ep
	cp.SubnetIDs = cloneStrings(ep.SubnetIDs)
	cp.VpcSecurityGroupIDs = cloneStrings(ep.VpcSecurityGroupIDs)

	return &cp
}

// ---------------------------------------------------------------------------
// ListManagedWorkgroups
// ---------------------------------------------------------------------------

// ListManagedWorkgroupsSL returns Glue Data Catalog-managed Redshift
// Serverless workgroups (see ManagedWorkgroupListItem's doc comment for what
// these are). This backend has no Glue Data Catalog / Lake Formation
// integration anywhere that could ever produce one, so the honest result is
// always an empty, correctly-shaped list -- not a fabricated entry.
func (b *InMemoryBackend) ListManagedWorkgroupsSL(_ string, _ int, _ string) ([]ManagedWorkgroupListItem, string) {
	return []ManagedWorkgroupListItem{}, ""
}
