package redshift

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---------------------------------------------------------------------------
// Workgroup CRUD
// ---------------------------------------------------------------------------

// CreateWorkgroup creates a new Redshift Serverless workgroup. tags holds
// CreateWorkgroupInput's "tags" (confirmed present on CreateWorkgroupRequest
// in service-2.json; UpdateWorkgroupRequest has no such field, so it is
// create-only and not part of WorkgroupParams, which UpdateWorkgroup shares).
func (b *InMemoryBackend) CreateWorkgroup(
	workgroupName, namespaceName string, p WorkgroupParams, tags map[string]string,
) (*Workgroup, error) {
	b.mu.Lock("CreateWorkgroup")
	defer b.mu.Unlock()

	if _, ok := b.slWorkgroups.Get(workgroupName); ok {
		return nil, fmt.Errorf(
			"%w: workgroup %q already exists",
			ErrWorkgroupAlreadyExists,
			workgroupName,
		)
	}

	ns, ok := b.slNamespaces.Get(namespaceName)
	if !ok {
		return nil, fmt.Errorf("%w: namespace %q not found", ErrNamespaceNotFound, namespaceName)
	}

	id := randomHex(slIDHexBytes)
	wgArn := arn.Build("redshift-serverless", b.region, b.accountID, "workgroup/"+id)

	baseCapacity := p.BaseCapacity
	if baseCapacity <= 0 {
		baseCapacity = slDefaultBaseCapacity
	}

	port := p.Port
	if port <= 0 {
		port = slServerlessPort
	}

	endpointAddr := fmt.Sprintf(
		"%s.%s.%s.redshift-serverless.amazonaws.com",
		workgroupName, randomHex(slEndpointHexBytes), b.region,
	)

	wg := &Workgroup{
		CreationDate:                         time.Now(),
		WorkgroupArn:                         wgArn,
		WorkgroupID:                          id,
		WorkgroupName:                        workgroupName,
		NamespaceName:                        namespaceName,
		Status:                               slStatusAvailable,
		BaseCapacity:                         baseCapacity,
		MaxCapacity:                          p.MaxCapacity,
		Port:                                 port,
		IPAddressType:                        p.IPAddressType,
		TrackName:                            p.TrackName,
		ConfigParameters:                     cloneConfigParameters(p.ConfigParameters),
		PricePerformanceTarget:               p.PricePerformanceTarget,
		EnhancedVpcRouting:                   p.EnhancedVpcRouting,
		ExtraComputeForAutomaticOptimization: p.ExtraComputeForAutomaticOptimization,
		PubliclyAccessible:                   p.PubliclyAccessible,
		Endpoint: WorkgroupEndpoint{
			Address: endpointAddr,
			Port:    port,
		},
		SubnetIDs:        cloneStrings(p.SubnetIDs),
		SecurityGroupIDs: cloneStrings(p.SecurityGroupIDs),
	}
	b.slWorkgroups.Put(wg)
	b.slWorkgroupIdx.insert(workgroupName)
	b.putServerlessTagsLocked(wgArn, tags)
	b.generateRecoveryPointLocked(ns, wg)

	return cloneWorkgroup(wg), nil
}

// GetWorkgroup returns a Redshift Serverless workgroup by name.
func (b *InMemoryBackend) GetWorkgroup(workgroupName string) (*Workgroup, error) {
	b.mu.RLock("GetWorkgroup")
	defer b.mu.RUnlock()

	wg, ok := b.slWorkgroups.Get(workgroupName)
	if !ok {
		return nil, fmt.Errorf("%w: workgroup %q not found", ErrWorkgroupNotFound, workgroupName)
	}

	return cloneWorkgroup(wg), nil
}

// ListWorkgroups returns all workgroups with pagination, optionally filtered
// by owner account.
//
// ownerAccount is honestly single-account: this backend has no cross-account
// workgroup sharing (every workgroup belongs to b.accountID), so a non-empty
// ownerAccount that doesn't match b.accountID matches nothing -- same
// rationale as GetServerlessSnapshot's ownerAccount filter.
func (b *InMemoryBackend) ListWorkgroups(ownerAccount string, maxResults int, nextToken string) ([]*Workgroup, string) {
	b.mu.RLock("ListWorkgroups")
	defer b.mu.RUnlock()

	if ownerAccount != "" && ownerAccount != b.accountID {
		return []*Workgroup{}, ""
	}

	// Iterate the pre-sorted index so results are ordered without re-sorting.
	keys := b.slWorkgroupIdx.ordered()
	list := make([]*Workgroup, 0, len(keys))

	for _, name := range keys {
		if wg, ok := b.slWorkgroups.Get(name); ok {
			list = append(list, cloneWorkgroup(wg))
		}
	}

	if maxResults <= 0 {
		maxResults = serverlessDefaultPageSize()
	}

	startIdx := decodeServerlessPageToken(nextToken)

	if startIdx >= len(list) {
		return []*Workgroup{}, ""
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

// UpdateWorkgroup updates a Redshift Serverless workgroup.
func (b *InMemoryBackend) UpdateWorkgroup(workgroupName string, p WorkgroupParams) (*Workgroup, error) {
	b.mu.Lock("UpdateWorkgroup")
	defer b.mu.Unlock()

	wg, ok := b.slWorkgroups.Get(workgroupName)
	if !ok {
		return nil, fmt.Errorf("%w: workgroup %q not found", ErrWorkgroupNotFound, workgroupName)
	}

	if p.BaseCapacity > 0 {
		wg.BaseCapacity = p.BaseCapacity
	}

	if p.MaxCapacity > 0 {
		wg.MaxCapacity = p.MaxCapacity
	}

	if p.Port > 0 {
		wg.Port = p.Port
		wg.Endpoint.Port = p.Port
	}

	if p.IPAddressType != "" {
		wg.IPAddressType = p.IPAddressType
	}

	if p.TrackName != "" {
		wg.TrackName = p.TrackName
	}

	if p.ConfigParameters != nil {
		wg.ConfigParameters = cloneConfigParameters(p.ConfigParameters)
	}

	if p.PricePerformanceTarget != nil {
		wg.PricePerformanceTarget = p.PricePerformanceTarget
	}

	if p.SubnetIDs != nil {
		wg.SubnetIDs = cloneStrings(p.SubnetIDs)
	}

	if p.SecurityGroupIDs != nil {
		wg.SecurityGroupIDs = cloneStrings(p.SecurityGroupIDs)
	}

	wg.EnhancedVpcRouting = p.EnhancedVpcRouting
	wg.ExtraComputeForAutomaticOptimization = p.ExtraComputeForAutomaticOptimization
	wg.PubliclyAccessible = p.PubliclyAccessible

	return cloneWorkgroup(wg), nil
}

// DeleteWorkgroup deletes a Redshift Serverless workgroup.
func (b *InMemoryBackend) DeleteWorkgroup(workgroupName string) (*Workgroup, error) {
	b.mu.Lock("DeleteWorkgroup")
	defer b.mu.Unlock()

	wg, ok := b.slWorkgroups.Get(workgroupName)
	if !ok {
		return nil, fmt.Errorf("%w: workgroup %q not found", ErrWorkgroupNotFound, workgroupName)
	}

	cp := cloneWorkgroup(wg)
	b.slWorkgroups.Delete(workgroupName)
	b.slWorkgroupIdx.remove(workgroupName)

	return cp, nil
}

func cloneWorkgroup(wg *Workgroup) *Workgroup {
	cp := *wg
	cp.SubnetIDs = cloneStrings(wg.SubnetIDs)
	cp.SecurityGroupIDs = cloneStrings(wg.SecurityGroupIDs)
	cp.ConfigParameters = cloneConfigParameters(wg.ConfigParameters)

	if wg.PricePerformanceTarget != nil {
		pt := *wg.PricePerformanceTarget
		cp.PricePerformanceTarget = &pt
	}

	if wg.CustomDomainCertificateExpiryTime != nil {
		t := *wg.CustomDomainCertificateExpiryTime
		cp.CustomDomainCertificateExpiryTime = &t
	}

	return &cp
}

func cloneStrings(in []string) []string {
	cp := make([]string, len(in))
	copy(cp, in)

	return cp
}

func cloneConfigParameters(in []ConfigParameter) []ConfigParameter {
	cp := make([]ConfigParameter, len(in))
	copy(cp, in)

	return cp
}

// ---------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------

// GetCredentials returns temporary credentials for a serverless workgroup.
// durationSeconds is clamped to the real API's [900, 3600] range; 0 uses the
// backend default (slCredExpiryMinutes).
func (b *InMemoryBackend) GetCredentials(
	workgroupName, dbName string,
	durationSeconds int,
) (string, string, time.Time, time.Time, error) {
	b.mu.RLock("GetCredentials")
	defer b.mu.RUnlock()

	wg, ok := b.slWorkgroups.Get(workgroupName)
	if !ok {
		return "", "", time.Time{}, time.Time{}, fmt.Errorf(
			"%w: workgroup %q not found",
			ErrWorkgroupNotFound,
			workgroupName,
		)
	}

	resolvedDB := dbName
	if resolvedDB == "" {
		ns, nsOK := b.slNamespaces.Get(wg.NamespaceName)
		if nsOK && ns.DBName != "" {
			resolvedDB = ns.DBName
		} else {
			resolvedDB = defaultDBName
		}
	}

	ttl := time.Duration(slCredExpiryMinutes) * time.Minute
	if durationSeconds > 0 {
		clamped := max(min(durationSeconds, slCredMaxDuration), slCredMinDuration)
		ttl = time.Duration(clamped) * time.Second
	}

	now := time.Now()
	expiration := now.Add(ttl)
	nextRefreshTime := now.Add(ttl / 2) //nolint:mnd // refresh at the ttl midpoint, not a magic threshold
	user := "IAMR:" + resolvedDB + ":" + strings.ToUpper(randomHex(slCredTokenHexBytes))

	return user, randomHex(slCredSecretHexBytes), expiration, nextRefreshTime, nil
}

// GetIdentityCenterAuthTokenSL mints an Identity Center authentication token
// scoped to workgroupNames. Real GetIdentityCenterAuthTokenInput requires 1-20
// valid WorkgroupNames, each needing "Identity Center integration enabled"
// (confirmed against api_op_GetIdentityCenterAuthToken.go) -- this backend
// does not model IDC integration state on workgroups at all
// (RedshiftIdcApplicationArn is accept-and-discard on Namespace, see
// CreateNamespaceParams' doc comment), so the only real check available is
// that every named workgroup actually exists, an FK validation classic
// Redshift's own same-named operation (handleGetIdentityCenterAuthToken in
// handler_idc_applications.go) does not even perform. The token itself is a
// synthetic opaque value: no real Identity Center backend exists here to mint
// one, the same honest limitation classic Redshift's sibling operation of the
// identical name already accepts (see its doc comment).
func (b *InMemoryBackend) GetIdentityCenterAuthTokenSL(workgroupNames []string) (string, time.Time, error) {
	b.mu.RLock("GetIdentityCenterAuthToken")
	defer b.mu.RUnlock()

	if len(workgroupNames) == 0 {
		return "", time.Time{}, fmt.Errorf("%w: workgroupNames is required", ErrServerlessValidation)
	}

	for _, name := range workgroupNames {
		if _, ok := b.slWorkgroups.Get(name); !ok {
			return "", time.Time{}, fmt.Errorf("%w: workgroup %q not found", ErrWorkgroupNotFound, name)
		}
	}

	expiry := time.Now().UTC().Add(slCredExpiryMinutes * time.Minute)
	token := "ict-" + randomHex(slIdcTokenHexBytes)

	return token, expiry, nil
}
