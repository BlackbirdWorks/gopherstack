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

// CreateWorkgroup creates a new Redshift Serverless workgroup.
func (b *InMemoryBackend) CreateWorkgroup(
	workgroupName, namespaceName string,
	baseCapacity int,
	subnetIDs, securityGroupIDs []string,
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

	if _, ok := b.slNamespaces.Get(namespaceName); !ok {
		return nil, fmt.Errorf("%w: namespace %q not found", ErrNamespaceNotFound, namespaceName)
	}

	id := randomHex(slIDHexBytes)
	wgArn := arn.Build("redshift-serverless", b.region, b.accountID, "workgroup/"+id)

	if baseCapacity <= 0 {
		baseCapacity = slDefaultBaseCapacity
	}

	endpointAddr := fmt.Sprintf(
		"%s.%s.%s.redshift-serverless.amazonaws.com",
		workgroupName, randomHex(slEndpointHexBytes), b.region,
	)

	subnetsCopy := make([]string, len(subnetIDs))
	copy(subnetsCopy, subnetIDs)

	sgCopy := make([]string, len(securityGroupIDs))
	copy(sgCopy, securityGroupIDs)

	wg := &Workgroup{
		CreationDate:  time.Now(),
		WorkgroupArn:  wgArn,
		WorkgroupID:   id,
		WorkgroupName: workgroupName,
		NamespaceName: namespaceName,
		Status:        slStatusAvailable,
		BaseCapacity:  baseCapacity,
		Endpoint: WorkgroupEndpoint{
			Address: endpointAddr,
			Port:    slServerlessPort,
		},
		SubnetIDs:        subnetsCopy,
		SecurityGroupIDs: sgCopy,
	}
	b.slWorkgroups.Put(wg)
	b.slWorkgroupIdx.insert(workgroupName)

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

// ListWorkgroups returns all workgroups with pagination.
//
//nolint:dupl // pagination pattern is structurally identical across serverless resource types
func (b *InMemoryBackend) ListWorkgroups(maxResults int, nextToken string) ([]*Workgroup, string) {
	b.mu.RLock("ListWorkgroups")
	defer b.mu.RUnlock()

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

	startIdx := 0
	if nextToken != "" {
		if n, err := strconv.Atoi(nextToken); err == nil {
			startIdx = n
		}
	}

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
func (b *InMemoryBackend) UpdateWorkgroup(
	workgroupName string,
	baseCapacity int,
	subnetIDs, securityGroupIDs []string,
) (*Workgroup, error) {
	b.mu.Lock("UpdateWorkgroup")
	defer b.mu.Unlock()

	wg, ok := b.slWorkgroups.Get(workgroupName)
	if !ok {
		return nil, fmt.Errorf("%w: workgroup %q not found", ErrWorkgroupNotFound, workgroupName)
	}

	if baseCapacity > 0 {
		wg.BaseCapacity = baseCapacity
	}

	if subnetIDs != nil {
		cp := make([]string, len(subnetIDs))
		copy(cp, subnetIDs)
		wg.SubnetIDs = cp
	}

	if securityGroupIDs != nil {
		cp := make([]string, len(securityGroupIDs))
		copy(cp, securityGroupIDs)
		wg.SecurityGroupIDs = cp
	}

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
	cp.SubnetIDs = make([]string, len(wg.SubnetIDs))
	copy(cp.SubnetIDs, wg.SubnetIDs)
	cp.SecurityGroupIDs = make([]string, len(wg.SecurityGroupIDs))
	copy(cp.SecurityGroupIDs, wg.SecurityGroupIDs)

	return &cp
}

// ---------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------

// GetCredentials returns temporary credentials for a serverless workgroup.
// Returns (dbUser, dbPassword, expiry, error).
func (b *InMemoryBackend) GetCredentials(
	workgroupName, dbName string,
) (string, string, string, error) {
	b.mu.RLock("GetCredentials")
	defer b.mu.RUnlock()

	wg, ok := b.slWorkgroups.Get(workgroupName)
	if !ok {
		return "", "", "", fmt.Errorf(
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

	expiry := time.Now().Add(slCredExpiryMinutes * time.Minute).Format(time.RFC3339)
	dbUser := "IAMR:" + resolvedDB + ":" + strings.ToUpper(randomHex(slCredTokenHexBytes))

	return dbUser,
		randomHex(slCredSecretHexBytes),
		expiry,
		nil
}
