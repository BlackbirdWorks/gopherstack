package redshift

import (
	"fmt"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---------------------------------------------------------------------------
// Namespace CRUD
// ---------------------------------------------------------------------------

// CreateNamespace creates a new Redshift Serverless namespace.
func (b *InMemoryBackend) CreateNamespace(
	namespaceName, adminUsername, dbName, kmsKeyID string,
	iamRoles, logExports []string,
) (*Namespace, error) {
	b.mu.Lock("CreateNamespace")
	defer b.mu.Unlock()

	if _, ok := b.slNamespaces.Get(namespaceName); ok {
		return nil, fmt.Errorf(
			"%w: namespace %q already exists",
			ErrNamespaceAlreadyExists,
			namespaceName,
		)
	}

	id := randomHex(slIDHexBytes)
	nsArn := arn.Build("redshift-serverless", b.region, b.accountID, "namespace/"+id)

	rolesCopy := make([]string, len(iamRoles))
	copy(rolesCopy, iamRoles)

	exportsCopy := make([]string, len(logExports))
	copy(exportsCopy, logExports)

	ns := &Namespace{
		CreationDate:  time.Now(),
		NamespaceArn:  nsArn,
		NamespaceID:   id,
		NamespaceName: namespaceName,
		AdminUsername: adminUsername,
		DBName:        dbName,
		KmsKeyID:      kmsKeyID,
		Status:        slStatusAvailable,
		IamRoles:      rolesCopy,
		LogExports:    exportsCopy,
	}
	b.slNamespaces.Put(ns)
	b.slNamespaceIdx.insert(namespaceName)

	return cloneNamespace(ns), nil
}

// GetNamespace returns a Redshift Serverless namespace by name.
func (b *InMemoryBackend) GetNamespace(namespaceName string) (*Namespace, error) {
	b.mu.RLock("GetNamespace")
	defer b.mu.RUnlock()

	ns, ok := b.slNamespaces.Get(namespaceName)
	if !ok {
		return nil, fmt.Errorf("%w: namespace %q not found", ErrNamespaceNotFound, namespaceName)
	}

	return cloneNamespace(ns), nil
}

// ListNamespaces returns all namespaces with pagination.
//
//nolint:dupl // pagination pattern is structurally identical across serverless resource types
func (b *InMemoryBackend) ListNamespaces(maxResults int, nextToken string) ([]*Namespace, string) {
	b.mu.RLock("ListNamespaces")
	defer b.mu.RUnlock()

	// Iterate the pre-sorted index so results are ordered without re-sorting.
	keys := b.slNamespaceIdx.ordered()
	list := make([]*Namespace, 0, len(keys))

	for _, name := range keys {
		if ns, ok := b.slNamespaces.Get(name); ok {
			list = append(list, cloneNamespace(ns))
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
		return []*Namespace{}, ""
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

// UpdateNamespace updates a Redshift Serverless namespace.
func (b *InMemoryBackend) UpdateNamespace(
	namespaceName, adminUsername, dbName, kmsKeyID string,
	iamRoles, logExports []string,
) (*Namespace, error) {
	b.mu.Lock("UpdateNamespace")
	defer b.mu.Unlock()

	ns, ok := b.slNamespaces.Get(namespaceName)
	if !ok {
		return nil, fmt.Errorf("%w: namespace %q not found", ErrNamespaceNotFound, namespaceName)
	}

	if adminUsername != "" {
		ns.AdminUsername = adminUsername
	}

	if dbName != "" {
		ns.DBName = dbName
	}

	if kmsKeyID != "" {
		ns.KmsKeyID = kmsKeyID
	}

	if iamRoles != nil {
		cp := make([]string, len(iamRoles))
		copy(cp, iamRoles)
		ns.IamRoles = cp
	}

	if logExports != nil {
		cp := make([]string, len(logExports))
		copy(cp, logExports)
		ns.LogExports = cp
	}

	return cloneNamespace(ns), nil
}

// DeleteNamespace deletes a Redshift Serverless namespace.
func (b *InMemoryBackend) DeleteNamespace(namespaceName string) (*Namespace, error) {
	b.mu.Lock("DeleteNamespace")
	defer b.mu.Unlock()

	ns, ok := b.slNamespaces.Get(namespaceName)
	if !ok {
		return nil, fmt.Errorf("%w: namespace %q not found", ErrNamespaceNotFound, namespaceName)
	}

	cp := cloneNamespace(ns)
	b.slNamespaces.Delete(namespaceName)
	b.slNamespaceIdx.remove(namespaceName)

	return cp, nil
}

func cloneNamespace(ns *Namespace) *Namespace {
	cp := *ns
	cp.IamRoles = make([]string, len(ns.IamRoles))
	copy(cp.IamRoles, ns.IamRoles)
	cp.LogExports = make([]string, len(ns.LogExports))
	copy(cp.LogExports, ns.LogExports)

	return &cp
}
