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
func (b *InMemoryBackend) CreateNamespace(p CreateNamespaceParams) (*Namespace, error) {
	b.mu.Lock("CreateNamespace")
	defer b.mu.Unlock()

	if _, ok := b.slNamespaces.Get(p.NamespaceName); ok {
		return nil, fmt.Errorf(
			"%w: namespace %q already exists",
			ErrNamespaceAlreadyExists,
			p.NamespaceName,
		)
	}

	id := randomHex(slIDHexBytes)
	nsArn := arn.Build("redshift-serverless", b.region, b.accountID, "namespace/"+id)

	rolesCopy := make([]string, len(p.IamRoles))
	copy(rolesCopy, p.IamRoles)

	exportsCopy := make([]string, len(p.LogExports))
	copy(exportsCopy, p.LogExports)

	ns := &Namespace{
		CreationDate:      time.Now(),
		NamespaceArn:      nsArn,
		NamespaceID:       id,
		NamespaceName:     p.NamespaceName,
		AdminUsername:     p.AdminUsername,
		DBName:            p.DBName,
		DefaultIamRoleArn: p.DefaultIamRoleArn,
		KmsKeyID:          p.KmsKeyID,
		Status:            slStatusAvailable,
		IamRoles:          rolesCopy,
		LogExports:        exportsCopy,
	}

	if p.ManageAdminPassword {
		ns.AdminPasswordSecretKmsKeyID = p.AdminPasswordSecretKmsKeyID
		ns.AdminPasswordSecretArn = arn.Build(
			"secretsmanager", b.region, b.accountID,
			"secret:redshift!"+p.NamespaceName+"-"+randomHex(slSecretHexBytes),
		)
	}

	b.slNamespaces.Put(ns)
	b.slNamespaceIdx.insert(p.NamespaceName)

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
func (b *InMemoryBackend) UpdateNamespace(namespaceName string, p UpdateNamespaceParams) (*Namespace, error) {
	b.mu.Lock("UpdateNamespace")
	defer b.mu.Unlock()

	ns, ok := b.slNamespaces.Get(namespaceName)
	if !ok {
		return nil, fmt.Errorf("%w: namespace %q not found", ErrNamespaceNotFound, namespaceName)
	}

	if p.AdminUsername != "" {
		ns.AdminUsername = p.AdminUsername
	}

	if p.DBName != "" {
		ns.DBName = p.DBName
	}

	if p.KmsKeyID != "" {
		ns.KmsKeyID = p.KmsKeyID
	}

	if p.DefaultIamRoleArn != "" {
		ns.DefaultIamRoleArn = p.DefaultIamRoleArn
	}

	if p.IamRoles != nil {
		cp := make([]string, len(p.IamRoles))
		copy(cp, p.IamRoles)
		ns.IamRoles = cp
	}

	if p.LogExports != nil {
		cp := make([]string, len(p.LogExports))
		copy(cp, p.LogExports)
		ns.LogExports = cp
	}

	if p.ManageAdminPassword != nil {
		b.applyManageAdminPassword(ns, namespaceName, p, *p.ManageAdminPassword)
	}

	return cloneNamespace(ns), nil
}

// applyManageAdminPassword updates a namespace's Secrets-Manager-backed admin
// password fields for UpdateNamespace's ManageAdminPassword flag, split out of
// UpdateNamespace to keep that function's nesting flat.
func (b *InMemoryBackend) applyManageAdminPassword(
	ns *Namespace,
	namespaceName string,
	p UpdateNamespaceParams,
	manage bool,
) {
	if !manage {
		ns.AdminPasswordSecretArn = ""
		ns.AdminPasswordSecretKmsKeyID = ""

		return
	}

	if p.AdminPasswordSecretKmsKeyID != "" {
		ns.AdminPasswordSecretKmsKeyID = p.AdminPasswordSecretKmsKeyID
	}

	if ns.AdminPasswordSecretArn == "" {
		ns.AdminPasswordSecretArn = arn.Build(
			"secretsmanager", b.region, b.accountID,
			"secret:redshift!"+namespaceName+"-"+randomHex(slSecretHexBytes),
		)
	}
}

// DeleteNamespace deletes a Redshift Serverless namespace. If finalSnapshotName
// is non-empty, a final snapshot is created first (real DeleteNamespaceInput
// carries FinalSnapshotName/FinalSnapshotRetentionPeriod for exactly this).
func (b *InMemoryBackend) DeleteNamespace(
	namespaceName, finalSnapshotName string,
	finalSnapshotRetentionPeriod int,
) (*Namespace, error) {
	b.mu.Lock("DeleteNamespace")
	defer b.mu.Unlock()

	ns, ok := b.slNamespaces.Get(namespaceName)
	if !ok {
		return nil, fmt.Errorf("%w: namespace %q not found", ErrNamespaceNotFound, namespaceName)
	}

	if finalSnapshotName != "" {
		if _, exists := b.slSnapshots.Get(finalSnapshotName); exists {
			return nil, fmt.Errorf(
				"%w: snapshot %q already exists",
				ErrServerlessConflict,
				finalSnapshotName,
			)
		}

		snapArn := arn.Build("redshift-serverless", b.region, b.accountID, "snapshot/"+finalSnapshotName)
		snap := &ServerlessSnapshot{
			SnapshotCreateTime:      time.Now(),
			SnapshotArn:             snapArn,
			SnapshotName:            finalSnapshotName,
			NamespaceName:           namespaceName,
			NamespaceArn:            ns.NamespaceArn,
			Status:                  slStatusAvailable,
			AdminUsername:           ns.AdminUsername,
			SnapshotRetentionPeriod: finalSnapshotRetentionPeriod,
		}
		b.slSnapshots.Put(snap)
		b.slSnapshotIdx.insert(finalSnapshotName)
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
