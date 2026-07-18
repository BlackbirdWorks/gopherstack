package eks

import (
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateAccessEntry creates an access entry that grants a principal access to a cluster.
func (b *InMemoryBackend) CreateAccessEntry(
	clusterName, principalARN, entryType, username string,
	kubernetesGroups []string,
	kv map[string]string,
) (*AccessEntry, error) {
	b.mu.Lock("CreateAccessEntry")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if _, ok := b.accessEntries.Get(accessEntryKey(clusterName, principalARN)); ok {
		return nil, fmt.Errorf(
			"%w: access entry for %s already exists in cluster %s",
			ErrAlreadyExists,
			principalARN,
			clusterName,
		)
	}

	entryARN := arn.Build(
		"eks",
		b.region,
		b.accountID,
		"access-entry/"+clusterName+"/"+stableID(clusterName+"/"+principalARN),
	)

	if entryType == "" {
		entryType = "STANDARD"
	}

	t := tags.New("eks.access-entry." + clusterName + "." + stableID(principalARN) + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	entry := &AccessEntry{
		PrincipalARN:     principalARN,
		ClusterName:      clusterName,
		ARN:              entryARN,
		Type:             entryType,
		Username:         username,
		KubernetesGroups: cloneStrings(kubernetesGroups),
		CreatedAt:        time.Now().UTC(),
		Tags:             t,
	}
	b.accessEntries.Put(entry)
	cp := *entry

	return &cp, nil
}

// DeleteAccessEntry removes an access entry from a cluster.
func (b *InMemoryBackend) DeleteAccessEntry(clusterName, principalARN string) error {
	b.mu.Lock("DeleteAccessEntry")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	entry, ok := b.accessEntries.Get(accessEntryKey(clusterName, principalARN))
	if !ok {
		return fmt.Errorf("%w: access entry for %s not found in cluster %s", ErrNotFound, principalARN, clusterName)
	}

	if entry.Tags != nil {
		entry.Tags.Close()
	}

	b.accessEntries.Delete(accessEntryKey(clusterName, principalARN))
	delete(b.accessPolicies[clusterName], principalARN)

	return nil
}

// DescribeAccessEntry returns an access entry by principal ARN.
func (b *InMemoryBackend) DescribeAccessEntry(clusterName, principalARN string) (*AccessEntry, error) {
	b.mu.RLock("DescribeAccessEntry")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	entry, ok := b.accessEntries.Get(accessEntryKey(clusterName, principalARN))
	if !ok {
		return nil, fmt.Errorf(
			"%w: access entry for %s not found in cluster %s",
			ErrNotFound,
			principalARN,
			clusterName,
		)
	}

	cp := *entry

	return &cp, nil
}

// ListAccessEntries returns all principal ARNs for access entries in a cluster.
func (b *InMemoryBackend) ListAccessEntries(clusterName string) ([]string, error) {
	b.mu.RLock("ListAccessEntries")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	items := b.accessEntriesByCluster.Get(clusterName)
	arns := make([]string, len(items))

	for i, e := range items {
		arns[i] = e.PrincipalARN
	}

	slices.Sort(arns)

	return arns, nil
}

// AccessEntryUpdate holds the mutable fields for UpdateAccessEntry.
type AccessEntryUpdate struct {
	Username         string
	KubernetesGroups []string
}

// UpdateAccessEntry updates an access entry's username and kubernetes groups.
func (b *InMemoryBackend) UpdateAccessEntry(
	clusterName, principalARN string,
	upd AccessEntryUpdate,
) (*AccessEntry, error) {
	b.mu.Lock("UpdateAccessEntry")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	entry, ok := b.accessEntries.Get(accessEntryKey(clusterName, principalARN))
	if !ok {
		return nil, fmt.Errorf(
			"%w: access entry for %s not found in cluster %s",
			ErrNotFound,
			principalARN,
			clusterName,
		)
	}

	if upd.Username != "" {
		entry.Username = upd.Username
	}

	if upd.KubernetesGroups != nil {
		entry.KubernetesGroups = cloneStrings(upd.KubernetesGroups)
	}

	cp := *entry

	return &cp, nil
}

// ListAccessPolicies returns a static list of AWS-managed EKS access policies.
func (b *InMemoryBackend) ListAccessPolicies() []map[string]string {
	return []map[string]string{
		{
			keyPolicyArn: "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy",
			keyName:      "AmazonEKSClusterAdminPolicy",
		},
		{keyPolicyArn: "arn:aws:eks::aws:cluster-access-policy/AmazonEKSAdminPolicy", keyName: "AmazonEKSAdminPolicy"},
		{keyPolicyArn: "arn:aws:eks::aws:cluster-access-policy/AmazonEKSEditPolicy", keyName: "AmazonEKSEditPolicy"},
		{keyPolicyArn: "arn:aws:eks::aws:cluster-access-policy/AmazonEKSViewPolicy", keyName: "AmazonEKSViewPolicy"},
		{
			keyPolicyArn: "arn:aws:eks::aws:cluster-access-policy/AmazonEKSAdminViewPolicy",
			keyName:      "AmazonEKSAdminViewPolicy",
		},
	}
}

// AssociateAccessPolicy associates an access policy with an access entry.
// If the principal already has an association for the same policyARN, it is replaced.
func (b *InMemoryBackend) AssociateAccessPolicy(
	clusterName, principalARN, policyARN string,
	accessScope map[string]any,
) (*AccessPolicyAssociation, error) {
	b.mu.Lock("AssociateAccessPolicy")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if _, ok := b.accessEntries.Get(accessEntryKey(clusterName, principalARN)); !ok {
		return nil, fmt.Errorf(
			"%w: access entry for %s not found in cluster %s",
			ErrNotFound,
			principalARN,
			clusterName,
		)
	}

	if b.accessPolicies[clusterName] == nil {
		b.accessPolicies[clusterName] = make(map[string][]*AccessPolicyAssociation)
	}

	assoc := &AccessPolicyAssociation{
		PolicyARN:    policyARN,
		ClusterName:  clusterName,
		PrincipalARN: principalARN,
		AccessScope:  accessScope,
		AssociatedAt: time.Now().UTC(),
	}

	existing := b.accessPolicies[clusterName][principalARN]
	replaced := false

	for i, a := range existing {
		if a.PolicyARN == policyARN {
			existing[i] = assoc
			replaced = true

			break
		}
	}

	if !replaced {
		b.accessPolicies[clusterName][principalARN] = append(existing, assoc)
	}

	cp := *assoc

	return &cp, nil
}

// ListAssociatedAccessPolicies returns all access policies associated with an entry.
func (b *InMemoryBackend) ListAssociatedAccessPolicies(
	clusterName, principalARN string,
) ([]*AccessPolicyAssociation, error) {
	b.mu.RLock("ListAssociatedAccessPolicies")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if _, ok := b.accessEntries.Get(accessEntryKey(clusterName, principalARN)); !ok {
		return nil, fmt.Errorf(
			"%w: access entry for %s not found in cluster %s",
			ErrNotFound,
			principalARN,
			clusterName,
		)
	}

	policies := b.accessPolicies[clusterName][principalARN]
	result := make([]*AccessPolicyAssociation, len(policies))

	for i, p := range policies {
		cp := *p
		result[i] = &cp
	}

	return result, nil
}

// DisassociateAccessPolicy removes an access policy from an access entry.
func (b *InMemoryBackend) DisassociateAccessPolicy(clusterName, principalARN, policyARN string) error {
	b.mu.Lock("DisassociateAccessPolicy")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if _, ok := b.accessEntries.Get(accessEntryKey(clusterName, principalARN)); !ok {
		return fmt.Errorf("%w: access entry for %s not found in cluster %s", ErrNotFound, principalARN, clusterName)
	}

	policies := b.accessPolicies[clusterName][principalARN]
	found := false

	for i, p := range policies {
		if p.PolicyARN == policyARN {
			b.accessPolicies[clusterName][principalARN] = append(policies[:i], policies[i+1:]...)
			found = true

			break
		}
	}

	if !found {
		return fmt.Errorf(
			"%w: policy %s not found for %s in cluster %s",
			ErrNotFound,
			policyARN,
			principalARN,
			clusterName,
		)
	}

	return nil
}

// AddAccessEntryInternal inserts a pre-built access entry into the backend.
// Intended only for test seeding.
func (b *InMemoryBackend) AddAccessEntryInternal(e *AccessEntry) {
	b.mu.Lock("AddAccessEntryInternal")
	defer b.mu.Unlock()

	if e.Tags == nil {
		e.Tags = tags.New("eks.access-entry." + e.ClusterName + "." + stableID(e.PrincipalARN) + ".tags")
	}

	b.accessEntries.Put(e)
}

// ListAllAccessEntries returns all access entries across all clusters.
func (b *InMemoryBackend) ListAllAccessEntries() []*AccessEntry {
	b.mu.RLock("ListAllAccessEntries")
	defer b.mu.RUnlock()

	items := b.accessEntries.All()
	list := make([]*AccessEntry, 0, len(items))

	for _, e := range items {
		cp := *e
		list = append(list, &cp)
	}

	return list
}
