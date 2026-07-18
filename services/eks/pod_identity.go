package eks

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreatePodIdentityAssociation creates a new pod identity association in a cluster.
func (b *InMemoryBackend) CreatePodIdentityAssociation(
	clusterName, namespace, serviceAccount, roleARN string,
	kv map[string]string,
) (*PodIdentityAssociation, error) {
	b.mu.Lock("CreatePodIdentityAssociation")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	assocID := uuid.NewString()
	assocARN := arn.Build("eks", b.region, b.accountID, "podidentityassociation/"+clusterName+"/"+assocID)

	t := tags.New("eks.podidentity." + clusterName + "." + assocID + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	assoc := &PodIdentityAssociation{
		ClusterName:    clusterName,
		AssociationID:  assocID,
		ARN:            assocARN,
		Namespace:      namespace,
		ServiceAccount: serviceAccount,
		RoleARN:        roleARN,
		CreatedAt:      time.Now().UTC(),
		Tags:           t,
	}
	b.podIdentityAssociations.Put(assoc)
	cp := *assoc

	return &cp, nil
}

// DeletePodIdentityAssociation removes a pod identity association from a cluster.
func (b *InMemoryBackend) DeletePodIdentityAssociation(
	clusterName, associationID string,
) (*PodIdentityAssociation, error) {
	b.mu.Lock("DeletePodIdentityAssociation")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	assoc, ok := b.podIdentityAssociations.Get(podIdentityAssociationKey(clusterName, associationID))
	if !ok {
		return nil, fmt.Errorf(
			"%w: pod identity association %s not found in cluster %s",
			ErrNotFound,
			associationID,
			clusterName,
		)
	}

	cp := *assoc

	if assoc.Tags != nil {
		assoc.Tags.Close()
	}

	b.podIdentityAssociations.Delete(podIdentityAssociationKey(clusterName, associationID))

	return &cp, nil
}

// DescribePodIdentityAssociation returns a pod identity association by ID.
func (b *InMemoryBackend) DescribePodIdentityAssociation(
	clusterName, associationID string,
) (*PodIdentityAssociation, error) {
	b.mu.RLock("DescribePodIdentityAssociation")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	assoc, ok := b.podIdentityAssociations.Get(podIdentityAssociationKey(clusterName, associationID))
	if !ok {
		return nil, fmt.Errorf(
			"%w: pod identity association %s not found in cluster %s",
			ErrNotFound,
			associationID,
			clusterName,
		)
	}

	cp := *assoc

	return &cp, nil
}

// ListPodIdentityAssociations returns all pod identity association summaries for a cluster.
func (b *InMemoryBackend) ListPodIdentityAssociations(clusterName string) ([]*PodIdentityAssociation, error) {
	b.mu.RLock("ListPodIdentityAssociations")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	items := b.podIdentityAssociationsByCluster.Get(clusterName)
	list := make([]*PodIdentityAssociation, len(items))

	for i, a := range items {
		cp := *a
		list[i] = &cp
	}

	sort.Slice(list, func(i, j int) bool { return list[i].AssociationID < list[j].AssociationID })

	return list, nil
}

// UpdatePodIdentityAssociation updates a pod identity association.
func (b *InMemoryBackend) UpdatePodIdentityAssociation(
	clusterName, associationID, roleARN string,
) (*PodIdentityAssociation, error) {
	b.mu.Lock("UpdatePodIdentityAssociation")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	assoc, ok := b.podIdentityAssociations.Get(podIdentityAssociationKey(clusterName, associationID))
	if !ok {
		return nil, fmt.Errorf(
			"%w: pod identity association %s not found in cluster %s",
			ErrNotFound,
			associationID,
			clusterName,
		)
	}

	if roleARN != "" {
		assoc.RoleARN = roleARN
	}

	cp := *assoc

	return &cp, nil
}

// AddPodIdentityAssociationInternal inserts a pre-built pod identity association.
// Intended only for test seeding.
func (b *InMemoryBackend) AddPodIdentityAssociationInternal(a *PodIdentityAssociation) {
	b.mu.Lock("AddPodIdentityAssociationInternal")
	defer b.mu.Unlock()

	if a.Tags == nil {
		a.Tags = tags.New("eks.podidentity." + a.ClusterName + "." + a.AssociationID + ".tags")
	}

	b.podIdentityAssociations.Put(a)
}

// ListAllPodIdentityAssociations returns all pod identity associations across all clusters.
func (b *InMemoryBackend) ListAllPodIdentityAssociations() []*PodIdentityAssociation {
	b.mu.RLock("ListAllPodIdentityAssociations")
	defer b.mu.RUnlock()

	items := b.podIdentityAssociations.All()
	list := make([]*PodIdentityAssociation, 0, len(items))

	for _, a := range items {
		cp := *a
		list = append(list, &cp)
	}

	return list
}
