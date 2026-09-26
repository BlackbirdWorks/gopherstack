package dsql

import (
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const defaultListLimit = 100

// CreateCluster creates a new Aurora DSQL cluster. New clusters start
// CREATING and lazily transition to ACTIVE on the next read once
// clusterActivationDelay elapses -- see PARITY.md for why this is a lazy
// deadline rather than a background reconciler.
func (b *InMemoryBackend) CreateCluster(accountID, region string, in CreateClusterInput) (*Cluster, error) {
	if err := validateTags(in.Tags); err != nil {
		return nil, err
	}

	if err := validateMultiRegion(in.MultiRegion, region); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	if b.countClustersLocked(accountID, region) >= maxClustersPerAccountRegion {
		return nil, ErrClusterQuotaExceeded
	}

	identifier := newIdentifier()
	now := time.Now().UTC()

	tags := make(map[string]string, len(in.Tags))
	maps.Copy(tags, in.Tags)

	c := &Cluster{
		Identifier:                identifier,
		ARN:                       clusterARN(region, accountID, identifier),
		Endpoint:                  clusterEndpoint(identifier, region),
		AccountID:                 accountID,
		Region:                    region,
		Status:                    statusCreating,
		CreationTime:              now,
		PendingUntil:              now.Add(clusterActivationDelay),
		KmsEncryptionKey:          in.KmsEncryptionKey,
		DeletionProtectionEnabled: in.DeletionProtectionEnabled,
		MultiRegion:               in.MultiRegion.clone(),
		Tags:                      tags,
	}

	if in.Policy != "" {
		c.Policy = &ClusterPolicy{Policy: in.Policy, Version: newVersionToken()}
	}

	b.clusters.Put(c)

	return c.clone(), nil
}

func (b *InMemoryBackend) countClustersLocked(accountID, region string) int {
	n := 0

	for _, c := range b.clusters.All() {
		if c.AccountID == accountID && c.Region == region {
			n++
		}
	}

	return n
}

// GetCluster returns the current information about a cluster.
func (b *InMemoryBackend) GetCluster(identifier string) (*Cluster, error) {
	b.mu.Lock("GetCluster")
	defer b.mu.Unlock()

	c, err := b.resolveClusterLocked(identifier)
	if err != nil {
		return nil, err
	}

	return c.clone(), nil
}

// ListClusters returns clusters ordered by identifier, paginated by nextToken/maxResults.
func (b *InMemoryBackend) ListClusters(nextToken string, maxResults int) ([]*Cluster, string, error) {
	b.mu.Lock("ListClusters")
	defer b.mu.Unlock()

	all := b.clusters.All()

	live := make([]*Cluster, 0, len(all))

	for _, c := range all {
		b.advanceClusterLocked(c)

		if c.Status == statusDeleting && time.Now().After(c.PendingUntil) {
			b.clusters.Delete(c.Identifier)

			continue
		}

		live = append(live, c.clone())
	}

	sort.Slice(live, func(i, j int) bool { return live[i].Identifier < live[j].Identifier })

	p := page.New(live, nextToken, maxResults, defaultListLimit)

	return p.Data, p.Next, nil
}

// UpdateCluster updates a cluster's mutable configuration and transitions it
// through UPDATING back to ACTIVE on the next read.
func (b *InMemoryBackend) UpdateCluster(identifier string, in UpdateClusterInput) (*Cluster, error) {
	b.mu.Lock("UpdateCluster")
	defer b.mu.Unlock()

	c, err := b.resolveClusterLocked(identifier)
	if err != nil {
		return nil, err
	}

	if in.MultiRegion != nil {
		if validateErr := validateMultiRegion(in.MultiRegion, c.Region); validateErr != nil {
			return nil, validateErr
		}

		c.MultiRegion = in.MultiRegion.clone()
	}

	if in.DeletionProtectionEnabled != nil {
		c.DeletionProtectionEnabled = *in.DeletionProtectionEnabled
	}

	switch in.KmsEncryptionKey {
	case "":
	case encryptionTypeAWSOwned:
		c.KmsEncryptionKey = ""
	default:
		c.KmsEncryptionKey = in.KmsEncryptionKey
	}

	now := time.Now().UTC()
	c.Status = statusUpdating
	c.PendingUntil = now.Add(clusterActivationDelay)

	return c.clone(), nil
}

// DeleteCluster marks a cluster DELETING; it is lazily removed from the
// table clusterDeletionDelay after this call, on the next resolve. A cluster
// with deletion protection enabled cannot be deleted.
func (b *InMemoryBackend) DeleteCluster(identifier string) (*Cluster, error) {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	c, err := b.resolveClusterLocked(identifier)
	if err != nil {
		return nil, err
	}

	if c.DeletionProtectionEnabled {
		return nil, ErrDeletionProtected
	}

	now := time.Now().UTC()
	c.Status = statusDeleting
	c.PendingUntil = now.Add(clusterDeletionDelay)

	return c.clone(), nil
}

func validateMultiRegion(m *MultiRegionProperties, region string) error {
	if m == nil || m.WitnessRegion == "" || region == "" {
		return nil
	}

	if m.WitnessRegion == region {
		return ErrValidation
	}

	return nil
}
