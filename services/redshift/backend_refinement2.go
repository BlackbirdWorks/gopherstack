package redshift

import (
	"encoding/hex"
	"fmt"
	"time"
)

const credentialSeedLength = 8

// ClusterCredentials holds temporary cluster credentials.
type ClusterCredentials struct {
	Expiration time.Time
	DBUser     string
	DBPassword string
}

// DeletePartner removes a partner integration from the specified cluster database.
func (b *InMemoryBackend) DeletePartner(_, clusterID, databaseName, partnerName string) error {
	if clusterID == "" {
		return fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeletePartner")
	defer b.mu.Unlock()

	key := partnerKey(clusterID, databaseName, partnerName)
	if _, exists := b.partners.Get(key); !exists {
		return fmt.Errorf("%w: partner %s not found", ErrPartnerNotFound, partnerName)
	}

	b.partners.Delete(key)

	return nil
}

// DescribePartners returns partner integrations filtered by cluster, database, or partner name.
func (b *InMemoryBackend) DescribePartners(_, clusterID, databaseName, partnerName string) ([]Partner, error) {
	b.mu.RLock("DescribePartners")
	defer b.mu.RUnlock()

	var result []Partner

	for _, p := range b.partners.All() {
		if clusterID != "" && p.ClusterIdentifier != clusterID {
			continue
		}

		if databaseName != "" && p.DatabaseName != databaseName {
			continue
		}

		if partnerName != "" && p.PartnerName != partnerName {
			continue
		}

		cp := *p
		result = append(result, cp)
	}

	return result, nil
}

// UpdatePartnerStatus updates the status and status message of a partner integration.
func (b *InMemoryBackend) UpdatePartnerStatus(
	_, clusterID, databaseName, partnerName, status, statusMessage string,
) (*Partner, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdatePartnerStatus")
	defer b.mu.Unlock()

	key := partnerKey(clusterID, databaseName, partnerName)

	p, exists := b.partners.Get(key)
	if !exists {
		return nil, fmt.Errorf("%w: partner %s not found", ErrPartnerNotFound, partnerName)
	}

	if status != "" {
		p.Status = status
	}

	if statusMessage != "" {
		p.StatusMessage = statusMessage
	}

	cp := *p

	return &cp, nil
}

// DescribeDataShares returns data shares, optionally filtered by ARN.
func (b *InMemoryBackend) DescribeDataShares(dataShareArn string) ([]DataShare, error) {
	b.mu.RLock("DescribeDataShares")
	defer b.mu.RUnlock()

	if dataShareArn != "" {
		ds, exists := b.dataShares.Get(dataShareArn)
		if !exists {
			return nil, fmt.Errorf("%w: data share %s not found", ErrDataShareNotFound, dataShareArn)
		}

		return []DataShare{*cloneDataShare(ds)}, nil
	}

	result := make([]DataShare, 0, b.dataShares.Len())

	for _, ds := range b.dataShares.All() {
		result = append(result, *cloneDataShare(ds))
	}

	return result, nil
}

// DescribeDataSharesForConsumer returns data shares with a matching consumer association.
func (b *InMemoryBackend) DescribeDataSharesForConsumer(consumerArn, status string) ([]DataShare, error) {
	b.mu.RLock("DescribeDataSharesForConsumer")
	defer b.mu.RUnlock()

	var result []DataShare

	for _, ds := range b.dataShares.All() {
		for _, a := range ds.DataShareAssociations {
			if consumerArn != "" && a.ConsumerIdentifier != consumerArn {
				continue
			}

			if status != "" && a.Status != status {
				continue
			}

			result = append(result, *cloneDataShare(ds))

			break
		}
	}

	return result, nil
}

// DescribeDataSharesForProducer returns data shares matching a producer ARN.
func (b *InMemoryBackend) DescribeDataSharesForProducer(producerArn, status string) ([]DataShare, error) {
	b.mu.RLock("DescribeDataSharesForProducer")
	defer b.mu.RUnlock()

	var result []DataShare

	for _, ds := range b.dataShares.All() {
		if producerArn != "" && ds.ProducerArn != producerArn {
			continue
		}

		if status != "" {
			match := false

			for _, a := range ds.DataShareAssociations {
				if a.Status == status {
					match = true

					break
				}
			}

			if !match {
				continue
			}
		}

		result = append(result, *cloneDataShare(ds))
	}

	return result, nil
}

// DeauthorizeDataShare sets the association status to DEAUTHORIZED for the given consumer.
func (b *InMemoryBackend) DeauthorizeDataShare(dataShareArn, consumerIdentifier string) (*DataShare, error) {
	if dataShareArn == "" {
		return nil, fmt.Errorf("%w: DataShareArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeauthorizeDataShare")
	defer b.mu.Unlock()

	ds, exists := b.dataShares.Get(dataShareArn)
	if !exists {
		return nil, fmt.Errorf("%w: data share %s not found", ErrDataShareNotFound, dataShareArn)
	}

	for i, a := range ds.DataShareAssociations {
		if a.ConsumerIdentifier == consumerIdentifier {
			ds.DataShareAssociations[i].Status = "DEAUTHORIZED"
		}
	}

	return cloneDataShare(ds), nil
}

// DisassociateDataShareConsumer removes the consumer association from a data share.
func (b *InMemoryBackend) DisassociateDataShareConsumer(
	dataShareArn, consumerArn, _ string,
	_ bool,
) (*DataShare, error) {
	if dataShareArn == "" {
		return nil, fmt.Errorf("%w: DataShareArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateDataShareConsumer")
	defer b.mu.Unlock()

	ds, exists := b.dataShares.Get(dataShareArn)
	if !exists {
		return nil, fmt.Errorf("%w: data share %s not found", ErrDataShareNotFound, dataShareArn)
	}

	filtered := make([]DataShareAssociation, 0, len(ds.DataShareAssociations))

	for _, a := range ds.DataShareAssociations {
		if a.ConsumerIdentifier != consumerArn {
			filtered = append(filtered, a)
		}
	}

	ds.DataShareAssociations = filtered

	return cloneDataShare(ds), nil
}

// RejectDataShare marks all associations as REJECTED and disables public access.
func (b *InMemoryBackend) RejectDataShare(dataShareArn string) (*DataShare, error) {
	if dataShareArn == "" {
		return nil, fmt.Errorf("%w: DataShareArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("RejectDataShare")
	defer b.mu.Unlock()

	ds, exists := b.dataShares.Get(dataShareArn)
	if !exists {
		return nil, fmt.Errorf("%w: data share %s not found", ErrDataShareNotFound, dataShareArn)
	}

	ds.AllowPubliclyAccessibleConsumers = false

	for i := range ds.DataShareAssociations {
		ds.DataShareAssociations[i].Status = "REJECTED"
	}

	return cloneDataShare(ds), nil
}

// DescribeEndpointAuthorization returns endpoint authorizations filtered by cluster and account.
func (b *InMemoryBackend) DescribeEndpointAuthorization(
	clusterID, account string,
	grantee bool,
) ([]EndpointAuthorization, error) {
	b.mu.RLock("DescribeEndpointAuthorization")
	defer b.mu.RUnlock()

	var result []EndpointAuthorization

	for _, ea := range b.endpointAuths.All() {
		if clusterID != "" && ea.ClusterIdentifier != clusterID {
			continue
		}

		if account != "" {
			if grantee && ea.Grantee != account {
				continue
			}

			if !grantee && ea.Grantor != account {
				continue
			}
		}

		cp := *cloneEndpointAuth(ea)
		result = append(result, cp)
	}

	return result, nil
}

// RevokeEndpointAccess revokes endpoint authorization for a cluster and account.
func (b *InMemoryBackend) RevokeEndpointAccess(
	clusterID, account string,
	_ []string,
	force bool,
) (*EndpointAuthorization, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("RevokeEndpointAccess")
	defer b.mu.Unlock()

	key := endpointAuthKey(clusterID, account)

	ea, exists := b.endpointAuths.Get(key)
	if !exists {
		return nil, fmt.Errorf(
			"%w: endpoint authorization for cluster %s account %s not found",
			ErrEndpointAuthNotFound,
			clusterID,
			account,
		)
	}

	ea.Status = "Revoking"

	cp := cloneEndpointAuth(ea)

	if force {
		b.endpointAuths.Delete(key)
	}

	return cp, nil
}

// DescribeResize returns the active resize progress for a cluster.
func (b *InMemoryBackend) DescribeResize(clusterID string) (*ResizeProgress, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeResize")
	defer b.mu.RUnlock()

	rp, exists := b.activeResizes[clusterID]
	if !exists {
		return nil, fmt.Errorf("%w: no active resize for cluster %s", ErrResizeNotFound, clusterID)
	}

	cp := *rp
	cp.ImportTablesCompleted = make([]string, len(rp.ImportTablesCompleted))
	copy(cp.ImportTablesCompleted, rp.ImportTablesCompleted)
	cp.ImportTablesInProgress = make([]string, len(rp.ImportTablesInProgress))
	copy(cp.ImportTablesInProgress, rp.ImportTablesInProgress)
	cp.ImportTablesNotStarted = make([]string, len(rp.ImportTablesNotStarted))
	copy(cp.ImportTablesNotStarted, rp.ImportTablesNotStarted)

	return &cp, nil
}

// RevokeSnapshotAccess removes restore access for the given account from a snapshot.
func (b *InMemoryBackend) RevokeSnapshotAccess(snapshotID, accountWithRestoreAccess string) (*Snapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: SnapshotIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("RevokeSnapshotAccess")
	defer b.mu.Unlock()

	snap, exists := b.snapshots.Get(snapshotID)
	if !exists {
		return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, snapshotID)
	}

	filtered := make([]AccountWithRestoreAccess, 0, len(snap.AccountsWithRestoreAccess))
	found := false

	for _, a := range snap.AccountsWithRestoreAccess {
		if a.AccountID == accountWithRestoreAccess {
			found = true

			continue
		}

		filtered = append(filtered, a)
	}

	if !found {
		return nil, fmt.Errorf(
			"%w: account %s does not have restore access",
			ErrInvalidParameter,
			accountWithRestoreAccess,
		)
	}

	snap.AccountsWithRestoreAccess = filtered

	return cloneSnapshot(snap), nil
}

// ModifyClusterSnapshot updates the manual retention period of a snapshot.
func (b *InMemoryBackend) ModifyClusterSnapshot(snapshotID string, retentionPeriod int, _ bool) (*Snapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: SnapshotIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyClusterSnapshot")
	defer b.mu.Unlock()

	snap, exists := b.snapshots.Get(snapshotID)
	if !exists {
		return nil, fmt.Errorf("%w: snapshot %s not found", ErrSnapshotNotFound, snapshotID)
	}

	if retentionPeriod >= -1 {
		snap.ManualSnapshotRetentionPeriod = retentionPeriod
	}

	return cloneSnapshot(snap), nil
}

// GetClusterCredentials generates temporary credentials for a cluster user.
func (b *InMemoryBackend) GetClusterCredentials(
	clusterID, dbUser string,
	_ bool,
) (*ClusterCredentials, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	if dbUser == "" {
		return nil, fmt.Errorf("%w: DbUser is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetClusterCredentials")
	defer b.mu.RUnlock()

	if _, exists := b.clusters.Get(clusterID); !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	// Build a deterministic pseudo-password from clusterID and dbUser.
	seed := hex.EncodeToString([]byte(clusterID + dbUser))
	if len(seed) > credentialSeedLength {
		seed = seed[:credentialSeedLength]
	}

	return &ClusterCredentials{
		DBUser:     dbUser,
		DBPassword: "Tmp1_" + seed,
		Expiration: time.Now().Add(time.Hour),
	}, nil
}
