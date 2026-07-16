package redshift

import (
	"encoding/hex"
	"fmt"
	"time"
)

const credentialSeedLength = 8

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

// GetClusterCredentialsWithIAM returns temporary cluster credentials including an IAM role.
func (b *InMemoryBackend) GetClusterCredentialsWithIAM(clusterID, _ string) (*ClusterCredentials, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetClusterCredentialsWithIAM")
	defer b.mu.RUnlock()

	if _, exists := b.clusters.Get(clusterID); !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	return &ClusterCredentials{
		DBUser:     "IAMUser:" + clusterID,
		DBPassword: "Tmp1_iam" + clusterID,
		Expiration: time.Now().Add(time.Hour),
	}, nil
}
