package redshift

import (
	"encoding/hex"
	"fmt"
	"time"
)

const credentialSeedLength = 8

// GetClusterCredentials(WithIAM)'s documented DurationSeconds constraint
// ("Constraint: minimum 900, maximum 3600. Default: 900" -- confirmed
// against redshift@v1.65.4 api_op_GetClusterCredentials.go and
// api_op_GetClusterCredentialsWithIAM.go, which share the same range).
const (
	defaultCredentialsDurationSeconds = 900
	minCredentialsDurationSeconds     = 900
	maxCredentialsDurationSeconds     = 3600
)

// resolveCredentialsDuration validates and defaults DurationSeconds, shared
// by GetClusterCredentials and GetClusterCredentialsWithIAM. A nil
// durationSeconds means the caller omitted it (default 900).
func resolveCredentialsDuration(durationSeconds *int) (time.Duration, error) {
	d := defaultCredentialsDurationSeconds
	if durationSeconds != nil {
		d = *durationSeconds
		if d < minCredentialsDurationSeconds || d > maxCredentialsDurationSeconds {
			return 0, fmt.Errorf(
				"%w: DurationSeconds must be between %d and %d",
				ErrInvalidParameter, minCredentialsDurationSeconds, maxCredentialsDurationSeconds,
			)
		}
	}

	return time.Duration(d) * time.Second, nil
}

// GetClusterCredentials generates temporary credentials for a cluster user.
func (b *InMemoryBackend) GetClusterCredentials(
	clusterID, dbUser string,
	_ bool,
	durationSeconds *int,
) (*ClusterCredentials, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	if dbUser == "" {
		return nil, fmt.Errorf("%w: DbUser is required", ErrInvalidParameter)
	}

	duration, err := resolveCredentialsDuration(durationSeconds)
	if err != nil {
		return nil, err
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
		Expiration: time.Now().Add(duration),
	}, nil
}

// GetClusterCredentialsWithIAM returns temporary cluster credentials including an IAM role.
func (b *InMemoryBackend) GetClusterCredentialsWithIAM(
	clusterID, _ string,
	durationSeconds *int,
) (*ClusterCredentials, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	duration, err := resolveCredentialsDuration(durationSeconds)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetClusterCredentialsWithIAM")
	defer b.mu.RUnlock()

	if _, exists := b.clusters.Get(clusterID); !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	return &ClusterCredentials{
		DBUser:     "IAMUser:" + clusterID,
		DBPassword: "Tmp1_iam" + clusterID,
		Expiration: time.Now().Add(duration),
	}, nil
}
