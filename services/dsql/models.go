package dsql

import (
	"maps"
	"slices"
	"time"
)

// Cluster status values, matching aws-sdk-go-v2/service/dsql/types.ClusterStatus.
const (
	statusCreating = "CREATING"
	statusActive   = "ACTIVE"
	statusUpdating = "UPDATING"
	statusDeleting = "DELETING"
)

// Stream status values, matching aws-sdk-go-v2/service/dsql/types.StreamStatus.
const (
	streamStatusCreating = "CREATING"
	streamStatusActive   = "ACTIVE"
)

const (
	encryptionTypeAWSOwned     = "AWS_OWNED_KMS_KEY"
	encryptionTypeCustomer     = "CUSTOMER_MANAGED_KMS_KEY"
	encryptionStatusEnabled    = "ENABLED"
	validationReasonLockedOut  = "deletionProtectionEnabled"
	validationReasonFieldError = "fieldValidationFailed"
	validationReasonOther      = "other"
)

// MultiRegionProperties mirrors types.MultiRegionProperties.
type MultiRegionProperties struct {
	WitnessRegion string
	Clusters      []string
}

func (m *MultiRegionProperties) clone() *MultiRegionProperties {
	if m == nil {
		return nil
	}

	cp := *m
	cp.Clusters = slices.Clone(m.Clusters)

	return &cp
}

// ClusterPolicy is a resource-based policy attached to a cluster.
type ClusterPolicy struct {
	Policy  string
	Version string
}

// Cluster is the persisted representation of an Aurora DSQL cluster.
type Cluster struct {
	CreationTime              time.Time
	PendingUntil              time.Time
	Policy                    *ClusterPolicy
	MultiRegion               *MultiRegionProperties
	Tags                      map[string]string
	Identifier                string
	ARN                       string
	Endpoint                  string
	Status                    string
	KmsEncryptionKey          string
	AccountID                 string
	Region                    string
	DeletionProtectionEnabled bool
}

func (c *Cluster) clone() *Cluster {
	if c == nil {
		return nil
	}

	cp := *c
	cp.Tags = make(map[string]string, len(c.Tags))
	maps.Copy(cp.Tags, c.Tags)
	cp.MultiRegion = c.MultiRegion.clone()

	if c.Policy != nil {
		p := *c.Policy
		cp.Policy = &p
	}

	return &cp
}

func (c *Cluster) encryptionType() string {
	if c.KmsEncryptionKey == "" {
		return encryptionTypeAWSOwned
	}

	return encryptionTypeCustomer
}

func (c *Cluster) kmsKeyARN() string {
	if c.KmsEncryptionKey == "" {
		return ""
	}

	return c.KmsEncryptionKey
}

// StreamTarget mirrors types.TargetDefinitionMemberKinesis.
type StreamTarget struct {
	RoleArn   string
	StreamArn string
}

// Stream is the persisted representation of an Aurora DSQL change-data-capture stream.
type Stream struct {
	CreationTime      time.Time
	PendingUntil      time.Time
	Target            *StreamTarget
	Tags              map[string]string
	ClusterIdentifier string
	StreamIdentifier  string
	ARN               string
	Status            string
	Format            string
	Ordering          string
}

func (s *Stream) clone() *Stream {
	if s == nil {
		return nil
	}

	cp := *s
	cp.Tags = make(map[string]string, len(s.Tags))
	maps.Copy(cp.Tags, s.Tags)

	if s.Target != nil {
		t := *s.Target
		cp.Target = &t
	}

	return &cp
}
