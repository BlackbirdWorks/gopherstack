package cloudhsmv2

import (
	"cmp"
	"context"
	"slices"
)

// CFNClusterSpec is the AWS::CloudHSM::Cluster property set.
type CFNClusterSpec struct {
	Tags           map[string]string
	HsmType        string
	Mode           string
	NetworkType    string
	RetentionType  string
	RetentionValue string
	SubnetIDs      []string
}

// CFNCluster is the read-only attribute set of AWS::CloudHSM::Cluster.
type CFNCluster struct {
	ID            string
	ARN           string
	State         string
	VpcID         string
	SecurityGroup string
	BackupPolicy  string
}

// CFNHsm is the attribute set of an HSM.
type CFNHsm struct {
	ID           string
	ClusterID    string
	Zone         string
	EniID        string
	EniIP        string
	State        string
	StateMessage string
}

func (s CFNClusterSpec) retention() *wireRetention {
	if s.RetentionType == "" && s.RetentionValue == "" {
		return nil
	}

	return &wireRetention{Type: s.RetentionType, Value: s.RetentionValue}
}

func (b *InMemoryBackend) cfnClusterOf(ctx context.Context, c wireCluster) CFNCluster {
	sc := b.scopeOf(ctx)

	return CFNCluster{
		ID: c.ClusterID, ARN: clusterARN(sc.account, sc.region, c.ClusterID), State: c.State, VpcID: c.VpcID,
		SecurityGroup: c.SecurityGroup, BackupPolicy: c.BackupPolicy,
	}
}

// CFNCreateCluster creates a cluster from CloudFormation properties.
func (b *InMemoryBackend) CFNCreateCluster(ctx context.Context, spec CFNClusterSpec) (CFNCluster, error) {
	tags := make([]wireTag, 0, len(spec.Tags))
	for k, v := range spec.Tags {
		tags = append(tags, wireTag{Key: k, Value: v})
	}

	slices.SortFunc(tags, func(a, z wireTag) int { return cmp.Compare(a.Key, z.Key) })

	c, err := b.CreateCluster(ctx, CreateClusterInput{
		Retention: spec.retention(), HsmType: spec.HsmType, Mode: spec.Mode, NetworkType: spec.NetworkType,
		SubnetIDs: spec.SubnetIDs, Tags: tags,
	})
	if err != nil {
		return CFNCluster{}, err
	}

	return b.cfnClusterOf(ctx, c), nil
}

// CFNDescribeCluster returns a cluster, or ErrResourceNotFound once it is gone.
func (b *InMemoryBackend) CFNDescribeCluster(ctx context.Context, id string) (CFNCluster, error) {
	p, err := b.DescribeClusters(ctx, map[string][]string{"clusterIds": {id}}, "", nil)
	if err != nil {
		return CFNCluster{}, err
	}

	if len(p.Data) == 0 {
		return CFNCluster{}, fail(ErrResourceNotFound, "cluster %s not found", id)
	}

	return b.cfnClusterOf(ctx, p.Data[0]), nil
}

// CFNUpdateCluster applies the mutable cluster properties: the backup retention policy and the tags.
func (b *InMemoryBackend) CFNUpdateCluster(ctx context.Context, id string, spec CFNClusterSpec) error {
	if r := spec.retention(); r != nil {
		if _, err := b.ModifyCluster(ctx, ModifyClusterInput{ClusterID: id, Retention: r}); err != nil {
			return err
		}
	}

	current, err := b.ListTags(ctx, id, "", nil)
	if err != nil {
		return err
	}

	var drop []string

	for _, t := range current.Data {
		if _, keep := spec.Tags[t.Key]; !keep {
			drop = append(drop, t.Key)
		}
	}

	if len(drop) > 0 {
		if err = b.UntagResource(ctx, id, drop); err != nil {
			return err
		}
	}

	if len(spec.Tags) > 0 {
		return b.TagResourceByARN(ctx, id, spec.Tags)
	}

	return nil
}

// CFNDeleteCluster starts deleting a cluster.
func (b *InMemoryBackend) CFNDeleteCluster(ctx context.Context, id string) error {
	_, err := b.DeleteCluster(ctx, id)

	return err
}

// CFNCreateHsm adds an HSM to a cluster.
func (b *InMemoryBackend) CFNCreateHsm(ctx context.Context, clusterID, zone, ip string) (CFNHsm, error) {
	h, err := b.CreateHsm(ctx, clusterID, zone, ip)
	if err != nil {
		return CFNHsm{}, err
	}

	return cfnHsmOf(h), nil
}

func cfnHsmOf(h wireHsm) CFNHsm {
	return CFNHsm{
		ID: h.HsmID, ClusterID: h.ClusterID, Zone: h.AZ, EniID: h.EniID, EniIP: h.EniIP,
		State: h.State, StateMessage: h.StateMessage,
	}
}

// CFNDescribeHsm returns an HSM, or ErrResourceNotFound once it is gone.
func (b *InMemoryBackend) CFNDescribeHsm(ctx context.Context, id string) (CFNHsm, error) {
	sc := b.scopeOf(ctx)

	b.mu.RLock("CFNDescribeHsm")
	defer b.mu.RUnlock()

	h, ok := b.hsms.Get(id)
	if !ok || h.Account != sc.account || h.Region != sc.region ||
		(!h.DeletedAt.IsZero() && !b.now().Before(h.DeleteUntil)) {
		return CFNHsm{}, fail(ErrResourceNotFound, "HSM %s not found", id)
	}

	return cfnHsmOf(b.wireHsmLocked(h)), nil
}

// CFNDeleteHsm starts deleting an HSM by its ID alone.
func (b *InMemoryBackend) CFNDeleteHsm(ctx context.Context, id string) error {
	h, err := b.CFNDescribeHsm(ctx, id)
	if err != nil {
		return err
	}

	_, err = b.DeleteHsm(ctx, DeleteHsmInput{ClusterID: h.ClusterID, HsmID: id})

	return err
}
