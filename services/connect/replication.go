package connect

import (
	"context"
	"regexp"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	arnRegionIndex    = 3
	replicaInProgress = "INSTANCE_REPLICATION_IN_PROGRESS"
	replicaComplete   = "INSTANCE_REPLICATION_COMPLETE"
)

var regionPattern = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-\d$`)

type replicaRec struct {
	Created awstime.EpochTime `json:"created"`
	Region  string            `json:"region"`
	Alias   string            `json:"alias,omitempty"`
	Arn     string            `json:"arn"`
}

// ReplicateInstanceInput is the ReplicateInstance request.
type ReplicateInstanceInput struct {
	ClientToken   string
	ReplicaAlias  string
	ReplicaRegion string
	instPath
}

// ReplicateInstanceOutput is the ReplicateInstance response.
type ReplicateInstanceOutput struct {
	Arn string `json:"Arn"`
	ID  string `json:"Id"`
}

// ReplicationStatusSummary is the replication state in one Region.
type ReplicationStatusSummary struct {
	Region            string `json:"Region"`
	ReplicationStatus string `json:"ReplicationStatus"`
}

// ReplicationConfiguration describes the replication of an instance across Regions.
type ReplicationConfiguration struct {
	SourceRegion                 string                     `json:"SourceRegion"`
	ReplicationStatusSummaryList []ReplicationStatusSummary `json:"ReplicationStatusSummaryList"`
}

// DescribeInstanceOutput is the DescribeInstance response.
type DescribeInstanceOutput struct {
	Instance                 *Instance                 `json:"Instance"`
	ReplicationConfiguration *ReplicationConfiguration `json:"ReplicationConfiguration,omitempty"`
}

// CrossRegionRoutingInput is the UpdateCrossRegionRouting request.
type CrossRegionRoutingInput struct {
	instPath
	IsolatedAll bool `json:"IsolatedAll"`
}

// CrossRegionRoutingOutput is the GetCrossRegionRouting response.
type CrossRegionRoutingOutput struct {
	IsolatedRegions []string `json:"IsolatedRegions"`
}

func regionOfARN(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) <= arnRegionIndex {
		return ""
	}

	return parts[arnRegionIndex]
}

func (b *Backend) replicaStatus(r replicaRec) string {
	if b.now().Sub(r.Created.Time) < b.delay {
		return replicaInProgress
	}

	return replicaComplete
}

// regionsOf lists the source Region and every replica Region of an instance, sorted.
func regionsOf(r *instanceRec) []string {
	out := make([]string, 1, 1+len(r.Replicas))
	out[0] = regionOfARN(r.Instance.Arn)

	for _, rp := range r.Replicas {
		out = append(out, rp.Region)
	}

	slices.Sort(out)

	return out
}

// ReplicateInstance creates a replica of an instance in another Region; it shares the instance ID.
func (b *Backend) ReplicateInstance(_ context.Context, in *ReplicateInstanceInput) (*ReplicateInstanceOutput, error) {
	if !regionPattern.MatchString(in.ReplicaRegion) {
		return nil, invalidParamErr("ReplicaRegion %q is not an Amazon Web Services Region", in.ReplicaRegion)
	}

	if in.ReplicaAlias != "" &&
		(len(in.ReplicaAlias) > maxAliasLen || !aliasPattern.MatchString(in.ReplicaAlias)) {
		return nil, invalidParamErr("ReplicaAlias must be 1-%d lowercase letters, digits or hyphens", maxAliasLen)
	}

	return writeInstance(b, "ReplicateInstance", in.InstanceID, func(inst string) (*ReplicateInstanceOutput, error) {
		r, err := b.instanceLocked(inst)
		if err != nil {
			return nil, err
		}

		src := regionOfARN(r.Instance.Arn)
		if in.ReplicaRegion == src {
			return nil, invalidRequestErr("the instance already exists in %s", src)
		}

		if b.viewInstance(r).InstanceStatus != statusActive {
			return nil, apiErr(statusNotFound, "ResourceNotReadyException", "instance %q is not active", inst)
		}

		if slices.Contains(regionsOf(r), in.ReplicaRegion) {
			return nil, resourceConflictErr("instance %q is already replicated to %s", inst, in.ReplicaRegion)
		}

		if in.ReplicaAlias != "" && b.aliasTaken(in.ReplicaAlias) {
			return nil, resourceConflictErr("the alias %q is already in use", in.ReplicaAlias)
		}

		rp := replicaRec{
			Created: awstime.NewEpochTime(b.now()), Region: in.ReplicaRegion, Alias: in.ReplicaAlias,
			Arn: strings.Replace(r.Instance.Arn, ":connect:"+src+":", ":connect:"+in.ReplicaRegion+":", 1),
		}
		r.Replicas = append(r.Replicas, rp)

		return &ReplicateInstanceOutput{Arn: rp.Arn, ID: inst}, nil
	})
}

func (b *Backend) aliasTaken(alias string) bool {
	for _, r := range b.instances.All() {
		if r.Instance.InstanceAlias == alias {
			return true
		}

		for _, rp := range r.Replicas {
			if rp.Alias == alias {
				return true
			}
		}
	}

	return false
}

// DescribeInstanceReplication describes an instance as seen from the Region of the request.
func (b *Backend) DescribeInstanceReplication(ctx context.Context, ref string) (*DescribeInstanceOutput, error) {
	b.mu.RLock("DescribeInstance")
	defer b.mu.RUnlock()

	r, err := b.instanceLocked(ref)
	if err != nil {
		return nil, err
	}

	out := b.viewInstance(r)
	if len(r.Replicas) == 0 {
		return &DescribeInstanceOutput{Instance: &out}, nil
	}

	src := regionOfARN(r.Instance.Arn)
	_, here := b.scope(ctx)
	cfg := &ReplicationConfiguration{SourceRegion: src}

	for _, rp := range r.Replicas {
		if rp.Region == here {
			out.Arn, out.InstanceAlias = rp.Arn, rp.Alias
			out.InstanceAccessURL = ""

			if rp.Alias != "" {
				out.InstanceAccessURL = "https://" + rp.Alias + ".my.connect.aws"
			}

			cfg.ReplicationStatusSummaryList = []ReplicationStatusSummary{
				{Region: rp.Region, ReplicationStatus: b.replicaStatus(rp)},
			}

			return &DescribeInstanceOutput{Instance: &out, ReplicationConfiguration: cfg}, nil
		}

		cfg.ReplicationStatusSummaryList = append(
			cfg.ReplicationStatusSummaryList,
			ReplicationStatusSummary{Region: rp.Region, ReplicationStatus: b.replicaStatus(rp)},
		)
	}

	return &DescribeInstanceOutput{Instance: &out, ReplicationConfiguration: cfg}, nil
}

func (b *Backend) globalResiliencyInstance(inst string) (*instanceRec, error) {
	r, err := b.instanceLocked(inst)
	if err != nil {
		return nil, err
	}

	if len(r.Replicas) == 0 {
		return nil, invalidRequestErr("instance %q has no replica: cross-region routing needs Global Resiliency", inst)
	}

	return r, nil
}

// GetCrossRegionRouting lists the Regions isolated from cross-region routing.
func (b *Backend) GetCrossRegionRouting(in *instPath) (*CrossRegionRoutingOutput, error) {
	return readInstance(
		b,
		"GetCrossRegionRouting",
		in.InstanceID,
		func(inst string) (*CrossRegionRoutingOutput, error) {
			r, err := b.globalResiliencyInstance(inst)
			if err != nil {
				return nil, err
			}

			out := &CrossRegionRoutingOutput{IsolatedRegions: []string{}}
			if r.IsolatedAll {
				out.IsolatedRegions = regionsOf(r)
			}

			return out, nil
		},
	)
}

// UpdateCrossRegionRouting isolates every Region of the instance from cross-region routing, or lifts it.
func (b *Backend) UpdateCrossRegionRouting(in *CrossRegionRoutingInput) (*EmptyOutput, error) {
	return writeInstance(b, "UpdateCrossRegionRouting", in.InstanceID, func(inst string) (*EmptyOutput, error) {
		r, err := b.globalResiliencyInstance(inst)
		if err != nil {
			return nil, err
		}

		r.IsolatedAll = in.IsolatedAll

		return &EmptyOutput{}, nil
	})
}
