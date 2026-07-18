package batch

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	jobDefStatusActive   = "ACTIVE"
	jobDefStatusInactive = "INACTIVE"
)

// jobDefNameRegex validates AWS Batch job definition names.
var jobDefNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)

// RegisterJobDefinition registers a new job definition (or a new revision).
func (b *InMemoryBackend) RegisterJobDefinition(
	ctx context.Context,
	name, defType string,
	tags map[string]string,
	platformCapabilities []string,
	timeoutSeconds int32,
	schedulingPriority int32,
	containerProps *ContainerProperties,
	nodeProps *NodeProperties,
	eksProps *EksProperties,
	runtimePlatform *RuntimePlatform,
	consumableResourceProperties []ConsumableResourceProperty,
	parameters map[string]string,
	propagateTags bool,
) (*JobDefinition, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("RegisterJobDefinition")
	defer b.mu.Unlock()

	if !jobDefNameRegex.MatchString(name) {
		return nil, fmt.Errorf(
			"%w: jobDefinitionName must match [a-zA-Z0-9_-]{1,128}",
			ErrValidation,
		)
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	revisions := b.jobDefRevisionsStore(region)
	revisions[name]++
	revision := revisions[name]

	jdARN := arn.Build("batch", region, b.accountID, fmt.Sprintf("job-definition/%s:%d", name, revision))

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	var timeout *JobTimeout
	if timeoutSeconds > 0 {
		timeout = &JobTimeout{AttemptDurationSeconds: timeoutSeconds}
	}

	jd := &JobDefinition{
		region:                       region,
		JobDefinitionName:            name,
		JobDefinitionArn:             jdARN,
		Type:                         defType,
		Status:                       jobDefStatusActive,
		Revision:                     revision,
		Tags:                         tagsCopy,
		PlatformCapabilities:         platformCapabilities,
		Timeout:                      timeout,
		SchedulingPriority:           schedulingPriority,
		ContainerProperties:          containerProps,
		NodeProperties:               nodeProps,
		EksProperties:                eksProps,
		RuntimePlatform:              runtimePlatform,
		ConsumableResourceProperties: newConsumableResourceProperties(consumableResourceProperties),
		Parameters:                   maps.Clone(parameters),
		PropagateTags:                propagateTags,
	}
	b.jobDefinitions.Put(jd)
	cp := *jd

	return &cp, nil
}

// DescribeJobDefinitions returns job definitions, optionally filtered by names/ARNs.
// When names is empty, results are paginated via maxResults/nextToken.
func (b *InMemoryBackend) DescribeJobDefinitions(
	ctx context.Context,
	names []string,
	status, jobDefinitionName string,
	maxResults int32,
	nextToken string,
) ([]*JobDefinition, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeJobDefinitions")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		return b.describeAllJobDefinitions(region, status, jobDefinitionName, maxResults, nextToken)
	}

	list := b.describeJobDefinitionsByNames(region, names, status)

	return list, ""
}

func (b *InMemoryBackend) describeAllJobDefinitions(
	region, status, jobDefinitionName string,
	maxResults int32,
	nextToken string,
) ([]*JobDefinition, string) {
	var arns []string

	for _, jd := range b.jobDefinitionsByRegion.Get(region) {
		if jobDefinitionName != "" && jd.JobDefinitionName != jobDefinitionName {
			continue
		}

		if status != "" && jd.Status != status {
			continue
		}

		arns = append(arns, jd.JobDefinitionArn)
	}

	// The comparator below fully orders arns (JobDefinitionName ties are
	// impossible for equal Revision, since revision is a per-name counter),
	// so the pre-sort order above is irrelevant to the final result.
	sort.Slice(arns, func(i, j int) bool {
		a, _ := b.jobDefinitions.Get(regionKey(region, arns[i]))
		c, _ := b.jobDefinitions.Get(regionKey(region, arns[j]))
		if a.JobDefinitionName == c.JobDefinitionName {
			return a.Revision > c.Revision
		}

		return a.JobDefinitionName < c.JobDefinitionName
	})

	keys, next := paginateMapKeys(arns, nextToken, maxResults)
	out := make([]*JobDefinition, 0, len(keys))

	for _, k := range keys {
		jd, _ := b.jobDefinitions.Get(regionKey(region, k))
		cp := *jd
		cp.Tags = tagsCloneOrEmpty(cp.Tags)
		out = append(out, &cp)
	}

	return out, next
}

func (b *InMemoryBackend) describeJobDefinitionsByNames(region string, names []string, status string) []*JobDefinition {
	seen := make(map[string]bool)
	list := make([]*JobDefinition, 0, len(names))

	for _, nameOrARN := range names {
		if jd, ok := b.jobDefinitions.Get(regionKey(region, nameOrARN)); ok {
			list = appendJobDefinitionMatch(list, seen, jd, status)

			continue
		}

		// Not a stored ARN: treat nameOrARN as "name" or "name:revision". A
		// caller-supplied revision must be matched exactly -- AWS returns only
		// the requested revision, not every revision of that name.
		baseName, revision, hasRevision := parseJobDefRevision(nameOrARN)

		for _, jd := range b.jobDefinitionsByRegion.Get(region) {
			if jd.JobDefinitionName != baseName || (hasRevision && jd.Revision != revision) {
				continue
			}

			list = appendJobDefinitionMatch(list, seen, jd, status)
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Revision > list[j].Revision
	})

	return list
}

// appendJobDefinitionMatch appends a tag-cloned copy of jd to list, unless it
// was already added (tracked by ARN in seen, since an ARN lookup and a
// name/name:revision lookup can resolve to the same definition) or it fails
// the optional status filter.
func appendJobDefinitionMatch(
	list []*JobDefinition, seen map[string]bool, jd *JobDefinition, status string,
) []*JobDefinition {
	if seen[jd.JobDefinitionArn] || (status != "" && jd.Status != status) {
		return list
	}

	seen[jd.JobDefinitionArn] = true
	cp := *jd
	cp.Tags = tagsCloneOrEmpty(jd.Tags)

	return append(list, &cp)
}

// DeregisterJobDefinition marks a job definition as INACTIVE by ARN or name:revision.
// INACTIVE definitions remain visible in DescribeJobDefinitions (matching AWS behavior)
// and are swept by the janitor after the configured TTL.
func (b *InMemoryBackend) DeregisterJobDefinition(ctx context.Context, arnOrNameRev string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeregisterJobDefinition")
	defer b.mu.Unlock()

	now := time.Now()

	// Try direct ARN lookup first.
	if jd, ok := b.jobDefinitions.Get(regionKey(region, arnOrNameRev)); ok {
		jd.Status = jobDefStatusInactive
		jd.DeregisteredAt = &now

		return nil
	}

	// Fall back to name:revision lookup (e.g. "my-job:3").
	for _, jd := range b.jobDefinitionsByRegion.Get(region) {
		nameRev := fmt.Sprintf("%s:%d", jd.JobDefinitionName, jd.Revision)
		if nameRev == arnOrNameRev {
			jd.Status = jobDefStatusInactive
			jd.DeregisteredAt = &now

			return nil
		}
	}

	return fmt.Errorf("%w: job definition %s not found", ErrNotFound, arnOrNameRev)
}
