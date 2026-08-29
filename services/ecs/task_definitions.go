package ecs

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// fargateCPUMemoryPairs returns valid Fargate CPU/memory combinations per the AWS docs.
// Keys are CPU values (vCPU units as strings); values are allowed memory values (MiB as strings).
func fargateCPUMemoryPairs() map[string][]string {
	return map[string][]string{
		"256":  {"512", "1024", "2048"},
		"512":  {"1024", "2048", "3072", "4096"},
		"1024": {"2048", "3072", "4096", "5120", "6144", "7168", "8192"},
		"2048": {
			"4096", "5120", "6144", "7168", "8192", "9216", "10240", "11264",
			"12288", "13312", "14336", "15360", "16384",
		},
		"4096": {
			"8192", "9216", "10240", "11264", "12288", "13312", "14336", "15360",
			"16384", "17408", "18432", "19456", "20480", "21504", "22528", "23552",
			"24576", "25600", "26624", "27648", "28672", "29696", "30720",
		},
		"8192": {
			"16384", "20480", "24576", "28672", "32768", "36864", "40960", "45056",
			"49152", "53248", "57344", "61440",
		},
		"16384": {
			"32768", "40960", "49152", "57344", "65536", "73728", "81920", "90112",
			"98304", "106496", "114688", "122880",
		},
	}
}

// validateFargateCPUMemory returns an error if the cpu/memory combination is
// invalid for Fargate. It is a no-op when either value is empty (EC2 launch type).
func validateFargateCPUMemory(cpu, memory string) error {
	if cpu == "" || memory == "" {
		return nil
	}

	pairs := fargateCPUMemoryPairs()
	validMemories, ok := pairs[cpu]

	if !ok {
		return fmt.Errorf(
			"%w: invalid Fargate CPU value %q; valid values: 256, 512, 1024, 2048, 4096, 8192, 16384",
			ErrInvalidParameter,
			cpu,
		)
	}

	if slices.Contains(validMemories, memory) {
		return nil
	}

	return fmt.Errorf(
		"%w: invalid Fargate memory %q for CPU %q; valid values: %s",
		ErrInvalidParameter, memory, cpu, strings.Join(validMemories, ", "),
	)
}

// RegisterTaskDefinition registers a new task definition revision.
func (b *InMemoryBackend) RegisterTaskDefinition(
	input RegisterTaskDefinitionInput,
) (*TaskDefinition, error) {
	if input.Family == "" {
		return nil, fmt.Errorf("%w: family is required", ErrInvalidParameter)
	}

	if err := validateRegisterTaskDefinition(input); err != nil {
		return nil, err
	}

	isFargate := false

	for _, rc := range input.RequiresCompatibilities {
		if strings.EqualFold(rc, launchTypeFargate) {
			isFargate = true

			break
		}
	}

	if isFargate {
		if err := validateFargateCPUMemory(input.CPU, input.Memory); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("RegisterTaskDefinition")
	defer b.mu.Unlock()

	revisions := b.taskDefinitions[input.Family]

	// Determine the next revision number based on the last stored revision
	// so trimming the cap does not desync the counter.
	revision := 1
	if len(revisions) > 0 {
		revision = revisions[len(revisions)-1].Revision + 1
	}

	td := &TaskDefinition{
		RegisteredAt: time.Now(),
		TaskDefinitionArn: fmt.Sprintf(
			"arn:aws:ecs:%s:%s:task-definition/%s:%d",
			b.region,
			b.accountID,
			input.Family,
			revision,
		),
		Family:                  input.Family,
		TaskRoleArn:             input.TaskRoleArn,
		ExecutionRoleArn:        input.ExecutionRoleArn,
		NetworkMode:             input.NetworkMode,
		CPU:                     input.CPU,
		Memory:                  input.Memory,
		PlatformFamily:          input.PlatformFamily,
		Status:                  statusActive,
		ContainerDefinitions:    input.ContainerDefinitions,
		Volumes:                 input.Volumes,
		PlacementConstraints:    input.PlacementConstraints,
		RequiresCompatibilities: input.RequiresCompatibilities,
		RuntimePlatform:         input.RuntimePlatform,
		EphemeralStorage:        input.EphemeralStorage,
		InferenceAccelerators:   input.InferenceAccelerators,
		Revision:                revision,
	}

	revisions = append(revisions, td)

	// Enforce the revision cap: if we exceed maxTaskDefinitionRevisions, trim
	// the oldest entries to prevent unbounded memory growth.
	if len(revisions) > maxTaskDefinitionRevisions {
		excess := len(revisions) - maxTaskDefinitionRevisions

		for _, evicted := range revisions[:excess] {
			b.taskDefByArn.Delete(evicted.TaskDefinitionArn)
		}

		revisions = revisions[excess:]
	}

	b.taskDefinitions[input.Family] = revisions
	b.taskDefByArn.Put(td)

	// Persist registration tags via the same resourceTags map used by
	// TagResource so that DescribeTaskDefinition can surface them when
	// the include=TAGS option is provided.
	if len(input.Tags) > 0 {
		if b.resourceTags == nil {
			b.resourceTags = make(map[string][]Tag)
		}

		copied := make([]Tag, len(input.Tags))
		copy(copied, input.Tags)
		b.resourceTags[resourceTagKey(td.TaskDefinitionArn)] = copied
	}

	cp := *td

	return &cp, nil
}

// DescribeTaskDefinition returns the latest revision of a task definition by family or ARN.
func (b *InMemoryBackend) DescribeTaskDefinition(familyOrArn string) (*TaskDefinition, error) {
	b.mu.RLock("DescribeTaskDefinition")
	defer b.mu.RUnlock()

	return b.findTaskDefinitionLocked(familyOrArn)
}

// findFamilyRevisionLocked looks up a task definition by "family:revision" shorthand.
// Must be called with lock held.
func (b *InMemoryBackend) findFamilyRevisionLocked(familyOrArn string) (*TaskDefinition, bool) {
	idx := strings.LastIndex(familyOrArn, ":")
	if idx <= 0 {
		return nil, false
	}

	family := familyOrArn[:idx]

	revNum, err := strconv.Atoi(familyOrArn[idx+1:])
	if err != nil {
		return nil, false
	}

	for _, td := range b.taskDefinitions[family] {
		if td.Revision == revNum {
			cp := *td

			return &cp, true
		}
	}

	return nil, false
}

// findTaskDefinitionLocked finds a task definition. Must be called with lock held.
func (b *InMemoryBackend) findTaskDefinitionLocked(familyOrArn string) (*TaskDefinition, error) {
	// Try direct family lookup (latest revision).
	if revs, ok := b.taskDefinitions[familyOrArn]; ok && len(revs) > 0 {
		cp := *revs[len(revs)-1]

		return &cp, nil
	}

	// Fast path: ARN cache lookup.
	if td, ok := b.taskDefByArn.Get(familyOrArn); ok {
		cp := *td

		return &cp, nil
	}

	// Fallback scan when the ARN cache is empty or stale (e.g. after restore).
	for _, revs := range b.taskDefinitions {
		for _, td := range revs {
			if td.TaskDefinitionArn == familyOrArn {
				cp := *td

				return &cp, nil
			}
		}
	}

	// Support "family:revision" shorthand.
	if td, ok := b.findFamilyRevisionLocked(familyOrArn); ok {
		return td, nil
	}

	return nil, fmt.Errorf("%w: task definition %s not found", ErrInvalidParameter, familyOrArn)
}

// DeregisterTaskDefinition marks a task definition revision as INACTIVE.
func (b *InMemoryBackend) DeregisterTaskDefinition(
	taskDefinitionArn string,
) (*TaskDefinition, error) {
	b.mu.Lock("DeregisterTaskDefinition")
	defer b.mu.Unlock()

	td, err := b.findTaskDefinitionLocked(taskDefinitionArn)
	if err != nil {
		return nil, err
	}

	for _, revs := range b.taskDefinitions {
		for _, r := range revs {
			if r.TaskDefinitionArn == td.TaskDefinitionArn {
				r.Status = statusInactive
				cp := *r

				return &cp, nil
			}
		}
	}

	return nil, fmt.Errorf("%w: task definition %s not found", ErrInvalidParameter, taskDefinitionArn)
}

// ListTaskDefinitions returns ARNs of task definitions, optionally filtered by family prefix.
// ARNs are returned sorted for deterministic output.
func (b *InMemoryBackend) ListTaskDefinitions(familyPrefix string) ([]string, error) {
	return b.ListTaskDefinitionsFiltered(ListTaskDefinitionsInput{FamilyPrefix: familyPrefix})
}

// ListTaskDefinitionsFiltered returns task definition ARNs with status
// filtering. By default (input.Sort != "DESC"), results are ordered
// lexicographically by family name and in ascending numerical order by
// revision, matching the real API's documented default
// (ecs@v1.90.0 api_op_ListTaskDefinitions.go ListTaskDefinitionsInput.Sort);
// "DESC" reverses both. Revision must be compared numerically, not as part of
// the ARN string -- "family:10" sorts before "family:2" as a string.
func (b *InMemoryBackend) ListTaskDefinitionsFiltered(
	input ListTaskDefinitionsInput,
) ([]string, error) {
	b.mu.RLock("ListTaskDefinitionsFiltered")
	defer b.mu.RUnlock()

	wantStatus := strings.ToUpper(input.Status)
	if wantStatus == "" {
		wantStatus = statusActive
	}

	var tds []*TaskDefinition

	for family, revs := range b.taskDefinitions {
		if input.FamilyPrefix != "" && !strings.HasPrefix(family, input.FamilyPrefix) {
			continue
		}

		for _, td := range revs {
			if strings.EqualFold(td.Status, wantStatus) {
				tds = append(tds, td)
			}
		}
	}

	slices.SortFunc(tds, func(a, c *TaskDefinition) int {
		if n := strings.Compare(a.Family, c.Family); n != 0 {
			return n
		}

		return a.Revision - c.Revision
	})

	if strings.EqualFold(input.Sort, "DESC") {
		slices.Reverse(tds)
	}

	arns := make([]string, 0, len(tds))
	for _, td := range tds {
		arns = append(arns, td.TaskDefinitionArn)
	}

	return arns, nil
}

// ListTaskDefinitionFamilies returns distinct task definition family names.
func (b *InMemoryBackend) ListTaskDefinitionFamilies(
	familyPrefix, status string,
) ([]string, error) {
	b.mu.RLock("ListTaskDefinitionFamilies")
	defer b.mu.RUnlock()

	families := make([]string, 0, len(b.taskDefinitions))

	for family, revs := range b.taskDefinitions {
		if familyPrefix != "" && !strings.HasPrefix(family, familyPrefix) {
			continue
		}

		// If a status filter is given, include the family only when at least one
		// revision matches that status.
		if status != "" {
			found := false

			for _, td := range revs {
				if strings.EqualFold(td.Status, status) {
					found = true

					break
				}
			}

			if !found {
				continue
			}
		}

		families = append(families, family)
	}

	return families, nil
}

// DeleteTaskDefinitions deletes task definitions that are INACTIVE.
// Definitions that are not INACTIVE are reported as failures.
func (b *InMemoryBackend) DeleteTaskDefinitions(
	taskDefinitionArns []string,
) ([]TaskDefinition, []Failure, error) {
	if len(taskDefinitionArns) == 0 {
		return nil, nil, fmt.Errorf("%w: taskDefinitions is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteTaskDefinitions")
	defer b.mu.Unlock()

	deleted := make([]TaskDefinition, 0, len(taskDefinitionArns))
	failures := make([]Failure, 0, len(taskDefinitionArns))

	for _, arnRef := range taskDefinitionArns {
		td, err := b.findTaskDefinitionLocked(arnRef)
		if err != nil {
			failures = append(failures, Failure{
				Arn:    arnRef,
				Reason: statusMissing,
				Detail: err.Error(),
			})

			continue
		}

		if td.Status != statusInactive {
			failures = append(failures, Failure{
				Arn:    td.TaskDefinitionArn,
				Reason: "INVALID",
				Detail: fmt.Sprintf("task definition %s is not in INACTIVE state", arnRef),
			})

			continue
		}

		// Remove the revision from the family slice.
		revs := b.taskDefinitions[td.Family]

		for i, r := range revs {
			if r.TaskDefinitionArn == td.TaskDefinitionArn {
				b.taskDefinitions[td.Family] = append(revs[:i], revs[i+1:]...)
				b.taskDefByArn.Delete(td.TaskDefinitionArn)
				b.deleteResourceTagsLocked(td.TaskDefinitionArn)

				break
			}
		}

		deleted = append(deleted, *td)
	}

	return deleted, failures, nil
}
