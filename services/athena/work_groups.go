package athena

import (
	"fmt"
	"maps"
	"sort"
	"time"
)

// athenaMinBytesScannedCutoff is the minimum value AWS Athena permits for the
// per-query data-scan limit (10 MB).
const athenaMinBytesScannedCutoff int64 = 10 * 1024 * 1024

// validateWorkGroupState reports an error if state is non-empty and not one of
// the two valid AWS values ("ENABLED" or "DISABLED"). An empty string is
// accepted where the caller treats it as "use default".
func validateWorkGroupState(state string) error {
	if state == "" || state == workGroupStateEnabled || state == workGroupStateDisabled {
		return nil
	}

	return fmt.Errorf(
		"%w: State %q is invalid; must be %s or %s",
		ErrValidation, state, workGroupStateEnabled, workGroupStateDisabled,
	)
}

// validateWorkGroupConfiguration enforces AWS-documented bounds for workgroup
// configuration knobs. Currently this only checks BytesScannedCutoffPerQuery
// (a positive value < 10 MiB is rejected; zero means "unlimited" and is
// permitted).
func validateWorkGroupConfiguration(cfg WorkGroupConfiguration) error {
	if cfg.BytesScannedCutoffPerQuery > 0 &&
		cfg.BytesScannedCutoffPerQuery < athenaMinBytesScannedCutoff {
		return fmt.Errorf(
			"%w: BytesScannedCutoffPerQuery must be at least %d bytes (10 MB)",
			ErrValidation, athenaMinBytesScannedCutoff,
		)
	}

	return nil
}

// CreateWorkGroup creates a new workgroup.
func (b *InMemoryBackend) CreateWorkGroup(
	name, description, state string,
	cfg WorkGroupConfiguration,
	tags map[string]string,
) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if err := validateWorkGroupState(state); err != nil {
		return err
	}

	if err := validateWorkGroupConfiguration(cfg); err != nil {
		return err
	}

	b.mu.Lock("CreateWorkGroup")
	defer b.mu.Unlock()

	if b.workGroups.Has(name) {
		return fmt.Errorf("%w: workgroup %q already exists", ErrAlreadyExists, name)
	}

	if state == "" {
		state = workGroupStateEnabled
	}

	now := float64(time.Now().UnixMilli()) / millisToSeconds
	b.workGroups.Put(&WorkGroup{
		Name:          name,
		Description:   description,
		State:         state,
		Tags:          maps.Clone(tags),
		Configuration: cfg,
		CreationTime:  now,
	})

	arn := b.workGroupARN(name)
	if len(tags) > 0 {
		b.resourceTags[arn] = copyTags(tags)
	}

	return nil
}

// GetWorkGroup retrieves a workgroup by name.
func (b *InMemoryBackend) GetWorkGroup(name string) (*WorkGroup, error) {
	b.mu.RLock("GetWorkGroup")
	defer b.mu.RUnlock()

	wg, ok := b.workGroups.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: workgroup %q not found", ErrNotFound, name)
	}

	cp := *wg
	cp.Tags = maps.Clone(wg.Tags)

	return &cp, nil
}

// ListWorkGroups returns summaries of all workgroups with optional NextToken/MaxResults pagination.
func (b *InMemoryBackend) ListWorkGroups(
	nextToken string,
	maxResults int,
) ([]*WorkGroupSummary, string, error) {
	b.mu.RLock("ListWorkGroups")
	defer b.mu.RUnlock()

	all := make([]*WorkGroupSummary, 0, b.workGroups.Len())
	for _, wg := range b.workGroups.All() {
		sum := &WorkGroupSummary{
			Name:         wg.Name,
			Description:  wg.Description,
			State:        wg.State,
			CreationTime: wg.CreationTime,
		}
		if ev := wg.Configuration.EngineVersion; ev.SelectedEngineVersion != "" ||
			ev.EffectiveEngineVersion != "" {
			cp := ev
			sum.EngineVersion = &cp
		}
		all = append(all, sum)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name < all[j].Name
	})

	const defaultMaxResults = 50
	limit := defaultMaxResults
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	start := 0
	if nextToken != "" {
		for i, s := range all {
			if s.Name == nextToken {
				start = i

				break
			}
		}
	}

	all = all[start:]

	outToken := ""
	if len(all) > limit {
		outToken = all[limit].Name
		all = all[:limit]
	}

	return all, outToken, nil
}

// UpdateWorkGroup updates an existing workgroup.
func (b *InMemoryBackend) UpdateWorkGroup(
	name, description, state string,
	cfg *WorkGroupConfiguration,
) error {
	if err := validateWorkGroupState(state); err != nil {
		return err
	}

	if cfg != nil {
		if err := validateWorkGroupConfiguration(*cfg); err != nil {
			return err
		}
	}

	b.mu.Lock("UpdateWorkGroup")
	defer b.mu.Unlock()

	wg, ok := b.workGroups.Get(name)
	if !ok {
		return fmt.Errorf("%w: workgroup %q not found", ErrNotFound, name)
	}

	if description != "" {
		wg.Description = description
	}

	if state != "" {
		wg.State = state
	}

	if cfg != nil {
		wg.Configuration = *cfg
	}

	return nil
}

// DeleteWorkGroup removes a workgroup by name. The "primary" workgroup cannot be deleted.
func (b *InMemoryBackend) DeleteWorkGroup(name string) error {
	b.mu.Lock("DeleteWorkGroup")
	defer b.mu.Unlock()

	if name == defaultWorkGroup {
		return fmt.Errorf("%w: cannot delete the primary workgroup", ErrProtected)
	}

	if !b.workGroups.Has(name) {
		return fmt.Errorf("%w: workgroup %q not found", ErrNotFound, name)
	}

	b.workGroups.Delete(name)
	delete(b.resourceTags, b.workGroupARN(name))

	return nil
}
