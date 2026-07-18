package codedeploy

import (
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// onPremInstanceNameRe validates on-premises instance names per AWS spec.
var onPremInstanceNameRe = regexp.MustCompile(`^[A-Za-z0-9._\-]{1,100}$`)

// AddTagsToOnPremisesInstances adds tags to on-premises instances, registering them if needed.
func (b *InMemoryBackend) AddTagsToOnPremisesInstances(instanceNames []string, kv map[string]string) error {
	b.mu.Lock("AddTagsToOnPremisesInstances")
	defer b.mu.Unlock()

	for _, name := range instanceNames {
		inst, ok := b.onPremisesInstances.Get(name)
		if !ok {
			t := tags.New("codedeploy.onprem." + name + ".tags")
			inst = &OnPremisesInstance{
				InstanceName: name,
				RegisterTime: time.Now().UTC(),
				Tags:         t,
			}
			b.onPremisesInstances.Put(inst)
		}

		inst.Tags.Merge(kv)
	}

	return nil
}

// RemoveTagsFromOnPremisesInstances removes tag keys from the given on-premises instances.
func (b *InMemoryBackend) RemoveTagsFromOnPremisesInstances(instanceNames []string, keys []string) error {
	b.mu.Lock("RemoveTagsFromOnPremisesInstances")
	defer b.mu.Unlock()

	for _, name := range instanceNames {
		inst, ok := b.onPremisesInstances.Get(name)
		if !ok {
			continue
		}

		inst.Tags.DeleteKeys(keys)
	}

	return nil
}

// RegisterOnPremisesInstance registers a new on-premises instance.
// Exactly one of iamSessionArn or iamUserArn must be set.
func (b *InMemoryBackend) RegisterOnPremisesInstance(name, iamSessionArn, iamUserArn string) error {
	b.mu.Lock("RegisterOnPremisesInstance")
	defer b.mu.Unlock()

	if !onPremInstanceNameRe.MatchString(name) {
		return fmt.Errorf("%w: instance name %q does not match pattern [A-Za-z0-9._-]{1,100}", ErrValidation, name)
	}

	if iamSessionArn != "" && iamUserArn != "" {
		return fmt.Errorf("%w: only one of iamSessionArn or iamUserArn may be set", ErrMultipleIamArns)
	}

	if iamSessionArn == "" && iamUserArn == "" {
		return fmt.Errorf("%w: one of iamSessionArn or iamUserArn must be set", ErrIamArnRequired)
	}

	if b.onPremisesInstances.Has(name) {
		return nil
	}

	t := tags.New("codedeploy.onprem." + name + ".tags")
	b.onPremisesInstances.Put(&OnPremisesInstance{
		InstanceName:  name,
		IamSessionArn: iamSessionArn,
		IamUserArn:    iamUserArn,
		RegisterTime:  time.Now().UTC(),
		Tags:          t,
	})

	return nil
}

// DeregisterOnPremisesInstance marks an on-premises instance as deregistered.
func (b *InMemoryBackend) DeregisterOnPremisesInstance(name string) error {
	b.mu.Lock("DeregisterOnPremisesInstance")
	defer b.mu.Unlock()

	inst, ok := b.onPremisesInstances.Get(name)
	if !ok {
		return fmt.Errorf("%w: instance %s not found", ErrOnPremisesInstanceNotFound, name)
	}

	now := time.Now().UTC()
	inst.DeregisterTime = &now

	return nil
}

// GetOnPremisesInstance returns an on-premises instance by name.
func (b *InMemoryBackend) GetOnPremisesInstance(name string) (*OnPremisesInstance, error) {
	b.mu.RLock("GetOnPremisesInstance")
	defer b.mu.RUnlock()

	inst, ok := b.onPremisesInstances.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: instance %s not found", ErrOnPremisesInstanceNotFound, name)
	}

	cp := *inst

	return &cp, nil
}

// ListOnPremisesInstances returns instance names, optionally filtered by registration status and tag filters.
func (b *InMemoryBackend) ListOnPremisesInstances(registrationStatus string, tagFilters []TagFilter) []string {
	b.mu.RLock("ListOnPremisesInstances")
	defer b.mu.RUnlock()

	all := b.onPremisesInstances.All()
	names := make([]string, 0, len(all))
	for _, inst := range all {
		if registrationStatus == "Deregistered" && inst.DeregisterTime == nil {
			continue
		}
		if registrationStatus == "Registered" && inst.DeregisterTime != nil {
			continue
		}
		if len(tagFilters) > 0 && !matchesTagFilters(inst.Tags, tagFilters) {
			continue
		}
		names = append(names, inst.InstanceName)
	}

	sort.Strings(names)

	return names
}

// matchesTagFilters returns true if the tags satisfy all the given filters.
//
//nolint:gocognit // tag filter matching requires nested condition evaluation
func matchesTagFilters(t *tags.Tags, filters []TagFilter) bool {
	if t == nil {
		return len(filters) == 0
	}

	kv := t.Clone()

	for _, f := range filters {
		switch f.Type {
		case "KEY_ONLY":
			if _, ok := kv[f.Key]; !ok {
				return false
			}
		case "VALUE_ONLY":
			found := false
			for _, v := range kv {
				if v == f.Value {
					found = true

					break
				}
			}
			if !found {
				return false
			}
		default: // EQUALS or empty
			if v, ok := kv[f.Key]; !ok || v != f.Value {
				return false
			}
		}
	}

	return true
}

// BatchGetOnPremisesInstances returns on-premises instance info for the given names.
// Names that do not exist are silently omitted.
func (b *InMemoryBackend) BatchGetOnPremisesInstances(instanceNames []string) []*OnPremisesInstance {
	b.mu.RLock("BatchGetOnPremisesInstances")
	defer b.mu.RUnlock()

	result := make([]*OnPremisesInstance, 0, len(instanceNames))

	for _, name := range instanceNames {
		inst, ok := b.onPremisesInstances.Get(name)
		if !ok {
			continue
		}

		cp := *inst
		result = append(result, &cp)
	}

	return result
}

// AddOnPremisesInstanceInternal adds an on-premises instance directly to the backend.
// Used for test seeding only.
func (b *InMemoryBackend) AddOnPremisesInstanceInternal(inst *OnPremisesInstance) {
	b.mu.Lock("AddOnPremisesInstanceInternal")
	defer b.mu.Unlock()

	inst.Tags = ensureTags(inst.Tags, "codedeploy.onprem."+inst.InstanceName+".tags")

	if inst.RegisterTime.IsZero() {
		inst.RegisterTime = time.Now().UTC()
	}

	b.onPremisesInstances.Put(inst)
}
