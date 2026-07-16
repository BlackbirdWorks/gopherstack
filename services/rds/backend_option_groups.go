package rds

import (
	"fmt"
	"slices"
)

// CreateOptionGroup creates a new option group.
func (b *InMemoryBackend) CreateOptionGroup(name, engine, majorVersion, description string) (*OptionGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: OptionGroupName must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CreateOptionGroup")
	defer b.mu.Unlock()
	if _, exists := b.optionGroups.Get(name); exists {
		return nil, fmt.Errorf("%w: option group %s already exists", ErrOptionGroupAlreadyExists, name)
	}
	og := &OptionGroup{
		OptionGroupName:        name,
		OptionGroupDescription: description,
		EngineName:             engine,
		MajorEngineVersion:     majorVersion,
		Options:                []OptionGroupOption{},
	}
	b.optionGroups.Put(og)
	cp := *og
	cp.Options = make([]OptionGroupOption, len(og.Options))
	copy(cp.Options, og.Options)

	return &cp, nil
}

// DescribeOptionGroups returns option groups. If name is non-empty, returns only that group.
func (b *InMemoryBackend) DescribeOptionGroups(name string) ([]OptionGroup, error) {
	b.mu.RLock("DescribeOptionGroups")
	defer b.mu.RUnlock()
	if name != "" {
		og, exists := b.optionGroups.Get(name)
		if !exists {
			return nil, fmt.Errorf("%w: option group %s not found", ErrOptionGroupNotFound, name)
		}
		cp := *og
		cp.Options = make([]OptionGroupOption, len(og.Options))
		copy(cp.Options, og.Options)

		return []OptionGroup{cp}, nil
	}
	result := make([]OptionGroup, 0, b.optionGroups.Len())
	for _, og := range b.optionGroups.All() {
		cp := *og
		cp.Options = make([]OptionGroupOption, len(og.Options))
		copy(cp.Options, og.Options)
		result = append(result, cp)
	}
	slices.SortFunc(result, func(a, b OptionGroup) int {
		if a.OptionGroupName < b.OptionGroupName {
			return -1
		}
		if a.OptionGroupName > b.OptionGroupName {
			return 1
		}

		return 0
	})

	return result, nil
}

// DeleteOptionGroup removes the given option group.
func (b *InMemoryBackend) DeleteOptionGroup(name string) error {
	b.mu.Lock("DeleteOptionGroup")
	defer b.mu.Unlock()
	if _, exists := b.optionGroups.Get(name); !exists {
		return fmt.Errorf("%w: option group %s not found", ErrOptionGroupNotFound, name)
	}
	b.optionGroups.Delete(name)
	delete(b.tags, b.rdsARN("og", name))

	return nil
}

// ModifyOptionGroup modifies an option group by adding/removing options.
func (b *InMemoryBackend) ModifyOptionGroup(
	name string,
	optionsToAdd []OptionGroupOption,
	optionsToRemove []string,
) (*OptionGroup, error) {
	b.mu.Lock("ModifyOptionGroup")
	defer b.mu.Unlock()
	og, exists := b.optionGroups.Get(name)
	if !exists {
		return nil, fmt.Errorf("%w: option group %s not found", ErrOptionGroupNotFound, name)
	}
	removeSet := make(map[string]bool, len(optionsToRemove))
	for _, o := range optionsToRemove {
		removeSet[o] = true
	}
	kept := make([]OptionGroupOption, 0, len(og.Options))
	for _, o := range og.Options {
		if !removeSet[o.OptionName] {
			kept = append(kept, o)
		}
	}
	kept = append(kept, optionsToAdd...)
	og.Options = kept
	cp := *og
	cp.Options = make([]OptionGroupOption, len(og.Options))
	copy(cp.Options, og.Options)

	return &cp, nil
}

// CopyOptionGroup creates a copy of the source option group.
func (b *InMemoryBackend) CopyOptionGroup(
	sourceGroupName, targetGroupName, targetDescription string,
) (*OptionGroup, error) {
	if sourceGroupName == "" {
		return nil, fmt.Errorf(
			"%w: SourceOptionGroupIdentifier must not be empty",
			ErrInvalidParameter,
		)
	}
	if targetGroupName == "" {
		return nil, fmt.Errorf(
			"%w: TargetOptionGroupIdentifier must not be empty",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CopyOptionGroup")
	defer b.mu.Unlock()

	src, exists := b.optionGroups.Get(sourceGroupName)
	if !exists {
		return nil, fmt.Errorf("%w: option group %s not found", ErrOptionGroupNotFound, sourceGroupName)
	}

	if _, alreadyExists := b.optionGroups.Get(targetGroupName); alreadyExists {
		return nil, fmt.Errorf(
			"%w: option group %s already exists",
			ErrOptionGroupAlreadyExists,
			targetGroupName,
		)
	}

	if targetDescription == "" {
		targetDescription = src.OptionGroupDescription
	}

	opts := make([]OptionGroupOption, len(src.Options))
	copy(opts, src.Options)

	og := &OptionGroup{
		OptionGroupName:        targetGroupName,
		OptionGroupDescription: targetDescription,
		EngineName:             src.EngineName,
		MajorEngineVersion:     src.MajorEngineVersion,
		Options:                opts,
	}
	b.optionGroups.Put(og)

	cp := *og
	cp.Options = make([]OptionGroupOption, len(og.Options))
	copy(cp.Options, og.Options)

	return &cp, nil
}
