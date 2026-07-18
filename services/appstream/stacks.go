package appstream

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

type storedStack struct {
	CreatedTime time.Time         `json:"createdTime"`
	Tags        map[string]string `json:"tags"`
	Name        string            `json:"name"`
	Arn         string            `json:"arn"`
	DisplayName string            `json:"displayName"`
	Description string            `json:"description"`
}

func (s *storedStack) toStack() *Stack {
	tags := make(map[string]string)
	maps.Copy(tags, s.Tags)

	return &Stack{
		CreatedTime: s.CreatedTime,
		Tags:        tags,
		Name:        s.Name,
		Arn:         s.Arn,
		DisplayName: s.DisplayName,
		Description: s.Description,
	}
}

func (b *InMemoryBackend) stackARN(name string) string {
	return arn.Build("appstream", b.region, b.accountID, fmt.Sprintf("stack/%s", name))
}

// CreateStack creates a new stack.
func (b *InMemoryBackend) CreateStack(name, displayName, description string, tags map[string]string) (*Stack, error) {
	b.mu.Lock("CreateStack")
	defer b.mu.Unlock()

	if b.stacks.Has(name) {
		return nil, ErrAlreadyExists
	}

	arn := b.stackARN(name)
	storedTags := make(map[string]string)
	maps.Copy(storedTags, tags)

	s := &storedStack{
		CreatedTime: time.Now().UTC(),
		Tags:        storedTags,
		Name:        name,
		Arn:         arn,
		DisplayName: displayName,
		Description: description,
	}
	b.stacks.Put(s)
	b.tags[arn] = storedTags

	return s.toStack(), nil
}

// DescribeStacks returns stacks, optionally filtered by names.
func (b *InMemoryBackend) DescribeStacks(names []string) ([]*Stack, error) {
	b.mu.RLock("DescribeStacks")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		var result []*Stack

		for _, name := range names {
			s, ok := b.stacks.Get(name)
			if !ok {
				return nil, ErrNotFound
			}

			result = append(result, s.toStack())
		}

		return result, nil
	}

	result := make([]*Stack, 0, b.stacks.Len())
	for _, s := range b.stacks.All() {
		result = append(result, s.toStack())
	}

	return result, nil
}

// UpdateStack updates mutable fields of an existing stack.
func (b *InMemoryBackend) UpdateStack(name, displayName, description string) (*Stack, error) {
	b.mu.Lock("UpdateStack")
	defer b.mu.Unlock()

	s, ok := b.stacks.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	if displayName != "" {
		s.DisplayName = displayName
	}

	if description != "" {
		s.Description = description
	}

	return s.toStack(), nil
}

// DeleteStack removes a stack. Returns ErrResourceInUse if any fleet is associated with the stack.
func (b *InMemoryBackend) DeleteStack(name string) error {
	b.mu.Lock("DeleteStack")
	defer b.mu.Unlock()

	s, ok := b.stacks.Get(name)
	if !ok {
		return ErrNotFound
	}

	for _, stacks := range b.associations {
		if stacks[name] {
			return ErrResourceInUse
		}
	}

	delete(b.tags, s.Arn)
	b.stacks.Delete(name)

	return nil
}
