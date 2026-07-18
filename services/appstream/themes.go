package appstream

import "time"

type storedTheme struct {
	CreatedTime time.Time `json:"createdTime"`
	StackName   string    `json:"stackName"`
	State       string    `json:"state"`
}

func (t *storedTheme) toTheme() *Theme {
	return &Theme{
		CreatedTime: t.CreatedTime,
		StackName:   t.StackName,
		State:       t.State,
	}
}

// CreateThemeForStack creates a theme for a stack.
func (b *InMemoryBackend) CreateThemeForStack(stackName string) (*Theme, error) {
	b.mu.Lock("CreateThemeForStack")
	defer b.mu.Unlock()

	if !b.stacks.Has(stackName) {
		return nil, ErrNotFound
	}

	if b.themes.Has(stackName) {
		return nil, ErrAlreadyExists
	}

	th := &storedTheme{
		CreatedTime: time.Now().UTC(),
		StackName:   stackName,
		State:       "ENABLED",
	}
	b.themes.Put(th)

	return th.toTheme(), nil
}

// DeleteThemeForStack removes a stack theme.
func (b *InMemoryBackend) DeleteThemeForStack(stackName string) error {
	b.mu.Lock("DeleteThemeForStack")
	defer b.mu.Unlock()

	if !b.themes.Has(stackName) {
		return ErrNotFound
	}

	b.themes.Delete(stackName)

	return nil
}

// DescribeThemeForStack returns the theme for a stack.
func (b *InMemoryBackend) DescribeThemeForStack(stackName string) (*Theme, error) {
	b.mu.RLock("DescribeThemeForStack")
	defer b.mu.RUnlock()

	th, ok := b.themes.Get(stackName)
	if !ok {
		return nil, ErrNotFound
	}

	return th.toTheme(), nil
}

// UpdateThemeForStack updates the theme for a stack.
func (b *InMemoryBackend) UpdateThemeForStack(stackName string) (*Theme, error) {
	b.mu.Lock("UpdateThemeForStack")
	defer b.mu.Unlock()

	th, ok := b.themes.Get(stackName)
	if !ok {
		return nil, ErrNotFound
	}

	return th.toTheme(), nil
}
