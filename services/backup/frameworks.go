package backup

import (
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateFramework creates an audit framework.
func (b *InMemoryBackend) CreateFramework(
	name, description string,
	controls []FrameworkControl,
) (*Framework, error) {
	b.mu.Lock("CreateFramework")
	defer b.mu.Unlock()

	if b.frameworks.Has(name) {
		return nil, fmt.Errorf("%w: framework %s already exists", ErrAlreadyExists, name)
	}

	frameworkARN := arn.Build("backup", b.region, b.accountID, "framework:"+name)
	t := tags.New("backup.framework." + name + ".tags")
	f := &Framework{
		FrameworkName:        name,
		FrameworkArn:         frameworkARN,
		FrameworkDescription: description,
		FrameworkControls:    controls,
		FrameworkStatus:      "ACTIVE",
		DeploymentStatus:     "COMPLETED",
		CreationTime:         time.Now().UTC(),
		Tags:                 t,
	}
	b.frameworks.Put(f)
	b.frameworkARNIndex[frameworkARN] = name
	cp := *f

	return &cp, nil
}

// DescribeFramework returns a framework by name.
func (b *InMemoryBackend) DescribeFramework(name string) (*Framework, error) {
	b.mu.RLock("DescribeFramework")
	defer b.mu.RUnlock()

	f, ok := b.frameworks.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: framework %s not found", ErrNotFound, name)
	}

	cp := *f

	return &cp, nil
}

// ListFrameworks returns all frameworks.
func (b *InMemoryBackend) ListFrameworks() []*Framework {
	b.mu.RLock("ListFrameworks")
	defer b.mu.RUnlock()

	all := b.frameworks.All()
	list := make([]*Framework, 0, len(all))
	for _, f := range all {
		cp := *f
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *Framework) int {
		if a.FrameworkName < b.FrameworkName {
			return -1
		}
		if a.FrameworkName > b.FrameworkName {
			return 1
		}

		return 0
	})

	return list
}

// UpdateFramework updates a framework's description.
func (b *InMemoryBackend) UpdateFramework(name, description string) (*Framework, error) {
	b.mu.Lock("UpdateFramework")
	defer b.mu.Unlock()

	f, ok := b.frameworks.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: framework %s not found", ErrNotFound, name)
	}

	f.FrameworkDescription = description
	cp := *f

	return &cp, nil
}

// DeleteFramework deletes a framework.
func (b *InMemoryBackend) DeleteFramework(name string) error {
	b.mu.Lock("DeleteFramework")
	defer b.mu.Unlock()

	f, ok := b.frameworks.Get(name)
	if !ok {
		return fmt.Errorf("%w: framework %s not found", ErrNotFound, name)
	}

	delete(b.frameworkARNIndex, f.FrameworkArn)
	b.frameworks.Delete(name)
	f.Tags.Close()

	return nil
}

// --- Report Plan read/update/delete methods ---
