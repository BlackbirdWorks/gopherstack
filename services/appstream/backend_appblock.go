package appstream

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	appBlockStateActive   = "ACTIVE"
	appBlockStateInactive = "INACTIVE"

	builderStateStopped  = "STOPPED"
	builderStateRunning  = "RUNNING"
	builderStateStarting = "STARTING"
	builderStateStopping = "STOPPING"
)

type storedAppBlock struct {
	CreatedTime time.Time         `json:"createdTime"`
	Tags        map[string]string `json:"tags"`
	Name        string            `json:"name"`
	Arn         string            `json:"arn"`
	Description string            `json:"description"`
	State       string            `json:"state"`
}

func (a *storedAppBlock) toAppBlock() *AppBlock {
	tags := make(map[string]string)
	maps.Copy(tags, a.Tags)

	return &AppBlock{
		CreatedTime: a.CreatedTime,
		Tags:        tags,
		Name:        a.Name,
		Arn:         a.Arn,
		Description: a.Description,
		State:       a.State,
	}
}

type storedAppBlockBuilder struct {
	CreatedTime  time.Time         `json:"createdTime"`
	Tags         map[string]string `json:"tags"`
	Name         string            `json:"name"`
	Arn          string            `json:"arn"`
	Description  string            `json:"description"`
	Platform     string            `json:"platform"`
	InstanceType string            `json:"instanceType"`
	State        string            `json:"state"`
}

func (b *storedAppBlockBuilder) toAppBlockBuilder() *AppBlockBuilder {
	tags := make(map[string]string)
	maps.Copy(tags, b.Tags)

	return &AppBlockBuilder{
		CreatedTime:  b.CreatedTime,
		Tags:         tags,
		Name:         b.Name,
		Arn:          b.Arn,
		Description:  b.Description,
		Platform:     b.Platform,
		InstanceType: b.InstanceType,
		State:        b.State,
	}
}

func (b *InMemoryBackend) appBlockARN(name string) string {
	return arn.Build("appstream", b.region, b.accountID, fmt.Sprintf("app-block/%s", name))
}

func (b *InMemoryBackend) appBlockBuilderARN(name string) string {
	return arn.Build("appstream", b.region, b.accountID, fmt.Sprintf("app-block-builder/%s", name))
}

// CreateAppBlock creates an app block.
func (b *InMemoryBackend) CreateAppBlock(name, description string, tags map[string]string) (*AppBlock, error) {
	b.mu.Lock("CreateAppBlock")
	defer b.mu.Unlock()

	if _, ok := b.appBlocks[name]; ok {
		return nil, ErrAlreadyExists
	}

	arn := b.appBlockARN(name)
	storedTags := make(map[string]string)
	maps.Copy(storedTags, tags)

	ab := &storedAppBlock{
		CreatedTime: time.Now().UTC(),
		Tags:        storedTags,
		Name:        name,
		Arn:         arn,
		Description: description,
		State:       appBlockStateInactive,
	}
	b.appBlocks[name] = ab
	b.tags[arn] = storedTags

	return ab.toAppBlock(), nil
}

// DeleteAppBlock removes an app block.
func (b *InMemoryBackend) DeleteAppBlock(name string) error {
	b.mu.Lock("DeleteAppBlock")
	defer b.mu.Unlock()

	ab, ok := b.appBlocks[name]
	if !ok {
		return ErrNotFound
	}

	delete(b.tags, ab.Arn)
	delete(b.appBlocks, name)

	return nil
}

// DescribeAppBlocks returns app blocks, optionally filtered by name.
func (b *InMemoryBackend) DescribeAppBlocks(names []string) ([]*AppBlock, error) {
	b.mu.RLock("DescribeAppBlocks")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		var result []*AppBlock

		for _, name := range names {
			ab, ok := b.appBlocks[name]
			if !ok {
				return nil, ErrNotFound
			}

			result = append(result, ab.toAppBlock())
		}

		return result, nil
	}

	result := make([]*AppBlock, 0, len(b.appBlocks))
	for _, ab := range b.appBlocks {
		result = append(result, ab.toAppBlock())
	}

	return result, nil
}

// CreateAppBlockBuilder creates an app block builder.
func (b *InMemoryBackend) CreateAppBlockBuilder(
	name, description, platform, instanceType string,
	tags map[string]string,
) (*AppBlockBuilder, error) {
	if instanceType == "" {
		return nil, fmt.Errorf("%w: InstanceType is required", awserr.ErrInvalidParameter)
	}

	b.mu.Lock("CreateAppBlockBuilder")
	defer b.mu.Unlock()

	if _, ok := b.appBlockBuilders[name]; ok {
		return nil, ErrAlreadyExists
	}

	arn := b.appBlockBuilderARN(name)
	storedTags := make(map[string]string)
	maps.Copy(storedTags, tags)

	bb := &storedAppBlockBuilder{
		CreatedTime:  time.Now().UTC(),
		Tags:         storedTags,
		Name:         name,
		Arn:          arn,
		Description:  description,
		Platform:     platform,
		InstanceType: instanceType,
		State:        builderStateStopped,
	}
	b.appBlockBuilders[name] = bb
	b.tags[arn] = storedTags

	return bb.toAppBlockBuilder(), nil
}

// DeleteAppBlockBuilder removes an app block builder.
func (b *InMemoryBackend) DeleteAppBlockBuilder(name string) error {
	b.mu.Lock("DeleteAppBlockBuilder")
	defer b.mu.Unlock()

	bb, ok := b.appBlockBuilders[name]
	if !ok {
		return ErrNotFound
	}

	if bb.State == builderStateRunning {
		return ErrFleetNotStopped
	}

	delete(b.tags, bb.Arn)
	delete(b.appBlockBuilders, name)
	delete(b.appBlockBuilderAssoc, name)

	return nil
}

// DescribeAppBlockBuilders returns builders, optionally filtered by name.
func (b *InMemoryBackend) DescribeAppBlockBuilders(names []string) ([]*AppBlockBuilder, error) {
	b.mu.RLock("DescribeAppBlockBuilders")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		var result []*AppBlockBuilder

		for _, name := range names {
			bb, ok := b.appBlockBuilders[name]
			if !ok {
				return nil, ErrNotFound
			}

			result = append(result, bb.toAppBlockBuilder())
		}

		return result, nil
	}

	result := make([]*AppBlockBuilder, 0, len(b.appBlockBuilders))
	for _, bb := range b.appBlockBuilders {
		result = append(result, bb.toAppBlockBuilder())
	}

	return result, nil
}

// StartAppBlockBuilder transitions a builder to RUNNING.
func (b *InMemoryBackend) StartAppBlockBuilder(name string) error {
	b.mu.Lock("StartAppBlockBuilder")
	defer b.mu.Unlock()

	bb, ok := b.appBlockBuilders[name]
	if !ok {
		return ErrNotFound
	}

	if bb.State == builderStateRunning {
		return ErrFleetNotStopped
	}

	bb.State = builderStateRunning

	return nil
}

// StopAppBlockBuilder transitions a builder to STOPPED.
func (b *InMemoryBackend) StopAppBlockBuilder(name string) error {
	b.mu.Lock("StopAppBlockBuilder")
	defer b.mu.Unlock()

	bb, ok := b.appBlockBuilders[name]
	if !ok {
		return ErrNotFound
	}

	if bb.State == builderStateStopped {
		return ErrFleetNotStopped
	}

	bb.State = builderStateStopped

	return nil
}

// UpdateAppBlockBuilder updates mutable builder fields.
func (b *InMemoryBackend) UpdateAppBlockBuilder(name, description, instanceType string) (*AppBlockBuilder, error) {
	b.mu.Lock("UpdateAppBlockBuilder")
	defer b.mu.Unlock()

	bb, ok := b.appBlockBuilders[name]
	if !ok {
		return nil, ErrNotFound
	}

	if description != "" {
		bb.Description = description
	}

	if instanceType != "" {
		bb.InstanceType = instanceType
	}

	return bb.toAppBlockBuilder(), nil
}

// CreateAppBlockBuilderStreamingURL returns a streaming URL for the builder.
func (b *InMemoryBackend) CreateAppBlockBuilderStreamingURL(name string) (string, error) {
	b.mu.RLock("CreateAppBlockBuilderStreamingURL")
	defer b.mu.RUnlock()

	if _, ok := b.appBlockBuilders[name]; !ok {
		return "", ErrNotFound
	}

	return fmt.Sprintf("https://appstream2.%s.aws.amazon.com/authenticate?param=builder-%s", b.region, name), nil
}

// AssociateAppBlockBuilderAppBlock links a builder to an app block.
func (b *InMemoryBackend) AssociateAppBlockBuilderAppBlock(builderName, appBlockName string) error {
	b.mu.Lock("AssociateAppBlockBuilderAppBlock")
	defer b.mu.Unlock()

	if _, ok := b.appBlockBuilders[builderName]; !ok {
		return ErrNotFound
	}

	if _, ok := b.appBlocks[appBlockName]; !ok {
		return ErrNotFound
	}

	if b.appBlockBuilderAssoc[builderName] == nil {
		b.appBlockBuilderAssoc[builderName] = make(map[string]bool)
	}

	b.appBlockBuilderAssoc[builderName][appBlockName] = true

	return nil
}

// DisassociateAppBlockBuilderAppBlock removes a builder-appblock link.
func (b *InMemoryBackend) DisassociateAppBlockBuilderAppBlock(builderName, appBlockName string) error {
	b.mu.Lock("DisassociateAppBlockBuilderAppBlock")
	defer b.mu.Unlock()

	if _, ok := b.appBlockBuilders[builderName]; !ok {
		return ErrNotFound
	}

	if _, ok := b.appBlocks[appBlockName]; !ok {
		return ErrNotFound
	}

	if b.appBlockBuilderAssoc[builderName] != nil {
		delete(b.appBlockBuilderAssoc[builderName], appBlockName)
	}

	return nil
}

// DescribeAppBlockBuilderAppBlockAssociations lists builder-appblock associations.
func (b *InMemoryBackend) DescribeAppBlockBuilderAppBlockAssociations(
	builderName, appBlockName string,
) ([]*AppBlockBuilderAppBlockAssociation, error) {
	b.mu.RLock("DescribeAppBlockBuilderAppBlockAssociations")
	defer b.mu.RUnlock()

	var result []*AppBlockBuilderAppBlockAssociation

	for bName, appBlocks := range b.appBlockBuilderAssoc {
		if builderName != "" && bName != builderName {
			continue
		}

		for abName := range appBlocks {
			ab, ok := b.appBlocks[abName]
			if !ok {
				continue
			}

			if appBlockName != "" && abName != appBlockName {
				continue
			}

			result = append(result, &AppBlockBuilderAppBlockAssociation{
				AppBlockBuilderName: bName,
				AppBlockArn:         ab.Arn,
				State:               "ASSOCIATED",
			})
		}
	}

	return result, nil
}
