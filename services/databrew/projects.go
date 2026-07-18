package databrew

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) projectARN(region, name string) string {
	return arn.Build("databrew", region, b.accountID, "project/"+name)
}

func (b *InMemoryBackend) CreateProject(
	ctx context.Context,
	name, datasetName, recipeName, roleArn string,
	sample Sample,
	tags map[string]string,
) (*Project, error) {
	b.mu.Lock("CreateProject")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if name == "" {
		return nil, ErrValidation
	}
	t := b.projectsTable(region)
	if t.Has(name) {
		return nil, ErrAlreadyExists
	}
	if sample.Type != "" && sample.Type != "FIRST_N" && sample.Type != "LAST_N" &&
		sample.Type != "RANDOM" {
		return nil, fmt.Errorf("%w: invalid Sample.Type %q", ErrValidation, sample.Type)
	}
	p := &Project{
		Name: name, Arn: b.projectARN(region, name), DatasetName: datasetName,
		RecipeName: recipeName, RoleArn: roleArn, Sample: sample,
		Tags: maps.Clone(tags), SessionStatus: "READY",
		CreateDate: float64(time.Now().Unix()), LastModifiedDate: float64(time.Now().Unix()),
	}
	t.Put(p)

	return p, nil
}

func (b *InMemoryBackend) DescribeProject(ctx context.Context, name string) (*Project, error) {
	b.mu.RLock("DescribeProject")
	defer b.mu.RUnlock()
	region := getRegion(ctx, b.defaultRegion)
	p, ok := b.projectsTable(region).Get(name)
	if !ok {
		return nil, ErrNotFound
	}
	cp := *p
	cp.Tags = maps.Clone(p.Tags)

	return &cp, nil
}

func (b *InMemoryBackend) ListProjects(
	ctx context.Context,
	maxResults int,
	nextToken string,
) ([]*Project, string) {
	b.mu.RLock("ListProjects")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	t := b.projectsTable(region)
	keys := snapshotKeys(t, projectKeyFn)
	pageKeys, next := paginateKeys(keys, maxResults, nextToken)
	out := make([]*Project, 0, len(pageKeys))
	for _, k := range pageKeys {
		v, _ := t.Get(k)
		cp := *v
		cp.Tags = maps.Clone(v.Tags)
		out = append(out, &cp)
	}

	return out, next
}

func (b *InMemoryBackend) UpdateProject(
	ctx context.Context,
	name, datasetName, roleArn string,
	sample Sample,
) error {
	b.mu.Lock("UpdateProject")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	p, ok := b.projectsTable(region).Get(name)
	if !ok {
		return ErrNotFound
	}
	if sample.Type != "" && sample.Type != "FIRST_N" && sample.Type != "LAST_N" &&
		sample.Type != "RANDOM" {
		return fmt.Errorf("%w: invalid Sample.Type %q", ErrValidation, sample.Type)
	}
	if datasetName != "" {
		p.DatasetName = datasetName
	}
	if roleArn != "" {
		p.RoleArn = roleArn
	}
	p.Sample = sample
	p.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteProject(ctx context.Context, name string) error {
	b.mu.Lock("DeleteProject")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if !b.projectsTable(region).Delete(name) {
		return ErrNotFound
	}

	return nil
}
