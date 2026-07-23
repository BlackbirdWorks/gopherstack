package databrew

import (
	"context"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) datasetARN(region, name string) string {
	return arn.Build("databrew", region, b.accountID, "dataset/"+name)
}

func (b *InMemoryBackend) CreateDataset(
	ctx context.Context,
	name, format string,
	input DatasetInput,
	formatOpts DatasetFormatOptions,
	tags map[string]string,
	pathOptions *PathOptions,
) (*Dataset, error) {
	b.mu.Lock("CreateDataset")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if name == "" {
		return nil, ErrValidation
	}
	t := b.datasetsTable(region)
	if t.Has(name) {
		return nil, ErrAlreadyExists
	}
	source := "S3"
	if input.DataCatalogInputDefinition != nil {
		source = "DATA_CATALOG"
	} else if input.DatabaseInputDefinition != nil {
		source = "DATABASE"
	}
	ds := &Dataset{
		Name: name, Arn: b.datasetARN(region, name), Format: format,
		Input: input, FormatOptions: formatOpts, Tags: maps.Clone(tags),
		PathOptions: pathOptions, AccountID: b.accountID,
		Source: source, CreateDate: float64(time.Now().Unix()),
		LastModifiedDate: float64(time.Now().Unix()),
	}
	t.Put(ds)

	return ds, nil
}

func (b *InMemoryBackend) DescribeDataset(ctx context.Context, name string) (*Dataset, error) {
	b.mu.RLock("DescribeDataset")
	defer b.mu.RUnlock()
	region := getRegion(ctx, b.defaultRegion)
	ds, ok := b.datasetsTable(region).Get(name)
	if !ok {
		return nil, ErrNotFound
	}
	cp := *ds
	cp.Tags = maps.Clone(ds.Tags)

	return &cp, nil
}

func (b *InMemoryBackend) ListDatasets(
	ctx context.Context,
	maxResults int,
	nextToken string,
) ([]*Dataset, string) {
	b.mu.RLock("ListDatasets")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	t := b.datasetsTable(region)
	keys := snapshotKeys(t, datasetKeyFn)
	pageKeys, next := paginateKeys(keys, maxResults, nextToken)
	out := make([]*Dataset, 0, len(pageKeys))
	for _, k := range pageKeys {
		v, _ := t.Get(k)
		cp := *v
		cp.Tags = maps.Clone(v.Tags)
		out = append(out, &cp)
	}

	return out, next
}

func (b *InMemoryBackend) UpdateDataset(
	ctx context.Context,
	name, format string,
	input DatasetInput,
	formatOpts DatasetFormatOptions,
	pathOptions *PathOptions,
) error {
	b.mu.Lock("UpdateDataset")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	ds, ok := b.datasetsTable(region).Get(name)
	if !ok {
		return ErrNotFound
	}
	ds.Format = format
	ds.Input = input
	ds.FormatOptions = formatOpts
	ds.PathOptions = pathOptions
	ds.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteDataset(ctx context.Context, name string) error {
	b.mu.Lock("DeleteDataset")
	defer b.mu.Unlock()
	region := getRegion(ctx, b.defaultRegion)
	if !b.datasetsTable(region).Delete(name) {
		return ErrNotFound
	}

	return nil
}
