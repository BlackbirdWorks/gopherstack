package glue

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func cloneUDF(u *UserDefinedFunction) *UserDefinedFunction {
	cp := *u
	cp.ResourceURIs = make([]ResourceURI, len(u.ResourceURIs))
	copy(cp.ResourceURIs, u.ResourceURIs)
	cp.Tags = maps.Clone(u.Tags)

	return &cp
}

func (b *InMemoryBackend) udfKey(dbName, name string) string {
	return dbName + "|" + name
}

func (b *InMemoryBackend) udfARN(dbName, name string) string {
	return arn.Build("glue", b.region, b.accountID, "userDefinedFunction/"+dbName+"/"+name)
}

func (b *InMemoryBackend) CreateUserDefinedFunction(
	dbName string,
	input UserDefinedFunction,
	tags map[string]string,
) (*UserDefinedFunction, error) {
	b.mu.Lock("CreateUserDefinedFunction")
	defer b.mu.Unlock()

	if !b.databases.Has(dbName) {
		return nil, fmt.Errorf("database %q not found: %w", dbName, ErrNotFound)
	}
	key := b.udfKey(dbName, input.FunctionName)
	if b.udfs.Has(key) {
		return nil, fmt.Errorf(
			"user defined function %q already exists in database %q: %w",
			input.FunctionName,
			dbName,
			ErrAlreadyExists,
		)
	}
	udf := input
	udf.DatabaseName = dbName
	udf.CatalogID = b.accountID
	udf.FunctionARN = b.udfARN(dbName, input.FunctionName)
	udf.CreateTime = float64(time.Now().Unix())
	b.udfs.Put(&udf)
	if len(tags) > 0 {
		_ = b.tagResource(udf.FunctionARN, tags)
	}

	return cloneUDF(&udf), nil
}

func (b *InMemoryBackend) GetUserDefinedFunction(
	dbName, name string,
) (*UserDefinedFunction, error) {
	b.mu.RLock("GetUserDefinedFunction")
	defer b.mu.RUnlock()

	u, ok := b.udfs.Get(b.udfKey(dbName, name))
	if !ok {
		return nil, fmt.Errorf(
			"user defined function %q not found in database %q: %w",
			name,
			dbName,
			ErrNotFound,
		)
	}

	return cloneUDF(u), nil
}

func (b *InMemoryBackend) GetUserDefinedFunctions(dbName string) []*UserDefinedFunction {
	b.mu.RLock("GetUserDefinedFunctions")
	defer b.mu.RUnlock()

	var out []*UserDefinedFunction
	for _, u := range b.udfs.All() {
		if dbName == "" || u.DatabaseName == dbName {
			out = append(out, cloneUDF(u))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FunctionName < out[j].FunctionName })

	return out
}

func (b *InMemoryBackend) UpdateUserDefinedFunction(
	dbName, name string,
	input UserDefinedFunction,
) error {
	b.mu.Lock("UpdateUserDefinedFunction")
	defer b.mu.Unlock()

	key := b.udfKey(dbName, name)
	existing, ok := b.udfs.Get(key)
	if !ok {
		return fmt.Errorf(
			"user defined function %q not found in database %q: %w",
			name,
			dbName,
			ErrNotFound,
		)
	}
	input.DatabaseName = dbName
	input.FunctionName = name
	input.FunctionARN = existing.FunctionARN
	input.CatalogID = existing.CatalogID
	input.CreateTime = existing.CreateTime
	input.Tags = existing.Tags
	b.udfs.Put(&input)

	return nil
}

func (b *InMemoryBackend) DeleteUserDefinedFunction(dbName, name string) error {
	b.mu.Lock("DeleteUserDefinedFunction")
	defer b.mu.Unlock()

	key := b.udfKey(dbName, name)
	if !b.udfs.Has(key) {
		return fmt.Errorf(
			"user defined function %q not found in database %q: %w",
			name,
			dbName,
			ErrNotFound,
		)
	}
	b.udfs.Delete(key)

	return nil
}
