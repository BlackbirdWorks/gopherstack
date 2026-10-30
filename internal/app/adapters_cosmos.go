package app

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

// containerReplacer is implemented by backends that support updating an existing container.
type containerReplacer interface {
	ReplaceContainer(dbID string, spec cosmosdb.ContainerSpec) (cosmosdb.ContainerInfo, error)
}

// azureCosmosAdapter adapts services/cosmosdb to services/azurearm.CosmosResources.
type azureCosmosAdapter struct {
	handler *cosmosdb.Handler
}

var (
	_ azurearm.CosmosResources       = (*azureCosmosAdapter)(nil)
	_ azurearm.CosmosPolicyResources = (*azureCosmosAdapter)(nil)
)

func (a *azureCosmosAdapter) Endpoint() (int, bool) { return a.handler.Port, a.handler.TLS }
func (a *azureCosmosAdapter) MasterKey() string     { return a.handler.MasterKey }
func (a *azureCosmosAdapter) AcceptKey(key string)  { a.handler.AcceptKey(key) }
func (a *azureCosmosAdapter) RevokeKey(key string)  { a.handler.RevokeKey(key) }

func (a *azureCosmosAdapter) SetRegions(regions []azurearm.CosmosRegion, multiWrite bool) {
	setter, ok := a.handler.Backend.(interface{ SetAccountRegions(cosmosdb.AccountRegions) })
	if !ok {
		return
	}

	out := cosmosdb.AccountRegions{MultiWrite: multiWrite}
	for _, r := range regions {
		out.Regions = append(out.Regions, cosmosdb.AccountRegion{Name: r.Name, FailoverPriority: r.Priority})
	}

	setter.SetAccountRegions(out)
}

func (a *azureCosmosAdapter) SetAccountPolicies(p azurearm.CosmosAccountPolicies) {
	setter, ok := a.handler.Backend.(interface {
		SetAccountPolicies(cosmosdb.AccountPolicies)
	})
	if !ok {
		return
	}

	setter.SetAccountPolicies(cosmosdb.AccountPolicies(p))
}

func cosmosErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, cosmosdb.ErrDatabaseNotFound):
		return fmt.Errorf("%w: %w", azurearm.ErrCosmosDatabaseNotFound, err)
	case errors.Is(err, cosmosdb.ErrContainerNotFound):
		return fmt.Errorf("%w: %w", azurearm.ErrCosmosContainerNotFound, err)
	case errors.Is(err, cosmosdb.ErrThroughputNotFound):
		return fmt.Errorf("%w: %w", azurearm.ErrCosmosThroughputNotFound, err)
	case errors.Is(err, cosmosdb.ErrDatabaseAlreadyExists), errors.Is(err, cosmosdb.ErrContainerAlreadyExists),
		errors.Is(err, cosmosdb.ErrPartitionKeyImmutable), errors.Is(err, cosmosdb.ErrUniqueKeyPolicyImmutable):
		return fmt.Errorf("%w: %w", azurearm.ErrCosmosConflict, err)
	}

	return fmt.Errorf("%w: %w", azurearm.ErrCosmosBadRequest, err)
}

func armDatabase(d cosmosdb.DatabaseInfo) azurearm.CosmosDatabase {
	return azurearm.CosmosDatabase{ID: d.ID, RID: d.RID, Modified: d.Modified}
}

func (a *azureCosmosAdapter) CreateDatabase(id string) (azurearm.CosmosDatabase, error) {
	d, err := a.handler.Backend.CreateDatabase(id)

	return armDatabase(d), cosmosErr(err)
}

func (a *azureCosmosAdapter) GetDatabase(id string) (azurearm.CosmosDatabase, error) {
	d, err := a.handler.Backend.GetDatabase(id)

	return armDatabase(d), cosmosErr(err)
}

func (a *azureCosmosAdapter) ListDatabases() []azurearm.CosmosDatabase {
	dbs := a.handler.Backend.ListDatabases()
	out := make([]azurearm.CosmosDatabase, 0, len(dbs))

	for _, d := range dbs {
		out = append(out, armDatabase(d))
	}

	return out
}

func (a *azureCosmosAdapter) DeleteDatabase(id string) error {
	return cosmosErr(a.handler.Backend.DeleteDatabase(id))
}

func containerSpec(c azurearm.CosmosContainer) cosmosdb.ContainerSpec {
	return cosmosdb.ContainerSpec{
		ID: c.ID, PartitionKeyPaths: c.PartitionKeyPaths, DefaultTTL: c.DefaultTTL,
		IndexingPolicy:           json.RawMessage(c.IndexingPolicy),
		ConflictResolutionPolicy: json.RawMessage(c.ConflictResolutionPolicy),
		UniqueKeyPolicy:          json.RawMessage(c.UniqueKeyPolicy),
	}
}

func armContainer(c cosmosdb.ContainerInfo) azurearm.CosmosContainer {
	return azurearm.CosmosContainer{
		ID: c.ID, RID: c.RID, Modified: c.Modified, DefaultTTL: c.DefaultTTL,
		PartitionKeyPaths:        containerPaths(c),
		IndexingPolicy:           c.IndexingPolicy,
		UniqueKeyPolicy:          c.UniqueKeyPolicy,
		ConflictResolutionPolicy: c.ConflictResolutionPolicy,
	}
}

func containerPaths(c cosmosdb.ContainerInfo) []string {
	if len(c.PartitionKeyPaths) > 0 {
		return c.PartitionKeyPaths
	}

	return []string{c.PartitionKeyPath}
}

func (a *azureCosmosAdapter) PutContainer(db string, c azurearm.CosmosContainer) (azurearm.CosmosContainer, error) {
	spec := containerSpec(c)

	if _, err := a.handler.Backend.GetContainer(db, c.ID); err == nil {
		if r, ok := a.handler.Backend.(containerReplacer); ok {
			info, rErr := r.ReplaceContainer(db, spec)

			return armContainer(info), cosmosErr(rErr)
		}
	}

	info, err := a.handler.Backend.CreateContainer(db, spec)

	return armContainer(info), cosmosErr(err)
}

func (a *azureCosmosAdapter) GetContainer(db, id string) (azurearm.CosmosContainer, error) {
	c, err := a.handler.Backend.GetContainer(db, id)

	return armContainer(c), cosmosErr(err)
}

func (a *azureCosmosAdapter) ListContainers(db string) ([]azurearm.CosmosContainer, error) {
	cs, err := a.handler.Backend.ListContainers(db)
	if err != nil {
		return nil, cosmosErr(err)
	}

	out := make([]azurearm.CosmosContainer, 0, len(cs))
	for _, c := range cs {
		out = append(out, armContainer(c))
	}

	return out, nil
}

func (a *azureCosmosAdapter) DeleteContainer(db, id string) error {
	return cosmosErr(a.handler.Backend.DeleteContainer(db, id))
}

func (a *azureCosmosAdapter) throughputBackend() (cosmosdb.ThroughputBackend, error) {
	tb, ok := a.handler.Backend.(cosmosdb.ThroughputBackend)
	if !ok {
		return nil, azurearm.ErrCosmosUnavailable
	}

	return tb, nil
}

func (a *azureCosmosAdapter) GetThroughput(db, container string) (azurearm.CosmosThroughput, error) {
	tb, err := a.throughputBackend()
	if err != nil {
		return azurearm.CosmosThroughput{}, err
	}

	info, err := tb.GetThroughput(db, container)
	if err != nil {
		return azurearm.CosmosThroughput{}, cosmosErr(err)
	}

	return azurearm.CosmosThroughput{Throughput: info.Manual, AutoscaleMax: info.AutoscaleMax}, nil
}

func (a *azureCosmosAdapter) PutThroughput(db, container string, t azurearm.CosmosThroughput) error {
	tb, err := a.throughputBackend()
	if err != nil {
		return err
	}

	spec := cosmosdb.ThroughputSpec{Manual: t.Throughput, AutoscaleMax: t.AutoscaleMax}
	_, err = tb.SetThroughput(db, container, spec)

	return cosmosErr(err)
}
