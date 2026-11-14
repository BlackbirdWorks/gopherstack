package app

import (
	"encoding/base64"
	"errors"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/odatatable"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azureblob"
	"github.com/blackbirdworks/gopherstack/services/azuresearch"
	"github.com/blackbirdworks/gopherstack/services/azuretable"
	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

// wireAzureSearch gives ARM's Microsoft.Search provider the Azure AI Search data plane and gives the
// data plane the in-process Blob Storage and Cosmos DB emulators as indexer sources.
func wireAzureSearch(byName map[string]service.Registerable) {
	searchH, ok := byName["AzureSearch"].(*azuresearch.Handler)
	if !ok {
		return
	}

	if armH, armOk := byName["AzureARM"].(*azurearm.Handler); armOk {
		armH.Registry.SetSearch(&azureSearchAdapter{b: searchH.Backend, port: searchH.Port})
	}

	if blobH, blobOk := byName["AzureBlob"].(*azureblob.Handler); blobOk {
		searchH.Backend.SetBlobSource(azureSearchBlobs{b: blobH.Backend})
	}

	if tableH, tableOk := byName["AzureTable"].(*azuretable.Handler); tableOk {
		searchH.Backend.SetTableSource(azureSearchTables{b: tableH.Backend})
	}

	if cosmosH, cosmosOk := byName["CosmosDB"].(*cosmosdb.Handler); cosmosOk {
		searchH.Backend.SetCosmosSource(azureSearchCosmos{b: cosmosH.Backend})
	}
}

// azureSearchBlobs exposes the Blob Storage emulator to the search indexers.
type azureSearchBlobs struct {
	b azureblob.StorageBackend
}

func (s azureSearchBlobs) ListBlobs(container string) ([]azuresearch.BlobEntry, error) {
	var out []azuresearch.BlobEntry

	marker := ""

	for {
		page, err := s.b.ListBlobsPage(container, azureblob.ListBlobsOptions{Marker: marker})
		if err != nil {
			return nil, err
		}

		for _, e := range page.Entries {
			if e.IsPrefix {
				continue
			}

			out = append(out, azuresearch.BlobEntry{
				Name: e.Blob.Name, ContentType: e.Blob.ContentType, ETag: e.Blob.ETag, MD5: e.Blob.ContentMD5,
				Size: e.Blob.ContentLength, LastModified: e.Blob.LastModified, Metadata: e.Blob.Metadata,
			})
		}

		if page.NextMarker == "" {
			return out, nil
		}

		marker = page.NextMarker
	}
}

func (s azureSearchBlobs) ReadBlob(container, name string) ([]byte, error) {
	_, data, err := s.b.GetBlobAt(container, name, "")

	return data, err
}

// azureSearchCosmos exposes the Cosmos DB emulator to the search indexers.
type azureSearchCosmos struct {
	b cosmosdb.StorageBackend
}

func (s azureSearchCosmos) ListDocuments(database, container string) ([]azuresearch.CosmosDoc, error) {
	docs, err := s.b.ListDocuments(database, container)
	if err != nil {
		return nil, err
	}

	out := make([]azuresearch.CosmosDoc, 0, len(docs))
	for _, d := range docs {
		out = append(out, azuresearch.CosmosDoc{ID: d.ID, ETag: d.ETag, Timestamp: d.Timestamp, Body: d.Body})
	}

	return out, nil
}

// azureSearchTables exposes the Table Storage emulator to the search indexers.
type azureSearchTables struct {
	b azuretable.StorageBackend
}

func (s azureSearchTables) ListEntities(table, filter string) ([]azuresearch.TableEntity, error) {
	var node odatatable.Node

	if filter != "" {
		n, err := odatatable.ParseFilter(filter)
		if err != nil {
			return nil, err
		}

		node = n
	}

	ents, err := s.b.QueryEntities(table, node, 0)
	if err != nil {
		return nil, err
	}

	out := make([]azuresearch.TableEntity, 0, len(ents))

	for _, e := range ents {
		props := make(map[string]any, len(e.Properties))
		for k, p := range e.Properties {
			props[k] = tableValue(p)
		}

		out = append(out, azuresearch.TableEntity{
			PartitionKey: e.PartitionKey, RowKey: e.RowKey, ETag: e.ETag, Timestamp: e.Timestamp, Properties: props,
		})
	}

	return out, nil
}

func tableValue(p odatatable.EntityProperty) any {
	switch v := p.Value.(type) {
	case int:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano)
	case []byte:
		return base64.StdEncoding.EncodeToString(v)
	}

	return p.Value
}

// azureSearchAdapter adapts services/azuresearch's backend to services/azurearm.SearchPlane.
type azureSearchAdapter struct {
	b    *azuresearch.InMemoryBackend
	port int
}

func (a *azureSearchAdapter) Port() int { return a.port }

var _ azurearm.SearchPlane = (*azureSearchAdapter)(nil)

func (a *azureSearchAdapter) PutService(name string, cfg azurearm.SearchServiceConfig) (time.Time, error) {
	return a.b.PutService(name, azuresearch.ServiceConfig(cfg))
}

func (a *azureSearchAdapter) DeleteService(name string) error {
	return ignoreMissing(a.b.DeleteService(name), azuresearch.ErrServiceNotFound)
}

func searchKeysOf(k azuresearch.AdminKeys, err error) (azurearm.SearchAdminKeys, error) {
	return azurearm.SearchAdminKeys{Primary: k.Primary, Secondary: k.Secondary}, err
}

func (a *azureSearchAdapter) AdminKeys(name string) (azurearm.SearchAdminKeys, error) {
	return searchKeysOf(a.b.Keys(name))
}

func (a *azureSearchAdapter) RegenerateAdminKey(name, kind string) (azurearm.SearchAdminKeys, error) {
	return searchKeysOf(a.b.RegenerateAdminKey(name, kind))
}

func (a *azureSearchAdapter) QueryKeys(name string) ([]azurearm.SearchQueryKey, error) {
	keys, err := a.b.ListQueryKeys(name)
	if err != nil {
		return nil, err
	}

	out := make([]azurearm.SearchQueryKey, 0, len(keys))
	for _, k := range keys {
		out = append(out, azurearm.SearchQueryKey(k))
	}

	return out, nil
}

func (a *azureSearchAdapter) CreateQueryKey(name, keyName string) (azurearm.SearchQueryKey, error) {
	k, err := a.b.CreateQueryKey(name, keyName)
	if errors.Is(err, azuresearch.ErrQueryKeyNameTaken) {
		return azurearm.SearchQueryKey{}, azurearm.ErrSearchQueryKeyNameTaken
	}

	return azurearm.SearchQueryKey(k), err
}

func (a *azureSearchAdapter) DeleteQueryKey(name, key string) error {
	if err := a.b.DeleteQueryKey(name, key); errors.Is(err, azuresearch.ErrQueryKeyNotFound) {
		return azurearm.ErrSearchQueryKeyNotFound
	} else if err != nil {
		return err
	}

	return nil
}
