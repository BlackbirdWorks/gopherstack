package azurearm

import "time"

// SearchServiceConfig is what ARM tells the data plane about a search service.
type SearchServiceConfig struct {
	SKU                 string
	HostingMode         string
	PublicNetworkAccess string
	AuthMode            string
	AADFailureMode      string
	ReplicaCount        int
	PartitionCount      int
	DisableLocalAuth    bool
}

// SearchQueryKey is one query key of a search service; Name may be empty.
type SearchQueryKey struct {
	Name string
	Key  string
}

// SearchAdminKeys is a service's admin key pair.
type SearchAdminKeys struct {
	Primary   string
	Secondary string
}

// SearchPlane is the seam the Microsoft.Search provider uses to keep services and their keys
// in the Azure AI Search data plane.
type SearchPlane interface {
	PutService(name string, cfg SearchServiceConfig) (time.Time, error)
	DeleteService(name string) error
	AdminKeys(name string) (SearchAdminKeys, error)
	RegenerateAdminKey(name, kind string) (SearchAdminKeys, error)
	QueryKeys(name string) ([]SearchQueryKey, error)
	CreateQueryKey(name, keyName string) (SearchQueryKey, error)
	DeleteQueryKey(name, key string) error
	Port() int
}

type noopSearchPlane struct{}

func (noopSearchPlane) PutService(string, SearchServiceConfig) (time.Time, error) {
	return time.Time{}, ErrSearchUnavailable
}
func (noopSearchPlane) DeleteService(string) error { return nil }
func (noopSearchPlane) Port() int                  { return 0 }
func (noopSearchPlane) AdminKeys(string) (SearchAdminKeys, error) {
	return SearchAdminKeys{}, ErrSearchUnavailable
}

func (noopSearchPlane) RegenerateAdminKey(string, string) (SearchAdminKeys, error) {
	return SearchAdminKeys{}, ErrSearchUnavailable
}

func (noopSearchPlane) QueryKeys(string) ([]SearchQueryKey, error) {
	return nil, ErrSearchUnavailable
}

func (noopSearchPlane) CreateQueryKey(string, string) (SearchQueryKey, error) {
	return SearchQueryKey{}, ErrSearchUnavailable
}
func (noopSearchPlane) DeleteQueryKey(string, string) error { return ErrSearchUnavailable }

var _ SearchPlane = noopSearchPlane{}
