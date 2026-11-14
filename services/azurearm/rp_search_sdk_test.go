package azurearm_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/search/armsearch"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const searchRG = "search-rg"

type fakeSearchPlane struct {
	services map[string]azurearm.SearchServiceConfig
	admin    map[string]azurearm.SearchAdminKeys
	query    map[string][]azurearm.SearchQueryKey
	counter  int
	mu       sync.Mutex
}

func newFakeSearchPlane() *fakeSearchPlane {
	return &fakeSearchPlane{
		services: map[string]azurearm.SearchServiceConfig{}, admin: map[string]azurearm.SearchAdminKeys{},
		query: map[string][]azurearm.SearchQueryKey{},
	}
}

func (f *fakeSearchPlane) Port() int { return 0 }

func (f *fakeSearchPlane) key() string {
	f.counter++

	return fmt.Sprintf("KEY%029d", f.counter)
}

func (f *fakeSearchPlane) PutService(name string, cfg azurearm.SearchServiceConfig) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.services[name]; !ok {
		f.admin[name] = azurearm.SearchAdminKeys{Primary: f.key(), Secondary: f.key()}
		f.query[name] = []azurearm.SearchQueryKey{{Key: f.key()}}
	}

	f.services[name] = cfg

	return time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), nil
}

func (f *fakeSearchPlane) DeleteService(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.services, name)
	delete(f.admin, name)
	delete(f.query, name)

	return nil
}

func (f *fakeSearchPlane) AdminKeys(name string) (azurearm.SearchAdminKeys, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.admin[name], nil
}

func (f *fakeSearchPlane) RegenerateAdminKey(name, kind string) (azurearm.SearchAdminKeys, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	k := f.admin[name]
	if kind == "primary" {
		k.Primary = f.key()
	} else {
		k.Secondary = f.key()
	}

	f.admin[name] = k

	return k, nil
}

func (f *fakeSearchPlane) QueryKeys(name string) ([]azurearm.SearchQueryKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]azurearm.SearchQueryKey(nil), f.query[name]...), nil
}

func (f *fakeSearchPlane) CreateQueryKey(name, keyName string) (azurearm.SearchQueryKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, k := range f.query[name] {
		if k.Name == keyName && keyName != "" {
			return azurearm.SearchQueryKey{}, azurearm.ErrSearchQueryKeyNameTaken
		}
	}

	k := azurearm.SearchQueryKey{Name: keyName, Key: f.key()}
	f.query[name] = append(f.query[name], k)

	return k, nil
}

func (f *fakeSearchPlane) DeleteQueryKey(name, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for i, k := range f.query[name] {
		if k.Key == key {
			f.query[name] = append(f.query[name][:i], f.query[name][i+1:]...)

			return nil
		}
	}

	return azurearm.ErrSearchQueryKeyNotFound
}

type searchStack struct {
	factory *armsearch.ClientFactory
	plane   *fakeSearchPlane
	handler *azurearm.Handler
	url     string
}

func newSearchStack(t *testing.T) *searchStack {
	t.Helper()

	svc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := svc.(*azurearm.Handler)
	require.True(t, ok)

	t.Cleanup(func() { h.Shutdown(context.Background()) })

	plane := newFakeSearchPlane()
	h.Registry.SetSearch(plane)

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	f, err := armsearch.NewClientFactory(aciSub, aciCred{}, &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: srv.URL, Audience: "https://management.core.windows.net/"},
		}},
		Transport: srv.Client(),
		Retry:     policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	return &searchStack{factory: f, plane: plane, handler: h, url: srv.URL}
}

func (s *searchStack) create(t *testing.T, name string, svc armsearch.Service) armsearch.Service {
	t.Helper()

	poller, err := s.factory.NewServicesClient().BeginCreateOrUpdate(t.Context(), searchRG, name, svc, nil, nil)
	require.NoError(t, err)

	res, err := poller.PollUntilDone(t.Context(), nil)
	require.NoError(t, err)

	return res.Service
}

func basicService(sku armsearch.SKUName) armsearch.Service {
	return armsearch.Service{Location: new("westus"), SKU: &armsearch.SKU{Name: &sku}}
}

func TestSearchSDK_ServiceLifecycle(t *testing.T) {
	t.Parallel()

	s := newSearchStack(t)
	svc := basicService(armsearch.SKUNameStandard)
	svc.Tags = map[string]*string{"env": new("test")}
	svc.Properties = &armsearch.ServiceProperties{
		ReplicaCount: new(
			int32(2),
		),
		PartitionCount:      new(int32(3)),
		PublicNetworkAccess: new(armsearch.PublicNetworkAccessEnabled),
		AuthOptions: &armsearch.DataPlaneAuthOptions{AADOrAPIKey: &armsearch.DataPlaneAADOrAPIKeyAuthOption{
			AADAuthFailureMode: new(armsearch.AADAuthFailureModeHttp403),
		}},
	}

	got := s.create(t, "mysearch", svc)
	assert.Equal(t, "Microsoft.Search/searchServices", *got.Type)
	assert.Equal(t, "mysearch", *got.Name)
	assert.Equal(t, "westus", *got.Location)
	assert.Equal(t, armsearch.SKUNameStandard, *got.SKU.Name)
	assert.EqualValues(t, 2, *got.Properties.ReplicaCount)
	assert.EqualValues(t, 3, *got.Properties.PartitionCount)
	assert.Equal(t, armsearch.HostingModeDefault, *got.Properties.HostingMode)
	assert.Equal(t, armsearch.ProvisioningStateSucceeded, *got.Properties.ProvisioningState)
	assert.Equal(t, armsearch.SearchServiceStatusRunning, *got.Properties.Status)
	assert.Equal(t, "test", *got.Tags["env"])
	assert.True(t, strings.HasPrefix(*got.Properties.Endpoint, "https://mysearch.search."), *got.Properties.Endpoint)
	require.NotNil(t, got.Properties.AuthOptions.AADOrAPIKey)
	assert.Equal(t, armsearch.AADAuthFailureModeHttp403, *got.Properties.AuthOptions.AADOrAPIKey.AADAuthFailureMode)
	assert.Equal(t, "aadOrApiKey", s.plane.services["mysearch"].AuthMode, "auth options reach the data plane")
	assert.Equal(t, "http403", s.plane.services["mysearch"].AADFailureMode)

	read, err := s.factory.NewServicesClient().Get(t.Context(), searchRG, "mysearch", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, *got.ID, *read.ID)

	pager := s.factory.NewServicesClient().NewListByResourceGroupPager(searchRG, nil, nil)
	page, err := pager.NextPage(t.Context())
	require.NoError(t, err)
	assert.Len(t, page.Value, 1)

	subPager := s.factory.NewServicesClient().NewListBySubscriptionPager(nil, nil)
	subPage, err := subPager.NextPage(t.Context())
	require.NoError(t, err)
	assert.Len(t, subPage.Value, 1)

	_, err = s.factory.NewServicesClient().Delete(t.Context(), searchRG, "mysearch", nil, nil)
	require.NoError(t, err)

	_, err = s.factory.NewServicesClient().Get(t.Context(), searchRG, "mysearch", nil, nil)
	require.Error(t, err)

	var respErr *azcore.ResponseError

	require.ErrorAs(t, err, &respErr)
	assert.Equal(t, http.StatusNotFound, respErr.StatusCode)
	assert.NotContains(t, s.plane.services, "mysearch", "delete reaches the data plane")
}

func TestSearchSDK_CapacityValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		sku        armsearch.SKUName
		hosting    armsearch.HostingMode
		replicas   int32
		partitions int32
		ok         bool
	}{
		{"free defaults", armsearch.SKUNameFree, "", 1, 1, true},
		{"free with replicas", armsearch.SKUNameFree, "", 2, 1, false},
		{"basic replicas", armsearch.SKUNameBasic, "", 3, 1, true},
		{"basic too many replicas", armsearch.SKUNameBasic, "", 4, 1, false},
		{"basic partitions", armsearch.SKUNameBasic, "", 1, 2, false},
		{"standard valid partitions", armsearch.SKUNameStandard, "", 3, 12, true},
		{"standard invalid partition count", armsearch.SKUNameStandard, "", 1, 5, false},
		{"standard search unit cap", armsearch.SKUNameStandard, "", 12, 12, false},
		{"zero replicas", armsearch.SKUNameStandard, "", 0, 1, false},
		{"high density on s3", armsearch.SKUNameStandard3, armsearch.HostingModeHighDensity, 1, 3, true},
		{"high density partitions", armsearch.SKUNameStandard3, armsearch.HostingModeHighDensity, 1, 4, false},
		{"high density on s1", armsearch.SKUNameStandard, armsearch.HostingModeHighDensity, 1, 1, false},
		{"storage optimized", armsearch.SKUNameStorageOptimizedL1, "", 2, 4, true},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newSearchStack(t)
			svc := basicService(tc.sku)
			svc.Properties = &armsearch.ServiceProperties{
				ReplicaCount:   new(tc.replicas),
				PartitionCount: new(tc.partitions),
			}

			if tc.hosting != "" {
				svc.Properties.HostingMode = new(tc.hosting)
			}

			poller, err := s.factory.NewServicesClient().
				BeginCreateOrUpdate(t.Context(), searchRG, fmt.Sprintf("cap-%d", i), svc, nil, nil)
			if tc.ok {
				require.NoError(t, err)

				_, err = poller.PollUntilDone(t.Context(), nil)
				require.NoError(t, err)

				return
			}

			var respErr *azcore.ResponseError

			require.ErrorAs(t, err, &respErr)
			assert.Equal(t, http.StatusBadRequest, respErr.StatusCode)
		})
	}
}

func TestSearchSDK_UpdateSemantics(t *testing.T) {
	t.Parallel()

	s := newSearchStack(t)
	s.create(t, "upd", basicService(armsearch.SKUNameStandard))

	updated, err := s.factory.NewServicesClient().Update(t.Context(), searchRG, "upd", armsearch.ServiceUpdate{
		Properties: &armsearch.ServiceProperties{ReplicaCount: new(int32(4))},
		Tags:       map[string]*string{"k": new("v")},
	}, nil, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 4, *updated.Properties.ReplicaCount)
	assert.EqualValues(t, 1, *updated.Properties.PartitionCount, "unspecified properties are kept")
	assert.Equal(t, armsearch.SKUNameStandard, *updated.SKU.Name)
	assert.Equal(t, 4, s.plane.services["upd"].ReplicaCount, "capacity changes reach the data plane")

	_, err = s.factory.NewServicesClient().Update(t.Context(), searchRG, "upd", armsearch.ServiceUpdate{
		SKU: &armsearch.SKU{Name: new(armsearch.SKUNameBasic)},
	}, nil, nil)

	var respErr *azcore.ResponseError

	require.ErrorAs(t, err, &respErr)
	assert.Equal(t, http.StatusBadRequest, respErr.StatusCode, "the SKU of a service cannot change")

	_, err = s.factory.NewServicesClient().Update(t.Context(), searchRG, "missing", armsearch.ServiceUpdate{}, nil, nil)
	require.ErrorAs(t, err, &respErr)
	assert.Equal(t, http.StatusNotFound, respErr.StatusCode)

	conflicting := basicService(armsearch.SKUNameStandard)
	conflicting.Properties = &armsearch.ServiceProperties{
		DisableLocalAuth: new(true),
		AuthOptions:      &armsearch.DataPlaneAuthOptions{APIKeyOnly: map[string]any{}},
	}
	_, err = s.factory.NewServicesClient().BeginCreateOrUpdate(t.Context(), searchRG, "conflict", conflicting, nil, nil)
	require.ErrorAs(t, err, &respErr)
	assert.Equal(t, http.StatusBadRequest, respErr.StatusCode, "authOptions cannot combine with disableLocalAuth")
}

func TestSearchSDK_NamesAndUniqueness(t *testing.T) {
	t.Parallel()

	s := newSearchStack(t)
	s.create(t, "taken", basicService(armsearch.SKUNameBasic))

	cases := []struct {
		name   string
		input  string
		reason string
		avail  bool
	}{
		{"free name", "fresh-name", "", true},
		{"taken", "taken", "AlreadyExists", false},
		{"uppercase", "Taken2", "Invalid", false},
		{"too short", "a", "Invalid", false},
		{"leading dash", "-abc", "Invalid", false},
		{"trailing dash", "abc-", "Invalid", false},
		{"double dash", "a--b", "Invalid", false},
		{"too long", strings.Repeat("a", 61), "Invalid", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := s.factory.NewServicesClient().CheckNameAvailability(t.Context(),
				armsearch.CheckNameAvailabilityInput{Name: new(tc.input), Type: new("searchServices")}, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.avail, *out.IsNameAvailable)

			if tc.reason != "" {
				assert.Equal(t, tc.reason, string(*out.Reason))
				assert.NotEmpty(t, *out.Message)
			}
		})
	}

	other := armsearch.Service{Location: new("westus"), SKU: &armsearch.SKU{Name: new(armsearch.SKUNameBasic)}}
	poller, err := s.factory.NewServicesClient().BeginCreateOrUpdate(t.Context(), "other-rg", "taken", other, nil, nil)

	var respErr *azcore.ResponseError

	if err == nil {
		_, err = poller.PollUntilDone(t.Context(), nil)
	}

	require.ErrorAs(t, err, &respErr)
	assert.Equal(t, http.StatusConflict, respErr.StatusCode, "a name is unique across resource groups")

	bad := basicService(armsearch.SKUNameBasic)
	_, err = s.factory.NewServicesClient().BeginCreateOrUpdate(t.Context(), searchRG, "Bad_Name", bad, nil, nil)
	require.ErrorAs(t, err, &respErr)
	assert.Equal(t, http.StatusBadRequest, respErr.StatusCode)
}

func TestSearchSDK_Keys(t *testing.T) {
	t.Parallel()

	s := newSearchStack(t)
	s.create(t, "keys", basicService(armsearch.SKUNameBasic))

	admin, err := s.factory.NewAdminKeysClient().Get(t.Context(), searchRG, "keys", nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, *admin.PrimaryKey)
	assert.NotEqual(t, *admin.PrimaryKey, *admin.SecondaryKey)

	regen, err := s.factory.NewAdminKeysClient().
		Regenerate(t.Context(), searchRG, "keys", armsearch.AdminKeyKindPrimary, nil, nil)
	require.NoError(t, err)
	assert.NotEqual(t, *admin.PrimaryKey, *regen.PrimaryKey, "the primary key changes")
	assert.Equal(t, *admin.SecondaryKey, *regen.SecondaryKey, "the secondary key is untouched")

	created, err := s.factory.NewQueryKeysClient().Create(t.Context(), searchRG, "keys", "reader", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "reader", *created.Name)
	assert.NotEmpty(t, *created.Key)

	pager := s.factory.NewQueryKeysClient().NewListBySearchServicePager(searchRG, "keys", nil, nil)
	page, err := pager.NextPage(t.Context())
	require.NoError(t, err)
	require.Len(t, page.Value, 2, "the default unnamed key plus the new one")

	var named []string

	for _, k := range page.Value {
		if k.Name != nil {
			named = append(named, *k.Name)
		}
	}

	assert.Contains(t, named, "reader")

	_, err = s.factory.NewQueryKeysClient().Create(t.Context(), searchRG, "keys", "reader", nil, nil)

	var respErr *azcore.ResponseError

	require.ErrorAs(t, err, &respErr)
	assert.Equal(t, http.StatusConflict, respErr.StatusCode)

	_, err = s.factory.NewQueryKeysClient().Delete(t.Context(), searchRG, "keys", *created.Key, nil, nil)
	require.NoError(t, err)

	_, err = s.factory.NewQueryKeysClient().Delete(t.Context(), searchRG, "keys", *created.Key, nil, nil)
	require.NoError(t, err, "armsearch accepts 404 for an already deleted key")
	assert.Len(t, s.plane.query["keys"], 1, "only the default key remains")

	_, err = s.factory.NewAdminKeysClient().Get(t.Context(), searchRG, "ghost", nil, nil)
	require.ErrorAs(t, err, &respErr)
	assert.Equal(t, http.StatusNotFound, respErr.StatusCode)
}

func TestSearchSDK_Usages(t *testing.T) {
	t.Parallel()

	s := newSearchStack(t)
	s.create(t, "u1", basicService(armsearch.SKUNameBasic))

	pager := s.factory.NewUsagesClient().NewListBySubscriptionPager("westus", nil, nil)
	page, err := pager.NextPage(t.Context())
	require.NoError(t, err)

	byName := map[string]*armsearch.QuotaUsageResult{}
	for _, u := range page.Value {
		byName[*u.Name.Value] = u
	}

	require.Contains(t, byName, "basic")
	assert.EqualValues(t, 1, *byName["basic"].CurrentValue)
	assert.EqualValues(t, 0, *byName["free"].CurrentValue)
	assert.Equal(t, "Count", *byName["basic"].Unit)

	one, err := s.factory.NewManagementClient().UsageBySubscriptionSKU(t.Context(), "westus", "basic", nil, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 1, *one.CurrentValue)
}

func TestSearchSDK_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	s := newSearchStack(t)
	svc := basicService(armsearch.SKUNameStandard)
	svc.Properties = &armsearch.ServiceProperties{ReplicaCount: new(int32(2)), PartitionCount: new(int32(2))}
	s.create(t, "persist", svc)

	snap := s.handler.Snapshot(t.Context())

	fresh := newSearchStack(t)
	require.NoError(t, fresh.handler.Restore(t.Context(), snap))

	got, err := fresh.factory.NewServicesClient().Get(t.Context(), searchRG, "persist", nil, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 2, *got.Properties.ReplicaCount)
	assert.EqualValues(t, 2, *got.Properties.PartitionCount)
	assert.Equal(t, armsearch.SKUNameStandard, *got.SKU.Name)

	fresh.handler.Reset()

	_, err = fresh.factory.NewServicesClient().Get(t.Context(), searchRG, "persist", nil, nil)
	require.Error(t, err)
}

func TestSearchSDK_UnavailableDataPlane(t *testing.T) {
	t.Parallel()

	svc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := svc.(*azurearm.Handler)
	require.True(t, ok)

	t.Cleanup(func() { h.Shutdown(context.Background()) })

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	f, err := armsearch.NewClientFactory(aciSub, aciCred{}, &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: srv.URL, Audience: "https://management.core.windows.net/"},
		}},
		Transport: srv.Client(), Retry: policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	_, err = f.NewServicesClient().
		BeginCreateOrUpdate(t.Context(), searchRG, "nodp", basicService(armsearch.SKUNameBasic), nil, nil)

	var respErr *azcore.ResponseError

	require.ErrorAs(t, err, &respErr)
	assert.Equal(
		t,
		http.StatusServiceUnavailable,
		respErr.StatusCode,
		"without a data plane nothing pretends to be created",
	)
}
