package app

import (
	"context"
	"crypto/tls"
	"net"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/redis/armredis/v3"
	"github.com/labstack/echo/v5"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azureblob"
	"github.com/blackbirdworks/gopherstack/services/azureredis"
)

const (
	redisSDKSub = "00000000-0000-0000-0000-000000000000"
	redisSDKRG  = "sdk-rg"
)

type redisSDKCredential struct{}

func (redisSDKCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "emulator", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

type redisSDKStack struct {
	arm     *httptest.Server
	factory *armredis.ClientFactory
	blobs   *azureblob.InMemoryBackend
}

func newRedisSDKStack(t *testing.T) *redisSDKStack {
	t.Helper()

	armSvc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	armH, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)

	blobs := azureblob.NewInMemoryBackend()
	backend := azureredis.NewInMemoryBackend(azureredis.DefaultSettings())
	backend.SetBlobStore(azureRedisBlobs{b: blobs})
	t.Cleanup(backend.Close)
	armH.Registry.SetRedis(&azureRedisAdapter{b: backend})

	e := echo.New()
	e.Any("/*", armH.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	opts := &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: srv.URL, Audience: "https://management.core.windows.net/"},
		}},
		Transport: srv.Client(),
		Retry:     policy.RetryOptions{MaxRetries: -1},
	}

	f, err := armredis.NewClientFactory(redisSDKSub, redisSDKCredential{}, opts)
	require.NoError(t, err)

	return &redisSDKStack{arm: srv, factory: f, blobs: blobs}
}

func (s *redisSDKStack) create(
	t *testing.T,
	name string,
	sku armredis.SKUName,
	family armredis.SKUFamily,
	capacity int32,
	props func(*armredis.CreateProperties),
) *armredis.ResourceInfo {
	t.Helper()

	cp := &armredis.CreateProperties{SKU: &armredis.SKU{
		Name: new(sku), Family: new(family), Capacity: new(capacity),
	}}
	if props != nil {
		props(cp)
	}

	poller, err := s.factory.NewClient().BeginCreate(t.Context(), redisSDKRG, name,
		armredis.CreateParameters{Location: new("local"), Properties: cp}, nil)
	require.NoError(t, err)

	res, err := poller.PollUntilDone(t.Context(), pollOpts())
	require.NoError(t, err)

	return &res.ResourceInfo
}

func sdkRedisClient(host string, port int32, password string, useTLS bool) *goredis.Client {
	opts := &goredis.Options{Addr: net.JoinHostPort(host, strconv.Itoa(int(port))), Password: password}
	if useTLS {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
		}
	}

	return goredis.NewClient(opts)
}

func TestAzureRedisARMSDK(t *testing.T) {
	t.Parallel()

	s := newRedisSDKStack(t)
	caches := s.factory.NewClient()
	host := "127.0.0.1"

	t.Run("create reports real endpoints", func(t *testing.T) {
		t.Parallel()

		c := s.create(t, "sdkcreate", armredis.SKUNameBasic, armredis.SKUFamilyC, 0, nil)
		require.Equal(t, armredis.ProvisioningStateSucceeded, *c.Properties.ProvisioningState)
		assert.Equal(t, "sdkcreate.redis.cache.windows.net", *c.Properties.HostName)
		assert.Nil(t, c.Properties.Port)
		require.NotNil(t, c.Properties.SSLPort)
	})

	t.Run("both keys authenticate and regenerate invalidates", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		tlsPort := *s.create(t, "sdkkeys", armredis.SKUNameBasic, armredis.SKUFamilyC, 0, nil).Properties.SSLPort
		keys := caches2keys(t, caches, "sdkkeys")

		for _, key := range []*string{keys.PrimaryKey, keys.SecondaryKey} {
			c := sdkRedisClient(host, tlsPort, *key, true)
			require.NoError(t, c.Ping(ctx).Err())
			_ = c.Close()
		}

		fresh, err := caches.RegenerateKey(ctx, redisSDKRG, "sdkkeys",
			armredis.RegenerateKeyParameters{KeyType: new(armredis.RedisKeyTypePrimary)}, nil)
		require.NoError(t, err)
		assert.NotEqual(t, *keys.PrimaryKey, *fresh.PrimaryKey)
		assert.Equal(t, *keys.SecondaryKey, *fresh.SecondaryKey)

		old := sdkRedisClient(host, tlsPort, *keys.PrimaryKey, true)
		require.Error(t, old.Ping(ctx).Err())
		_ = old.Close()

		now := sdkRedisClient(host, tlsPort, *fresh.PrimaryKey, true)
		require.NoError(t, now.Ping(ctx).Err())
		_ = now.Close()
	})

	t.Run("enableNonSslPort opens the plaintext port", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		s.create(t, "sdkplain", armredis.SKUNameBasic, armredis.SKUFamilyC, 0, nil)

		poller, err := caches.BeginUpdate(ctx, redisSDKRG, "sdkplain", armredis.UpdateParameters{
			Properties: &armredis.UpdateProperties{EnableNonSSLPort: new(true)},
		}, nil)
		require.NoError(t, err)

		res, err := poller.PollUntilDone(ctx, pollOpts())
		require.NoError(t, err)
		require.NotNil(t, res.Properties.Port)

		keys := caches2keys(t, caches, "sdkplain")
		c := sdkRedisClient(host, *res.Properties.Port, *keys.PrimaryKey, false)
		defer c.Close()
		require.NoError(t, c.Ping(ctx).Err())
	})

	t.Run("reboot of a basic cache loses data", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		tlsPort := *s.create(t, "sdkreboot", armredis.SKUNameBasic, armredis.SKUFamilyC, 0, nil).Properties.SSLPort
		keys := caches2keys(t, caches, "sdkreboot")

		c := sdkRedisClient(host, tlsPort, *keys.PrimaryKey, true)
		defer c.Close()
		require.NoError(t, c.Set(ctx, "k", "v", 0).Err())

		_, err := caches.ForceReboot(ctx, redisSDKRG, "sdkreboot",
			armredis.RebootParameters{RebootType: new(armredis.RebootTypeAllNodes)}, nil)
		require.NoError(t, err)
		assert.Zero(t, c.Exists(ctx, "k").Val())
	})

	t.Run("firewall rules", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		s.create(t, "sdkfw", armredis.SKUNameBasic, armredis.SKUFamilyC, 0, nil)

		rules := s.factory.NewFirewallRulesClient()
		_, err := rules.CreateOrUpdate(ctx, redisSDKRG, "sdkfw", "office", armredis.FirewallRule{
			Properties: &armredis.FirewallRuleProperties{StartIP: new("10.0.0.1"), EndIP: new("10.0.0.9")},
		}, nil)
		require.NoError(t, err)

		got, err := rules.Get(ctx, redisSDKRG, "sdkfw", "office", nil)
		require.NoError(t, err)
		assert.Equal(t, "10.0.0.9", *got.Properties.EndIP)

		page, err := rules.NewListPager(redisSDKRG, "sdkfw", nil).NextPage(ctx)
		require.NoError(t, err)
		assert.Len(t, page.Value, 1)

		_, err = rules.Delete(ctx, redisSDKRG, "sdkfw", "office", nil)
		require.NoError(t, err)
		_, err = rules.Get(ctx, redisSDKRG, "sdkfw", "office", nil)
		require.Error(t, err)
	})

	t.Run("export then import round trip", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		require.NoError(t, s.blobs.CreateContainer("exports"))

		tlsPort := *s.create(t, "sdkexport", armredis.SKUNameBasic, armredis.SKUFamilyC, 0, nil).Properties.SSLPort
		keys := caches2keys(t, caches, "sdkexport")

		src := sdkRedisClient(host, tlsPort, *keys.PrimaryKey, true)
		defer src.Close()
		require.NoError(t, src.Set(ctx, "persisted", "yes", 0).Err())

		exp, err := caches.BeginExportData(ctx, redisSDKRG, "sdkexport", armredis.ExportRDBParameters{
			Container: new("https://acct.blob.core.windows.net/exports?sv=2024&sig=x"),
			Prefix:    new("backup"), Format: new("RDB"),
		}, nil)
		require.NoError(t, err)
		_, err = exp.PollUntilDone(ctx, pollOpts())
		require.NoError(t, err)

		other := s.create(t, "sdkimport", armredis.SKUNameStandard, armredis.SKUFamilyC, 1, nil)
		imp, err := caches.BeginImportData(ctx, redisSDKRG, "sdkimport", armredis.ImportRDBParameters{
			Files:  []*string{new("https://acct.blob.core.windows.net/exports/backup.rdb?sv=2024&sig=x")},
			Format: new("RDB"),
		}, nil)
		require.NoError(t, err)
		_, err = imp.PollUntilDone(ctx, pollOpts())
		require.NoError(t, err)

		otherKeys := caches2keys(t, caches, "sdkimport")
		dst := sdkRedisClient(host, *other.Properties.SSLPort, *otherKeys.PrimaryKey, true)
		defer dst.Close()
		assert.Equal(t, "yes", dst.Get(ctx, "persisted").Val())
	})

	t.Run("delete", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		s.create(t, "sdkdelete", armredis.SKUNameBasic, armredis.SKUFamilyC, 0, nil)

		del, err := caches.BeginDelete(ctx, redisSDKRG, "sdkdelete", nil)
		require.NoError(t, err)
		_, err = del.PollUntilDone(ctx, pollOpts())
		require.NoError(t, err)

		_, err = caches.Get(ctx, redisSDKRG, "sdkdelete", nil)
		require.Error(t, err)
	})
}

func caches2keys(t *testing.T, c *armredis.Client, name string) armredis.ClientListKeysResponse {
	t.Helper()

	k, err := c.ListKeys(t.Context(), redisSDKRG, name, nil)
	require.NoError(t, err)

	return k
}

func TestAzureRedisARMSDKPremiumFeatures(t *testing.T) {
	t.Parallel()

	s := newRedisSDKStack(t)
	ctx := t.Context()

	s.create(t, "sdkprem", armredis.SKUNameBasic, armredis.SKUFamilyC, 0, nil)
	prem := s.create(
		t,
		"sdkprem2",
		armredis.SKUNamePremium,
		armredis.SKUFamilyP,
		1,
		func(p *armredis.CreateProperties) {
			p.RedisConfiguration = &armredis.CommonPropertiesRedisConfiguration{MaxmemoryPolicy: new("allkeys-lru")}
		},
	)
	assert.Equal(t, "allkeys-lru", *prem.Properties.RedisConfiguration.MaxmemoryPolicy)

	patches := s.factory.NewPatchSchedulesClient()
	_, err := patches.CreateOrUpdate(ctx, redisSDKRG, "sdkprem2", armredis.DefaultNameDefault, armredis.PatchSchedule{
		Properties: &armredis.ScheduleEntries{ScheduleEntries: []*armredis.ScheduleEntry{
			{DayOfWeek: new(armredis.DayOfWeekMonday), StartHourUTC: new(int32(3)), MaintenanceWindow: new("PT5H")},
		}},
	}, nil)
	require.NoError(t, err)

	got, err := patches.Get(ctx, redisSDKRG, "sdkprem2", armredis.DefaultNameDefault, nil)
	require.NoError(t, err)
	require.Len(t, got.Properties.ScheduleEntries, 1)
	assert.Equal(t, armredis.DayOfWeekMonday, *got.Properties.ScheduleEntries[0].DayOfWeek)

	_, err = patches.CreateOrUpdate(ctx, redisSDKRG, "sdkprem", armredis.DefaultNameDefault, armredis.PatchSchedule{
		Properties: &armredis.ScheduleEntries{ScheduleEntries: []*armredis.ScheduleEntry{
			{DayOfWeek: new(armredis.DayOfWeekMonday), StartHourUTC: new(int32(3))},
		}},
	}, nil)
	require.Error(t, err, "patch schedules are Premium-only")

	_, err = patches.Delete(ctx, redisSDKRG, "sdkprem2", armredis.DefaultNameDefault, nil)
	require.NoError(t, err)
}

func pollOpts() *runtime.PollUntilDoneOptions {
	return &runtime.PollUntilDoneOptions{Frequency: 100 * time.Millisecond}
}
