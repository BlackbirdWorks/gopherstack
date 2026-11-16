package azureappconfig_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ac "github.com/blackbirdworks/gopherstack/services/azureappconfig"
)

type fakePlatform struct {
	replicas map[string]string
	settings ac.PlatformSettings
}

func (f fakePlatform) StoreSettings(string) (ac.PlatformSettings, bool) { return f.settings, true }

func (f fakePlatform) ReplicaParent(label string) (string, bool) {
	p, ok := f.replicas[label]

	return p, ok
}

func newPlatformBackend(t *testing.T, p fakePlatform) (*ac.InMemoryBackend, *fakeClock) {
	t.Helper()

	b := ac.NewInMemoryBackend()
	clk := newClock()
	ac.SetNow(b, clk.Now)
	b.SetPlatform(p)

	_, err := b.PutStore(storeName)
	require.NoError(t, err)

	return b, clk
}

func TestAdmitRequestQuota(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		sku    string
		limit  int
		window time.Duration
	}{
		{name: "free daily", sku: "Free", limit: 1000, window: 24 * time.Hour},
		{name: "developer hourly", sku: "Developer", limit: 6000, window: time.Hour},
		{name: "standard hourly", sku: "Standard", limit: 30000, window: time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, clk := newPlatformBackend(t, fakePlatform{settings: ac.PlatformSettings{SKU: tt.sku}})

			for range tt.limit {
				_, err := b.Admit(storeName)
				require.NoError(t, err)
			}

			wait, err := b.Admit(storeName)
			require.ErrorIs(t, err, ac.ErrTooManyRequests)
			assert.Positive(t, wait)

			clk.Advance(tt.window)

			_, err = b.Admit(storeName)
			require.NoError(t, err, "the quota resets with the window")
		})
	}
}

func TestAdmitPremiumUnlimited(t *testing.T) {
	t.Parallel()

	b, _ := newPlatformBackend(t, fakePlatform{settings: ac.PlatformSettings{SKU: "premium"}})

	for range 31000 {
		_, err := b.Admit(storeName)
		require.NoError(t, err)
	}
}

func TestPublicNetworkAccessAndReplica(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		host   string
		access string
		status int
	}{
		{name: "store host", host: storeName + ".localhost", status: http.StatusOK},
		{name: "replica host serves parent data", host: storeName + "-west.localhost", status: http.StatusOK},
		{name: "unknown host label falls to default store", host: "other.localhost", status: http.StatusNotFound},
		{
			name: "public access disabled", host: storeName + ".localhost", access: "Disabled",
			status: http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newPlatformBackend(t, fakePlatform{
				settings: ac.PlatformSettings{SKU: "standard", PublicNetworkAccess: tt.access},
				replicas: map[string]string{storeName + "-west": storeName},
			})

			_, err := b.SetKeyValue(storeName, "k", "", ac.KVInput{Value: new("v")}, ac.Precondition{})
			require.NoError(t, err)

			e := echo.New()
			e.Any("/*", ac.NewHandler(b, ac.Settings{Port: ac.DefaultPort}).Handler())

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/kv/k?"+av, strings.NewReader(""))
			req.Host = tt.host
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			assert.Equal(t, tt.status, rec.Code)
		})
	}
}

func TestStorageQuotaAndRevisionPruning(t *testing.T) {
	t.Parallel()

	b, clk := newPlatformBackend(t, fakePlatform{settings: ac.PlatformSettings{SKU: "free"}})
	require.NoError(t, b.SetRevisionRetention(storeName, 7))

	big := strings.Repeat("x", 1_000_000)

	for i := range 10 {
		_, err := b.SetKeyValue(storeName, "big", "", ac.KVInput{Value: &big}, ac.Precondition{})
		require.NoError(t, err, "write %d fits the free tier's 10 MB", i)
	}

	_, err := b.SetKeyValue(storeName, "big", "", ac.KVInput{Value: &big}, ac.Precondition{})
	require.ErrorIs(t, err, ac.ErrTooManyRequests, "revisions count toward the storage quota")

	clk.Advance(8 * 24 * time.Hour)

	_, err = b.SetKeyValue(storeName, "big", "", ac.KVInput{Value: &big}, ac.Precondition{})
	require.NoError(t, err, "revisions past the retention window free their storage")
	assert.Equal(t, 1, ac.HistoryLen(b, storeName), "only the new write remains")
}

func TestSnapshotProvisioningLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sku        string
		wantStatus string
		wantOp     string
		valueBytes int
		retention  int64
		wantErr    bool
		prior      bool
	}{
		{name: "ready", sku: "standard", valueBytes: 10, wantStatus: "ready", wantOp: "Succeeded"},
		{name: "quota failed", sku: "free", valueBytes: 9_000_000, prior: true, wantStatus: "failed", wantOp: "Failed"},
		{name: "free retention above a week", sku: "free", valueBytes: 10, retention: 8 * 24 * 3600, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, clk := newPlatformBackend(t, fakePlatform{settings: ac.PlatformSettings{SKU: tt.sku}})

			val := strings.Repeat("v", min(tt.valueBytes, 900_000))
			for i := range max(tt.valueBytes/900_000, 1) {
				key := "k" + string(rune('a'+i))
				_, err := b.SetKeyValue(storeName, key, "", ac.KVInput{Value: &val}, ac.Precondition{})
				require.NoError(t, err)
			}

			if tt.prior {
				_, err := b.CreateSnapshot(storeName, "s0", ac.SnapshotSpec{Filters: []ac.Filter{{Key: "*"}}})
				require.NoError(t, err)
			}

			snap, err := b.CreateSnapshot(storeName, "s1", ac.SnapshotSpec{
				Filters: []ac.Filter{{Key: "*"}}, Retention: tt.retention,
			})
			if tt.wantErr {
				require.ErrorIs(t, err, ac.ErrInvalidArgument)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "provisioning", snap.Status)

			op, _, err := b.SnapshotOperation(storeName, "s1")
			require.NoError(t, err)
			assert.Equal(t, "Running", op)

			_, err = b.SetSnapshotStatus(storeName, "s1", "archived", ac.Precondition{})
			require.ErrorIs(t, err, ac.ErrSnapshotStatus, "a provisioning snapshot cannot be archived")

			clk.Advance(time.Minute)

			got, err := b.GetSnapshot(storeName, "s1")
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, got.Status)

			op, detail, err := b.SnapshotOperation(storeName, "s1")
			require.NoError(t, err)
			assert.Equal(t, tt.wantOp, op)
			assert.Equal(t, tt.wantOp == "Failed", detail != "")
		})
	}
}

type unknownPlatform struct{}

func (unknownPlatform) StoreSettings(string) (ac.PlatformSettings, bool) {
	return ac.PlatformSettings{}, false
}
func (unknownPlatform) ReplicaParent(string) (string, bool) { return "", false }

func TestAdmitUnmanagedStoreIsNotThrottled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		platform ac.Platform
		name     string
	}{
		{name: "no platform", platform: nil},
		{name: "store unknown to arm", platform: unknownPlatform{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ac.NewInMemoryBackend()
			b.SetPlatform(tt.platform)

			_, err := b.PutStore(storeName)
			require.NoError(t, err)

			for range 31000 {
				_, err = b.Admit(storeName)
				require.NoError(t, err)
			}
		})
	}
}

func TestAdmitRetryDelayRunsToWindowEnd(t *testing.T) {
	t.Parallel()

	b, clk := newPlatformBackend(t, fakePlatform{settings: ac.PlatformSettings{SKU: "Free"}})

	for range 1000 {
		_, err := b.Admit(storeName)
		require.NoError(t, err)
	}

	clk.Advance(time.Hour)

	wait, err := b.Admit(storeName)
	require.ErrorIs(t, err, ac.ErrTooManyRequests)
	assert.Equal(t, 11*time.Hour, wait, "429 lasts until midnight UTC")
}

func TestAdmitRetryDelayHasAFloor(t *testing.T) {
	t.Parallel()

	b, clk := newPlatformBackend(t, fakePlatform{settings: ac.PlatformSettings{SKU: "Free"}})

	for range 1000 {
		_, err := b.Admit(storeName)
		require.NoError(t, err)
	}

	clk.Advance(12*time.Hour - 500*time.Millisecond)

	wait, err := b.Admit(storeName)
	require.ErrorIs(t, err, ac.ErrTooManyRequests)
	assert.Equal(t, time.Second, wait)
}
