package azureredis_test

import (
	"crypto/tls"
	"net"
	"strconv"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureredis"
)

func TestEndpointPorts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		nonSSL    bool
		wantPlain bool
	}{
		{name: "tls only", nonSSL: false, wantPlain: false},
		{name: "with non ssl port", nonSSL: true, wantPlain: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newBackend(t)
			spec := basicSpec()
			spec.EnableNonSSLPort = tt.nonSSL
			ep := ensure(t, b, "c1", spec)

			assert.NotZero(t, ep.SSLPort)
			assert.Equal(t, tt.wantPlain, ep.Port != 0)

			keys, err := b.Keys("c1")
			require.NoError(t, err)
			require.NoError(t, setGet(t.Context(), dialTLS(t, ep, keys.Primary), "k", "v"))

			if tt.wantPlain {
				require.NoError(t, setGet(t.Context(), dialPlain(t, ep, keys.Primary), "k2", "v"))
			}
		})
	}
}

func TestToggleNonSSLPort(t *testing.T) {
	t.Parallel()

	b, _ := newBackend(t)
	spec := basicSpec()
	ep := ensure(t, b, "c1", spec)
	require.Zero(t, ep.Port)

	spec.EnableNonSSLPort = true
	on := ensure(t, b, "c1", spec)
	assert.NotZero(t, on.Port)
	assert.Equal(t, ep.SSLPort, on.SSLPort, "the TLS port is stable across updates")

	spec.EnableNonSSLPort = false
	off := ensure(t, b, "c1", spec)
	assert.Zero(t, off.Port)
}

func TestAuth(t *testing.T) {
	t.Parallel()

	b, _ := newBackend(t)
	ep := ensure(t, b, "c1", basicSpec())
	keys, err := b.Keys("c1")
	require.NoError(t, err)

	tests := []struct {
		name     string
		password string
		wantErr  bool
	}{
		{name: "primary", password: keys.Primary},
		{name: "secondary", password: keys.Secondary},
		{name: "wrong", password: "nope", wantErr: true},
		{name: "none", password: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pingErr := dialTLS(t, ep, tt.password).Ping(t.Context()).Err()
			if tt.wantErr {
				require.Error(t, pingErr)

				return
			}

			require.NoError(t, pingErr)
		})
	}
}

func TestAuthRESP2(t *testing.T) {
	t.Parallel()

	b, _ := newBackend(t)
	ep := ensure(t, b, "c1", basicSpec())
	keys, err := b.Keys("c1")
	require.NoError(t, err)

	c := goredis.NewClient(&goredis.Options{
		Addr: net.JoinHostPort(ep.Host, strconv.Itoa(ep.SSLPort)), Password: keys.Secondary, Protocol: 2,
		TLSConfig: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12},
	})
	t.Cleanup(func() { _ = c.Close() })

	require.NoError(t, c.Ping(t.Context()).Err())
}

func TestRegenerateKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		keyType       string
		oldPrimaryOK  bool
		oldSecondOK   bool
		newPrimaryOK  bool
		newSecondOK   bool
		wantRegenFail bool
	}{
		{name: "primary", keyType: "Primary", oldSecondOK: true, newPrimaryOK: true, newSecondOK: true},
		{name: "secondary", keyType: "secondary", oldPrimaryOK: true, newPrimaryOK: true, newSecondOK: true},
		{
			name: "bad type", keyType: "Tertiary", wantRegenFail: true,
			oldPrimaryOK: true, oldSecondOK: true, newPrimaryOK: true, newSecondOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newBackend(t)
			ep := ensure(t, b, "c1", basicSpec())
			old, err := b.Keys("c1")
			require.NoError(t, err)

			fresh, err := b.RegenerateKey("c1", tt.keyType)
			if tt.wantRegenFail {
				require.ErrorIs(t, err, azureredis.ErrInvalid)
			} else {
				require.NoError(t, err)
			}

			ok := func(pw string) bool { return dialTLS(t, ep, pw).Ping(t.Context()).Err() == nil }
			assert.Equal(t, tt.oldPrimaryOK, ok(old.Primary), "old primary")
			assert.Equal(t, tt.oldSecondOK, ok(old.Secondary), "old secondary")

			if !tt.wantRegenFail {
				assert.Equal(t, tt.newPrimaryOK, ok(fresh.Primary), "new primary")
				assert.Equal(t, tt.newSecondOK, ok(fresh.Secondary), "new secondary")
			}
		})
	}
}

func TestMinimumTLSVersion(t *testing.T) {
	t.Parallel()

	b, _ := newBackend(t)
	spec := basicSpec()
	spec.MinTLS = "1.0"
	ep := ensure(t, b, "c1", spec)
	keys, _ := b.Keys("c1")

	dial := func(maxVersion uint16) error {
		c := goredis.NewClient(&goredis.Options{
			Addr: net.JoinHostPort(ep.Host, strconv.Itoa(ep.SSLPort)), Password: keys.Primary,
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true, MinVersion: tls.VersionTLS10, MaxVersion: maxVersion,
			},
		})
		defer c.Close()

		return c.Ping(t.Context()).Err()
	}

	require.NoError(t, dial(tls.VersionTLS11), "TLS 1.1 allowed while minimumTlsVersion is 1.0")

	spec.MinTLS = "1.2"
	ensure(t, b, "c1", spec)
	require.Error(t, dial(tls.VersionTLS11), "TLS 1.1 refused once minimumTlsVersion is 1.2")
	require.NoError(t, dial(tls.VersionTLS12))
}

func TestReboot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sku        string
		rebootType string
		persist    bool
		wantKept   bool
		wantErr    bool
	}{
		{name: "basic loses data", sku: azureredis.SKUBasic, rebootType: azureredis.RebootAll, wantKept: false},
		{name: "basic primary node", sku: azureredis.SKUBasic, rebootType: azureredis.RebootPrimary, wantKept: false},
		{
			name:       "basic has no secondary",
			sku:        azureredis.SKUBasic,
			rebootType: azureredis.RebootSecondary,
			wantErr:    true,
			wantKept:   true,
		},
		{
			name:       "standard primary keeps data",
			sku:        azureredis.SKUStandard,
			rebootType: azureredis.RebootPrimary,
			wantKept:   true,
		},
		{
			name:       "standard secondary",
			sku:        azureredis.SKUStandard,
			rebootType: azureredis.RebootSecondary,
			wantKept:   true,
		},
		{name: "standard all nodes", sku: azureredis.SKUStandard, rebootType: azureredis.RebootAll},
		{
			name: "premium all nodes persisted", sku: azureredis.SKUPremium, persist: true,
			rebootType: azureredis.RebootAll, wantKept: true,
		},
		{name: "bogus type", sku: azureredis.SKUStandard, rebootType: "Everything", wantErr: true, wantKept: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newBackend(t)
			ep := ensure(t, b, "c1", azureredis.Spec{SKU: tt.sku, MinTLS: "1.2", Persistence: tt.persist})
			keys, _ := b.Keys("c1")
			c := dialTLS(t, ep, keys.Primary)
			require.NoError(t, c.Set(t.Context(), "k", "v", 0).Err())

			err := b.Reboot("c1", tt.rebootType)
			if tt.wantErr {
				require.ErrorIs(t, err, azureredis.ErrInvalid)
			} else {
				require.NoError(t, err)
			}

			n, err := c.Exists(t.Context(), "k").Result()
			require.NoError(t, err)
			assert.Equal(t, tt.wantKept, n == 1)
		})
	}
}

func TestImportExportRoundTrip(t *testing.T) {
	t.Parallel()

	b, blobs := newBackend(t)
	ep := ensure(t, b, "src", basicSpec())
	keys, _ := b.Keys("src")
	src := dialTLS(t, ep, keys.Primary)
	require.NoError(t, src.Set(t.Context(), "str", "value", 0).Err())
	require.NoError(t, src.HSet(t.Context(), "hash", "f", "1").Err())
	require.NoError(t, src.RPush(t.Context(), "list", "a", "b").Err())

	const containerURL = "https://acct.blob.core.windows.net/exports?sv=2024&sig=SECRET"
	require.NoError(t, b.Export("src", "RDB", "backup", containerURL))
	require.Contains(t, blobs.objs, "exports/backup.rdb")

	dstEP := ensure(t, b, "dst", basicSpec())
	dstKeys, _ := b.Keys("dst")
	dst := dialTLS(t, dstEP, dstKeys.Primary)

	require.NoError(
		t,
		b.Import("dst", "RDB", []string{"https://acct.blob.core.windows.net/exports/backup.rdb?sig=SECRET"}),
	)

	got, err := dst.Get(t.Context(), "str").Result()
	require.NoError(t, err)
	assert.Equal(t, "value", got)
	assert.Equal(t, "1", dst.HGet(t.Context(), "hash", "f").Val())
	assert.Equal(t, []string{"a", "b"}, dst.LRange(t.Context(), "list", 0, -1).Val())
}

func TestImportExportErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run     func(b *azureredis.InMemoryBackend) error
		wantErr error
		name    string
	}{
		{
			name: "import bad format",
			run: func(b *azureredis.InMemoryBackend) error {
				return b.Import("c1", "AOF", []string{"https://a.blob.core.windows.net/c/b"})
			},
			wantErr: azureredis.ErrInvalid,
		},
		{
			name: "import two files",
			run: func(b *azureredis.InMemoryBackend) error {
				return b.Import(
					"c1",
					"RDB",
					[]string{"https://a.blob.core.windows.net/c/b", "https://a.blob.core.windows.net/c/d"},
				)
			},
			wantErr: azureredis.ErrInvalid,
		},
		{
			name: "import missing blob",
			run: func(b *azureredis.InMemoryBackend) error {
				return b.Import("c1", "RDB", []string{"https://a.blob.core.windows.net/c/missing.rdb"})
			},
			wantErr: azureredis.ErrInvalid,
		},
		{
			name: "import unknown cache",
			run: func(b *azureredis.InMemoryBackend) error {
				return b.Import("nope", "RDB", []string{"https://a.blob.core.windows.net/c/b"})
			},
			wantErr: azureredis.ErrNotFound,
		},
		{
			name: "export without prefix",
			run: func(b *azureredis.InMemoryBackend) error {
				return b.Export("c1", "RDB", "", "https://a.blob.core.windows.net/c")
			},
			wantErr: azureredis.ErrInvalid,
		},
		{
			name: "export to a blob url",
			run: func(b *azureredis.InMemoryBackend) error {
				return b.Export("c1", "RDB", "p", "https://a.blob.core.windows.net/c/blob")
			},
			wantErr: azureredis.ErrInvalid,
		},
		{
			name: "export unknown cache",
			run: func(b *azureredis.InMemoryBackend) error {
				return b.Export("nope", "RDB", "p", "https://a.blob.core.windows.net/c")
			},
			wantErr: azureredis.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newBackend(t)
			ensure(t, b, "c1", basicSpec())
			require.ErrorIs(t, tt.run(b), tt.wantErr)
		})
	}
}

func TestPathStyleBlobURL(t *testing.T) {
	t.Parallel()

	b, blobs := newBackend(t)
	ensure(t, b, "c1", basicSpec())
	require.NoError(t, b.Export("c1", "", "p", "http://localhost:18000/devstoreaccount1/exports?sig=x"))
	assert.Contains(t, blobs.objs, "exports/p.rdb")
}

func TestDeleteAndNotFound(t *testing.T) {
	t.Parallel()

	b, _ := newBackend(t)
	ensure(t, b, "c1", basicSpec())
	require.NoError(t, b.Delete("C1"))

	_, ok := b.Endpoint("c1")
	assert.False(t, ok)
	require.ErrorIs(t, b.Delete("c1"), azureredis.ErrNotFound)

	_, err := b.Keys("c1")
	require.ErrorIs(t, err, azureredis.ErrNotFound)
	require.ErrorIs(t, b.Reboot("c1", azureredis.RebootAll), azureredis.ErrNotFound)
}

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	b, _ := newBackend(t)
	ep := ensure(t, b, "c1", azureredis.Spec{SKU: azureredis.SKUStandard, MinTLS: "1.2", EnableNonSSLPort: true})
	keys, _ := b.Keys("c1")
	require.NoError(t, dialTLS(t, ep, keys.Primary).Set(t.Context(), "k", "v", 0).Err())

	snap := b.Snapshot(t.Context())

	other, _ := newBackend(t)
	require.NoError(t, other.Restore(t.Context(), snap))

	got, ok := other.Endpoint("c1")
	require.True(t, ok)
	assert.NotZero(t, got.Port)

	restored, err := other.Keys("c1")
	require.NoError(t, err)
	assert.Equal(t, keys, restored)
	assert.Equal(t, "v", dialTLS(t, got, restored.Secondary).Get(t.Context(), "k").Val())
}

func TestCacheLimit(t *testing.T) {
	t.Parallel()

	b, _ := newBackend(t)

	for i := range 128 {
		ensure(t, b, "c"+strconv.Itoa(i), basicSpec())
	}

	_, err := b.Ensure("overflow", basicSpec())
	require.ErrorIs(t, err, azureredis.ErrInvalid)
}

func TestCertificateSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  azureredis.Settings
	}{
		{name: "cert without key", cfg: azureredis.Settings{TLSCertFile: "/x"}},
		{name: "key without cert", cfg: azureredis.Settings{TLSKeyFile: "/x"}},
		{name: "unreadable pair", cfg: azureredis.Settings{TLSCertFile: "/nope", TLSKeyFile: "/nope"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azureredis.NewInMemoryBackend(tt.cfg)
			t.Cleanup(b.Close)

			_, err := b.Ensure("c1", basicSpec())
			require.Error(t, err)
		})
	}
}

func TestAuthNotRequired(t *testing.T) {
	t.Parallel()

	b, _ := newBackend(t)
	spec := basicSpec()
	spec.AuthNotRequired = true
	ep := ensure(t, b, "c1", spec)
	require.NoError(t, dialTLS(t, ep, "").Ping(t.Context()).Err())

	spec.AuthNotRequired = false
	ensure(t, b, "c1", spec)
	require.Error(t, dialTLS(t, ep, "").Ping(t.Context()).Err())
}
