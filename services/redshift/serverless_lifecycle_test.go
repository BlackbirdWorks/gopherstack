package redshift_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

const lifecycleDelay = 10 * time.Second

func newLifecycleBackend(t *testing.T, delay time.Duration) *redshift.InMemoryBackend {
	t.Helper()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetServerlessLifecycleDelay(delay)

	return b
}

func TestServerlessWorkgroupLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  string
		delay time.Duration
	}{
		{"instant", "AVAILABLE", 0},
		{"delayed", "CREATING", lifecycleDelay},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := newLifecycleBackend(t, tt.delay)
				_, err := b.CreateNamespace(redshift.CreateNamespaceParams{NamespaceName: "ns-life"})
				require.NoError(t, err)

				wg, err := b.CreateWorkgroup("wg-life", "ns-life", redshift.WorkgroupParams{}, nil)
				require.NoError(t, err)
				assert.Equal(t, tt.want, wg.Status)

				if tt.delay > 0 {
					_, err = b.UpdateWorkgroup("wg-life", redshift.WorkgroupParams{})
					require.ErrorIs(t, err, redshift.ErrServerlessConflict)
					_, err = b.DeleteWorkgroup("wg-life")
					require.ErrorIs(t, err, redshift.ErrServerlessConflict)

					time.Sleep(tt.delay)
				}

				got, err := b.GetWorkgroup("wg-life")
				require.NoError(t, err)
				assert.Equal(t, "AVAILABLE", got.Status)

				upd, err := b.UpdateWorkgroup("wg-life", redshift.WorkgroupParams{BaseCapacity: 64})
				require.NoError(t, err)

				if tt.delay > 0 {
					assert.Equal(t, "MODIFYING", upd.Status)
					time.Sleep(tt.delay)
				}

				got, err = b.GetWorkgroup("wg-life")
				require.NoError(t, err)
				assert.Equal(t, "AVAILABLE", got.Status)

				del, err := b.DeleteWorkgroup("wg-life")
				require.NoError(t, err)
				assert.Equal(t, "DELETING", del.Status)

				if tt.delay > 0 {
					got, err = b.GetWorkgroup("wg-life")
					require.NoError(t, err)
					assert.Equal(t, "DELETING", got.Status)
					time.Sleep(tt.delay)
				}

				_, err = b.GetWorkgroup("wg-life")
				require.ErrorIs(t, err, redshift.ErrWorkgroupNotFound)
			})
		})
	}
}

func TestServerlessNamespaceLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		delay time.Duration
	}{
		{"instant", 0},
		{"delayed", lifecycleDelay},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := newLifecycleBackend(t, tt.delay)

				ns, err := b.CreateNamespace(redshift.CreateNamespaceParams{NamespaceName: "ns-life"})
				require.NoError(t, err)
				assert.Equal(t, "AVAILABLE", ns.Status)

				upd, err := b.UpdateNamespace("ns-life", redshift.UpdateNamespaceParams{AdminUsername: "root"})
				require.NoError(t, err)

				if tt.delay > 0 {
					assert.Equal(t, "MODIFYING", upd.Status)
					_, err = b.DeleteNamespace("ns-life", "", 0)
					require.ErrorIs(t, err, redshift.ErrServerlessConflict)
					time.Sleep(tt.delay)
				}

				del, err := b.DeleteNamespace("ns-life", "", 0)
				require.NoError(t, err)
				assert.Equal(t, "DELETING", del.Status)

				time.Sleep(tt.delay)

				_, err = b.GetNamespace("ns-life")
				require.ErrorIs(t, err, redshift.ErrNamespaceNotFound)
			})
		})
	}
}

func TestServerlessWorkgroupStatus(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := newLifecycleBackend(t, lifecycleDelay)
		_, err := b.CreateNamespace(redshift.CreateNamespaceParams{NamespaceName: "ns-stat"})
		require.NoError(t, err)
		_, err = b.CreateWorkgroup("wg-stat", "ns-stat", redshift.WorkgroupParams{}, nil)
		require.NoError(t, err)

		st, ok := b.WorkgroupStatus("wg-stat")
		assert.True(t, ok)
		assert.Equal(t, "CREATING", st)

		time.Sleep(lifecycleDelay)

		st, _ = b.WorkgroupStatus("wg-stat")
		assert.Equal(t, "AVAILABLE", st)

		_, ok = b.WorkgroupStatus("missing-wg")
		assert.False(t, ok)
	})
}

func TestServerlessValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		wgName string
		params redshift.WorkgroupParams
		ok     bool
	}{
		{"valid", "wg-ok", redshift.WorkgroupParams{BaseCapacity: 32, MaxCapacity: 64, Port: 5439}, true},
		{"base four", "wg-ok", redshift.WorkgroupParams{BaseCapacity: 4}, true},
		{"high port", "wg-ok", redshift.WorkgroupParams{Port: 8200}, true},
		{"short name", "wg", redshift.WorkgroupParams{}, false},
		{"upper name", "WG-Upper", redshift.WorkgroupParams{}, false},
		{"underscore name", "wg_bad", redshift.WorkgroupParams{}, false},
		{"base not multiple", "wg-ok", redshift.WorkgroupParams{BaseCapacity: 12}, false},
		{"base too big", "wg-ok", redshift.WorkgroupParams{BaseCapacity: 6000}, false},
		{"max not multiple", "wg-ok", redshift.WorkgroupParams{MaxCapacity: 12}, false},
		{"max below base", "wg-ok", redshift.WorkgroupParams{BaseCapacity: 64, MaxCapacity: 32}, false},
		{"port low", "wg-ok", redshift.WorkgroupParams{Port: 5000}, false},
		{"port gap", "wg-ok", redshift.WorkgroupParams{Port: 6000}, false},
		{"port high", "wg-ok", redshift.WorkgroupParams{Port: 8216}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newLifecycleBackend(t, 0)
			_, err := b.CreateNamespace(redshift.CreateNamespaceParams{NamespaceName: "ns-val"})
			require.NoError(t, err)

			_, err = b.CreateWorkgroup(tt.wgName, "ns-val", tt.params, nil)
			if tt.ok {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, redshift.ErrServerlessValidation)
		})
	}
}

func TestServerlessUpdateValidation(t *testing.T) {
	t.Parallel()

	b := newLifecycleBackend(t, 0)
	_, err := b.CreateNamespace(redshift.CreateNamespaceParams{NamespaceName: "ns-upd"})
	require.NoError(t, err)
	_, err = b.CreateWorkgroup("wg-upd", "ns-upd", redshift.WorkgroupParams{}, nil)
	require.NoError(t, err)

	_, err = b.UpdateWorkgroup("wg-upd", redshift.WorkgroupParams{Port: 1})
	require.ErrorIs(t, err, redshift.ErrServerlessValidation)

	_, err = b.CreateNamespace(redshift.CreateNamespaceParams{NamespaceName: "NS"})
	require.ErrorIs(t, err, redshift.ErrServerlessValidation)
}
