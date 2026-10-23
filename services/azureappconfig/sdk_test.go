package azureappconfig_test

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azappconfig/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ac "github.com/blackbirdworks/gopherstack/services/azureappconfig"
)

func statusOf(err error) int {
	if re, ok := errors.AsType[*azcore.ResponseError](err); ok {
		return re.StatusCode
	}

	return 0
}

func collect(t *testing.T, c *azappconfig.Client, sel azappconfig.SettingSelector) []azappconfig.Setting {
	t.Helper()

	var out []azappconfig.Setting

	pager := c.NewListSettingsPager(sel, nil)
	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		out = append(out, page.Settings...)
	}

	return out
}

func keysOf(ss []azappconfig.Setting) []string {
	out := make([]string, 0, len(ss))

	for _, s := range ss {
		k := *s.Key
		if s.Label != nil {
			k += "|" + *s.Label
		}

		out = append(out, k)
	}

	return out
}

func TestSDK_SetGetDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		label *string
		name  string
	}{
		{name: "null label"},
		{name: "prod label", label: new("prod")},
		{name: "dev label", label: new("dev")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			c := e.client(t)

			set, err := c.SetSetting(t.Context(), "app/color", new("red"), &azappconfig.SetSettingOptions{
				Label: tt.label, ContentType: new("text/plain"), Tags: map[string]*string{"team": new("core")},
			})
			require.NoError(t, err)
			assert.NotEmpty(t, set.SyncToken)
			assert.NotEmpty(t, *set.ETag)

			got, err := c.GetSetting(t.Context(), "app/color", &azappconfig.GetSettingOptions{Label: tt.label})
			require.NoError(t, err)
			assert.Equal(t, "red", *got.Value)
			assert.Equal(t, "text/plain", *got.ContentType)
			assert.Equal(t, "core", *got.Tags["team"])
			assert.Equal(t, *set.ETag, *got.ETag)
			assert.False(t, *got.IsReadOnly)

			_, err = c.DeleteSetting(t.Context(), "app/color", &azappconfig.DeleteSettingOptions{Label: tt.label})
			require.NoError(t, err)

			_, err = c.GetSetting(t.Context(), "app/color", &azappconfig.GetSettingOptions{Label: tt.label})
			assert.Equal(t, http.StatusNotFound, statusOf(err))
		})
	}
}

func TestSDK_LabelsAreSeparateSettings(t *testing.T) {
	t.Parallel()

	c := newEnv(t, envOpts{}).client(t)

	for _, l := range []*string{nil, new("a"), new("b")} {
		v := "v-null"
		if l != nil {
			v = "v-" + *l
		}

		_, err := c.SetSetting(t.Context(), "k", &v, &azappconfig.SetSettingOptions{Label: l})
		require.NoError(t, err)
	}

	got, err := c.GetSetting(t.Context(), "k", &azappconfig.GetSettingOptions{Label: new("b")})
	require.NoError(t, err)
	assert.Equal(t, "v-b", *got.Value)
	assert.Len(t, collect(t, c, azappconfig.SettingSelector{}), 3)
}

func TestSDK_ListFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		key   *string
		label *string
		want  []string
	}{
		{name: "all", want: []string{"app/a|prod", "app/b|dev", "app/b|prod", "other", "svc/x"}},
		{name: "key prefix", key: new("app/*"), want: []string{"app/a|prod", "app/b|dev", "app/b|prod"}},
		{name: "key suffix", key: new("*/b"), want: []string{"app/b|dev", "app/b|prod"}},
		{name: "key contains", key: new("*pp*"), want: []string{"app/a|prod", "app/b|dev", "app/b|prod"}},
		{name: "key exact", key: new("other"), want: []string{"other"}},
		{name: "key list", key: new("other,svc/x"), want: []string{"other", "svc/x"}},
		{name: "label exact", label: new("prod"), want: []string{"app/a|prod", "app/b|prod"}},
		{name: "null label", label: new(`\0`), want: []string{"other", "svc/x"}},
		{name: "label prefix", key: new("app/*"), label: new("pr*"), want: []string{"app/a|prod", "app/b|prod"}},
		{name: "key and label", key: new("app/b"), label: new("dev"), want: []string{"app/b|dev"}},
		{name: "no match", key: new("zzz*"), want: []string{}},
	}

	e := newEnv(t, envOpts{})
	c := e.client(t)

	for _, s := range []struct{ key, label string }{
		{"app/a", "prod"}, {"app/b", "prod"}, {"app/b", "dev"}, {"other", ""}, {"svc/x", ""},
	} {
		opts := &azappconfig.SetSettingOptions{}
		if s.label != "" {
			opts.Label = new(s.label)
		}

		_, err := c.SetSetting(t.Context(), s.key, new("v"), opts)
		require.NoError(t, err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := keysOf(collect(t, c, azappconfig.SettingSelector{KeyFilter: tt.key, LabelFilter: tt.label}))
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSDK_ListTagFilterAndSelect(t *testing.T) {
	t.Parallel()

	c := newEnv(t, envOpts{}).client(t)

	_, err := c.SetSetting(t.Context(), "t1", new("v1"), &azappconfig.SetSettingOptions{
		Tags: map[string]*string{"env": new("prod")},
	})
	require.NoError(t, err)
	_, err = c.SetSetting(t.Context(), "t2", new("v2"), &azappconfig.SetSettingOptions{
		Tags: map[string]*string{"env": new("dev")},
	})
	require.NoError(t, err)

	got := collect(t, c, azappconfig.SettingSelector{TagsFilter: []string{"env=prod"}})
	assert.Equal(t, []string{"t1"}, keysOf(got))

	sel := collect(t, c, azappconfig.SettingSelector{
		Fields: []azappconfig.SettingFields{azappconfig.SettingFieldsKey},
	})
	require.Len(t, sel, 2)
	assert.Nil(t, sel[0].Value)
	assert.Nil(t, sel[0].ETag)
}

func TestSDK_Pagination(t *testing.T) {
	t.Parallel()

	c := newEnv(t, envOpts{}).client(t)

	const total = 230

	for i := range total {
		_, err := c.SetSetting(t.Context(), "page/"+string(rune('a'+i/26))+string(rune('a'+i%26)), new("v"), nil)
		require.NoError(t, err)
	}

	pages := 0
	count := 0
	pager := c.NewListSettingsPager(azappconfig.SettingSelector{KeyFilter: new("page/*")}, nil)

	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		pages++
		count += len(page.Settings)
	}

	assert.Equal(t, 3, pages)
	assert.Equal(t, total, count)
}

func TestSDK_ConditionalUpdates(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	c := e.client(t)

	first, err := c.AddSetting(t.Context(), "cond", new("1"), nil)
	require.NoError(t, err)

	_, err = c.AddSetting(t.Context(), "cond", new("again"), nil)
	assert.Equal(t, http.StatusPreconditionFailed, statusOf(err), "add on an existing key")

	stale := azcore.ETag("stale")
	_, err = c.SetSetting(t.Context(), "cond", new("2"), &azappconfig.SetSettingOptions{OnlyIfUnchanged: &stale})
	assert.Equal(t, http.StatusPreconditionFailed, statusOf(err), "set with a stale etag")

	second, err := c.SetSetting(
		t.Context(),
		"cond",
		new("2"),
		&azappconfig.SetSettingOptions{OnlyIfUnchanged: first.ETag},
	)
	require.NoError(t, err)
	assert.NotEqual(t, *first.ETag, *second.ETag)

	_, err = c.DeleteSetting(t.Context(), "cond", &azappconfig.DeleteSettingOptions{OnlyIfUnchanged: first.ETag})
	assert.Equal(t, http.StatusPreconditionFailed, statusOf(err), "delete with a stale etag")

	_, err = c.GetSetting(t.Context(), "cond", &azappconfig.GetSettingOptions{OnlyIfChanged: second.ETag})
	assert.Equal(t, http.StatusNotModified, statusOf(err), "get with the current etag")

	_, err = c.DeleteSetting(t.Context(), "cond", &azappconfig.DeleteSettingOptions{OnlyIfUnchanged: second.ETag})
	require.NoError(t, err)
}

func TestSDK_ReadOnlyLocks(t *testing.T) {
	t.Parallel()

	c := newEnv(t, envOpts{}).client(t)

	_, err := c.SetSetting(t.Context(), "locked", new("v1"), nil)
	require.NoError(t, err)

	res, err := c.SetReadOnly(t.Context(), "locked", true, nil)
	require.NoError(t, err)
	assert.True(t, *res.IsReadOnly)

	_, err = c.SetSetting(t.Context(), "locked", new("v2"), nil)
	assert.Equal(t, http.StatusConflict, statusOf(err))

	_, err = c.DeleteSetting(t.Context(), "locked", nil)
	assert.Equal(t, http.StatusConflict, statusOf(err))

	got, err := c.GetSetting(t.Context(), "locked", nil)
	require.NoError(t, err)
	assert.True(t, *got.IsReadOnly)

	res, err = c.SetReadOnly(t.Context(), "locked", false, nil)
	require.NoError(t, err)
	assert.False(t, *res.IsReadOnly)

	_, err = c.SetSetting(t.Context(), "locked", new("v2"), nil)
	require.NoError(t, err)

	_, err = c.SetReadOnly(t.Context(), "missing", true, nil)
	assert.Equal(t, http.StatusNotFound, statusOf(err))
}

func TestSDK_RevisionsAndPointInTime(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	c := e.client(t)

	stamps := make([]time.Time, 0, 3)

	for _, v := range []string{"one", "two", "three"} {
		_, err := c.SetSetting(t.Context(), "rev", new(v), nil)
		require.NoError(t, err)

		stamps = append(stamps, e.clock.Now())
		e.clock.Advance(time.Minute)
	}

	var revs []string

	pager := c.NewListRevisionsPager(azappconfig.SettingSelector{KeyFilter: new("rev")}, nil)
	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		for _, s := range page.Settings {
			revs = append(revs, *s.Value)
		}
	}

	assert.Equal(t, []string{"one", "two", "three"}, revs)

	at := stamps[1].Add(time.Second)
	old, err := c.GetSetting(t.Context(), "rev", &azappconfig.GetSettingOptions{AcceptDateTime: &at})
	require.NoError(t, err)
	assert.Equal(t, "two", *old.Value)

	listed := collect(t, c, azappconfig.SettingSelector{KeyFilter: new("rev"), AcceptDateTime: &at})
	require.Len(t, listed, 1)
	assert.Equal(t, "two", *listed[0].Value)

	before := stamps[0].Add(-time.Hour)
	_, err = c.GetSetting(t.Context(), "rev", &azappconfig.GetSettingOptions{AcceptDateTime: &before})
	assert.Equal(t, http.StatusNotFound, statusOf(err))

	_, err = c.DeleteSetting(t.Context(), "rev", nil)
	require.NoError(t, err)
	e.clock.Advance(time.Minute)

	assert.Empty(t, collect(t, c, azappconfig.SettingSelector{KeyFilter: new("rev")}))

	again, err := c.GetSetting(t.Context(), "rev", &azappconfig.GetSettingOptions{AcceptDateTime: &at})
	require.NoError(t, err)
	assert.Equal(t, "two", *again.Value, "a deleted key is still readable at an earlier time")
}

func TestSDK_FeatureFlagAndKeyVaultReference(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		key         string
		contentType string
		value       string
	}{
		{
			name: "feature flag", key: ac.FeatureFlagPrefix + "beta", contentType: ac.ContentTypeFeatureFlag,
			value: `{"id":"beta","enabled":true,"conditions":{"client_filters":[]}}`,
		},
		{
			name: "key vault reference", key: "secrets/db", contentType: ac.ContentTypeKeyVaultRef,
			value: `{"uri":"https://vault.example/secrets/db"}`,
		},
	}

	c := newEnv(t, envOpts{}).client(t)

	for _, tt := range tests {
		_, err := c.SetSetting(t.Context(), tt.key, new(tt.value), &azappconfig.SetSettingOptions{
			ContentType: new(tt.contentType),
		})
		require.NoError(t, err, tt.name)

		got, err := c.GetSetting(t.Context(), tt.key, nil)
		require.NoError(t, err, tt.name)
		assert.Equal(t, tt.value, *got.Value, tt.name)
		assert.Equal(t, tt.contentType, *got.ContentType, tt.name)
		assert.Equal(t, tt.key, *got.Key, tt.name)
	}

	flags := collect(t, c, azappconfig.SettingSelector{KeyFilter: new(ac.FeatureFlagPrefix + "*")})
	assert.Equal(t, []string{ac.FeatureFlagPrefix + "beta"}, keysOf(flags))
}

func TestSDK_Snapshots(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	c := e.client(t)

	for _, s := range []struct{ key, label, val string }{
		{"app/a", "", "a0"}, {"app/a", "prod", "a1"}, {"app/b", "prod", "b1"}, {"other", "", "o"},
	} {
		opts := &azappconfig.SetSettingOptions{}
		if s.label != "" {
			opts.Label = new(s.label)
		}

		_, err := c.SetSetting(t.Context(), s.key, new(s.val), opts)
		require.NoError(t, err)
	}

	tests := []struct {
		name        string
		composition azappconfig.CompositionType
		want        []string
	}{
		{name: "snap-key", composition: azappconfig.CompositionTypeKey, want: []string{"app/a|prod", "app/b|prod"}},
		{
			name: "snap-keylabel", composition: azappconfig.CompositionTypeKeyLabel,
			want: []string{"app/a", "app/a|prod", "app/b|prod"},
		},
	}

	t.Run("compose", func(t *testing.T) {
		t.Parallel()

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				comp := tt.composition
				poller, err := c.BeginCreateSnapshot(t.Context(), tt.name, []azappconfig.SettingFilter{
					{KeyFilter: new("app/*")},
				}, &azappconfig.BeginCreateSnapshotOptions{CompositionType: &comp})
				require.NoError(t, err)

				created, err := poller.PollUntilDone(t.Context(), nil)
				require.NoError(t, err)
				assert.Equal(t, azappconfig.SnapshotStatusReady, *created.Status)
				assert.Equal(t, tt.composition, *created.CompositionType)
				assert.EqualValues(t, len(tt.want), *created.ItemsCount)

				var got []azappconfig.Setting

				pager := c.NewListSettingsForSnapshotPager(tt.name, nil)
				for pager.More() {
					page, pageErr := pager.NextPage(t.Context())
					require.NoError(t, pageErr)

					got = append(got, page.Settings...)
				}

				assert.Equal(t, tt.want, keysOf(got))
			})
		}
	})
}

func seededSnapshotClient(t *testing.T) *azappconfig.Client {
	t.Helper()

	c := newEnv(t, envOpts{}).client(t)

	for _, kv := range []struct{ key, label, val string }{
		{"app/a", "", "a0"}, {"app/a", "prod", "a1"}, {"app/b", "prod", "b1"}, {"other", "", "o"},
	} {
		opts := &azappconfig.SetSettingOptions{}
		if kv.label != "" {
			opts.Label = new(kv.label)
		}

		_, err := c.SetSetting(t.Context(), kv.key, new(kv.val), opts)
		require.NoError(t, err)
	}

	return c
}

func TestSDK_SnapshotLifecycle(t *testing.T) {
	t.Parallel()

	c := seededSnapshotClient(t)

	poller, err := c.BeginCreateSnapshot(t.Context(), "snap-life", []azappconfig.SettingFilter{
		{KeyFilter: new("other")},
	}, nil)
	require.NoError(t, err)
	_, err = poller.PollUntilDone(t.Context(), nil)
	require.NoError(t, err)

	_, err = c.SetSetting(t.Context(), "other", new("changed"), nil)
	require.NoError(t, err)

	got, err := c.GetSnapshot(t.Context(), "snap-life", nil)
	require.NoError(t, err)
	assert.Equal(t, azappconfig.SnapshotStatusReady, *got.Status)

	arch, err := c.ArchiveSnapshot(t.Context(), "snap-life", nil)
	require.NoError(t, err)
	assert.Equal(t, azappconfig.SnapshotStatusArchived, *arch.Status)
	assert.NotNil(t, arch.Expires)

	_, err = c.ArchiveSnapshot(t.Context(), "snap-life", nil)
	assert.Equal(t, http.StatusConflict, statusOf(err))

	rec, err := c.RecoverSnapshot(t.Context(), "snap-life", nil)
	require.NoError(t, err)
	assert.Equal(t, azappconfig.SnapshotStatusReady, *rec.Status)

	pager := c.NewListSettingsForSnapshotPager("snap-life", nil)
	page, err := pager.NextPage(t.Context())
	require.NoError(t, err)

	assert.Equal(t, "o", *page.Settings[0].Value, "snapshot content does not follow later writes")

	_, err = c.GetSnapshot(t.Context(), "nope", nil)
	assert.Equal(t, http.StatusNotFound, statusOf(err))

	names := 0
	lp := c.NewListSnapshotsPager(nil)

	for lp.More() {
		lpage, lerr := lp.NextPage(t.Context())
		require.NoError(t, lerr)

		names += len(lpage.Snapshots)
	}

	assert.GreaterOrEqual(t, names, 1)
}

func TestSDK_SnapshotDuplicateName(t *testing.T) {
	t.Parallel()

	c := seededSnapshotClient(t)

	poller, err := c.BeginCreateSnapshot(
		t.Context(),
		"snap-key",
		[]azappconfig.SettingFilter{{KeyFilter: new("*")}},
		nil,
	)
	require.NoError(t, err)
	_, err = poller.PollUntilDone(t.Context(), nil)
	require.NoError(t, err)

	_, err = c.BeginCreateSnapshot(t.Context(), "snap-key", []azappconfig.SettingFilter{{KeyFilter: new("*")}}, nil)
	assert.Equal(t, http.StatusConflict, statusOf(err))
}
