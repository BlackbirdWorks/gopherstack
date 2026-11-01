package azurefile_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func listNames(t *testing.T, e *env, o *service.ListSharesOptions) []string {
	t.Helper()

	var names []string

	pager := e.svc.NewListSharesPager(o)
	for pager.More() {
		page, err := pager.NextPage(ctx(t))
		require.NoError(t, err)

		for _, s := range page.Shares {
			name := *s.Name
			if s.Snapshot != nil {
				name += "@snap"
			}

			if s.Deleted != nil && *s.Deleted {
				name += "@deleted"
			}

			names = append(names, name)
		}
	}

	return names
}

func TestSDK_ServiceProperties(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, e *env)
		name  string
	}{
		{name: "defaults", check: func(t *testing.T, e *env) {
			t.Helper()

			got, err := e.svc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Empty(t, got.CORS)
			require.NotNil(t, got.HourMetrics)
			assert.Equal(t, "1.0", *got.HourMetrics.Version)
			assert.False(t, *got.MinuteMetrics.Enabled)
			require.NotNil(t, got.Protocol)
			assert.False(t, *got.Protocol.Smb.Multichannel.Enabled)
		}},
		{name: "round trip", check: func(t *testing.T, e *env) {
			t.Helper()

			_, err := e.svc.SetProperties(ctx(t), &service.SetPropertiesOptions{
				CORS: []*service.CORSRule{{
					AllowedOrigins: new("https://example.com"), AllowedMethods: new("GET,PUT"),
					AllowedHeaders: new("x-ms-*"), ExposedHeaders: new("x-ms-meta-*"), MaxAgeInSeconds: new(int32(60)),
				}},
				MinuteMetrics: &service.Metrics{
					Enabled: new(true), IncludeAPIs: new(true), Version: new("1.0"),
					RetentionPolicy: &service.RetentionPolicy{Enabled: new(true), Days: new(int32(3))},
				},
				Protocol: &service.ProtocolSettings{Smb: &service.SMBSettings{
					Multichannel: &service.SMBMultichannel{Enabled: new(true)},
				}},
			})
			require.NoError(t, err)

			got, err := e.svc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			require.Len(t, got.CORS, 1)
			assert.Equal(t, "GET,PUT", *got.CORS[0].AllowedMethods)
			assert.Equal(t, int32(60), *got.CORS[0].MaxAgeInSeconds)
			assert.True(t, *got.MinuteMetrics.Enabled)
			assert.Equal(t, int32(3), *got.MinuteMetrics.RetentionPolicy.Days)
			assert.True(t, *got.Protocol.Smb.Multichannel.Enabled)
			assert.True(t, *got.HourMetrics.Enabled, "unset sections are left unchanged")
		}},
		{name: "too many cors rules", check: func(t *testing.T, e *env) {
			t.Helper()

			rules := make([]*service.CORSRule, 6)
			for i := range rules {
				rules[i] = &service.CORSRule{
					AllowedOrigins: new("*"), AllowedMethods: new("GET"), AllowedHeaders: new("*"),
					ExposedHeaders: new("*"), MaxAgeInSeconds: new(int32(1)),
				}
			}

			_, err := e.svc.SetProperties(ctx(t), &service.SetPropertiesOptions{CORS: rules})
			requireCode(t, err, fileerror.InvalidXMLDocument)
		}},
		{name: "bad cors method", check: func(t *testing.T, e *env) {
			t.Helper()

			_, err := e.svc.SetProperties(ctx(t), &service.SetPropertiesOptions{CORS: []*service.CORSRule{{
				AllowedOrigins: new("*"), AllowedMethods: new("FETCH"), AllowedHeaders: new("*"),
				ExposedHeaders: new("*"), MaxAgeInSeconds: new(int32(1)),
			}}})
			requireCode(t, err, fileerror.InvalidXMLDocument)
		}},
		{name: "retention needs days", check: func(t *testing.T, e *env) {
			t.Helper()

			_, err := e.svc.SetProperties(ctx(t), &service.SetPropertiesOptions{HourMetrics: &service.Metrics{
				Enabled: new(true), IncludeAPIs: new(true), Version: new("1.0"),
				RetentionPolicy: &service.RetentionPolicy{Enabled: new(true)},
			}})
			requireCode(t, err, fileerror.InvalidXMLDocument)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.check(t, newEnv(t))
		})
	}
}

func TestSDK_ListShares(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, e *env)
		opts  *service.ListSharesOptions
		name  string
		want  []string
	}{
		{name: "empty", want: nil},
		{
			name:  "sorted all",
			setup: withShares("bravo", "alpha", "charlie"),
			want:  []string{"alpha", "bravo", "charlie"},
		},
		{
			name:  "prefix",
			setup: withShares("app-a", "app-b", "other"),
			opts:  &service.ListSharesOptions{Prefix: new("app")},
			want:  []string{"app-a", "app-b"},
		},
		{
			name:  "paged with max results",
			setup: withShares("sh1", "sh2", "sh3", "sh4", "sh5"),
			opts:  &service.ListSharesOptions{MaxResults: new(int32(2))},
			want:  []string{"sh1", "sh2", "sh3", "sh4", "sh5"},
		},
		{
			name: "snapshots included",
			setup: func(t *testing.T, e *env) {
				t.Helper()

				createShares(t, e, "snapme")
				_, err := e.svc.NewShareClient("snapme").CreateSnapshot(ctx(t), nil)
				require.NoError(t, err)
			},
			opts: &service.ListSharesOptions{Include: service.ListSharesInclude{Snapshots: true}},
			want: []string{"snapme", "snapme@snap"},
		},
		{
			name: "deleted need soft delete",
			setup: func(t *testing.T, e *env) {
				t.Helper()

				enableSoftDelete(t, e)
				createShares(t, e, "gone", "kept")
				_, err := e.svc.DeleteShare(ctx(t), "gone", nil)
				require.NoError(t, err)
			},
			opts: &service.ListSharesOptions{Include: service.ListSharesInclude{Deleted: true}},
			want: []string{"gone@deleted", "kept"},
		},
		{
			name: "deleted hidden by default",
			setup: func(t *testing.T, e *env) {
				t.Helper()

				enableSoftDelete(t, e)
				createShares(t, e, "gone", "kept")
				_, err := e.svc.DeleteShare(ctx(t), "gone", nil)
				require.NoError(t, err)
			},
			want: []string{"kept"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			if tt.setup != nil {
				tt.setup(t, e)
			}

			assert.Equal(t, tt.want, listNames(t, e, tt.opts))
		})
	}
}

func TestSDK_ListSharesMetadata(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	_, err := e.svc.CreateShare(ctx(t), "withmeta", &service.CreateShareOptions{
		Metadata: map[string]*string{"Env": new("test")}, Quota: new(int32(10)), AccessTier: new(share.AccessTierHot),
	})
	require.NoError(t, err)

	for _, include := range []bool{false, true} {
		t.Run(fmt.Sprintf("include=%v", include), func(t *testing.T) {
			t.Parallel()

			pager := e.svc.NewListSharesPager(
				&service.ListSharesOptions{Include: service.ListSharesInclude{Metadata: include}},
			)
			page, pageErr := pager.NextPage(ctx(t))
			require.NoError(t, pageErr)
			require.Len(t, page.Shares, 1)

			s := page.Shares[0]
			assert.Equal(t, int32(10), *s.Properties.Quota)
			assert.Equal(t, "Hot", *s.Properties.AccessTier)

			if include {
				assert.Equal(t, "test", md(s.Metadata, "env"))
			} else {
				assert.Empty(t, s.Metadata)
			}
		})
	}
}

func createShares(t *testing.T, e *env, names ...string) {
	t.Helper()

	for _, n := range names {
		_, err := e.svc.CreateShare(ctx(t), n, nil)
		require.NoError(t, err)
	}
}

func enableSoftDelete(t *testing.T, e *env) {
	t.Helper()

	e.setShareRetention(t, 7)
}

func withShares(names ...string) func(*testing.T, *env) {
	return func(t *testing.T, e *env) {
		t.Helper()
		createShares(t, e, names...)
	}
}

func TestSDK_DataPlaneIgnoresRetentionElement(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	body := `<StorageServiceProperties><ShareDeleteRetentionPolicy><Enabled>true</Enabled><Days>5</Days>` +
		`</ShareDeleteRetentionPolicy></StorageServiceProperties>`

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut,
		e.url+"/devstoreaccount1?restype=service&comp=properties", strings.NewReader(body))
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	enabled, _ := e.h.Backend.ShareRetention("devstoreaccount1")
	assert.False(t, enabled, "soft delete is configured through ARM only")

	sc := e.svc.NewShareClient("stillhard")
	_, err = sc.Create(ctx(t), nil)
	require.NoError(t, err)
	_, err = sc.Delete(ctx(t), nil)
	require.NoError(t, err)
	assert.Empty(t, listNames(t, e, &service.ListSharesOptions{Include: service.ListSharesInclude{Deleted: true}}))
}
