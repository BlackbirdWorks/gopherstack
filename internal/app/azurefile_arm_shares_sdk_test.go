package app

import (
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/storage/armstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func armShareStatus(t *testing.T, err error) int {
	t.Helper()

	var re *azcore.ResponseError
	require.ErrorAs(t, err, &re)

	return re.StatusCode
}

func TestAzureFileARMShares(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, s *afStack)
		name  string
	}{
		{name: "create defaults and data plane visibility", check: func(t *testing.T, s *afStack) {
			t.Helper()

			got, err := s.fileShares.Create(t.Context(), afRG, afAccount, "defaults", armstorage.FileShare{
				FileShareProperties: &armstorage.FileShareProperties{
					Metadata: map[string]*string{"team": new("a")},
				},
			}, nil)
			require.NoError(t, err)

			p := got.FileShareProperties
			assert.Equal(t, "defaults", *got.Name)
			assert.Equal(t, "Microsoft.Storage/storageAccounts/fileServices/shares", *got.Type)
			assert.Equal(t, int32(5120), *p.ShareQuota)
			assert.Equal(t, armstorage.ShareAccessTierTransactionOptimized, *p.AccessTier)
			assert.Equal(t, armstorage.EnabledProtocolsSMB, *p.EnabledProtocols)
			assert.Equal(t, armstorage.LeaseStateAvailable, *p.LeaseState)
			assert.Equal(t, armstorage.LeaseStatusUnlocked, *p.LeaseStatus)
			assert.Equal(t, "a", *p.Metadata["team"])
			assert.NotNil(t, p.LastModifiedTime)

			props, err := s.shares.NewShareClient("defaults").GetProperties(t.Context(), nil)
			require.NoError(t, err)
			assert.Equal(t, int32(5120), *props.Quota)
		}},
		{name: "create with options", check: func(t *testing.T, s *afStack) {
			t.Helper()

			got, err := s.fileShares.Create(t.Context(), afRG, afAccount, "nfsshare", armstorage.FileShare{
				FileShareProperties: &armstorage.FileShareProperties{
					ShareQuota: new(int32(100)),
					AccessTier: new(armstorage.ShareAccessTierPremium),
					EnabledProtocols: new(
						armstorage.EnabledProtocolsNFS,
					),
					RootSquash: new(armstorage.RootSquashTypeAllSquash),
					SignedIdentifiers: []*armstorage.SignedIdentifier{{
						ID:           new("p1"),
						AccessPolicy: &armstorage.AccessPolicy{Permission: new("rw")},
					}},
				},
			}, nil)
			require.NoError(t, err)
			assert.Equal(t, int32(100), *got.FileShareProperties.ShareQuota)
			assert.Equal(t, armstorage.EnabledProtocolsNFS, *got.FileShareProperties.EnabledProtocols)
			assert.Equal(t, armstorage.RootSquashTypeAllSquash, *got.FileShareProperties.RootSquash)
			require.Len(t, got.FileShareProperties.SignedIdentifiers, 1)
			assert.Equal(t, "rw", *got.FileShareProperties.SignedIdentifiers[0].AccessPolicy.Permission)
		}},
		{name: "update and get", check: func(t *testing.T, s *afStack) {
			t.Helper()

			_, err := s.fileShares.Create(t.Context(), afRG, afAccount, "upd", armstorage.FileShare{}, nil)
			require.NoError(t, err)

			_, err = s.fileShares.Update(t.Context(), afRG, afAccount, "upd", armstorage.FileShare{
				FileShareProperties: &armstorage.FileShareProperties{
					ShareQuota: new(int32(7)),
					Metadata:   map[string]*string{"k": new("v")},
				},
			}, nil)
			require.NoError(t, err)

			got, err := s.fileShares.Get(t.Context(), afRG, afAccount, "upd", nil)
			require.NoError(t, err)
			assert.Equal(t, int32(7), *got.FileShareProperties.ShareQuota)
			assert.Equal(t, "v", *got.FileShareProperties.Metadata["k"])
			assert.Nil(t, got.FileShareProperties.ShareUsageBytes)

			stats, err := s.fileShares.Get(
				t.Context(),
				afRG,
				afAccount,
				"upd",
				&armstorage.FileSharesClientGetOptions{Expand: new("stats")},
			)
			require.NoError(t, err)
			assert.Equal(t, int64(0), *stats.FileShareProperties.ShareUsageBytes)
		}},
		{name: "invalid input", check: func(t *testing.T, s *afStack) {
			t.Helper()

			_, err := s.fileShares.Create(t.Context(), afRG, afAccount, "badquota", armstorage.FileShare{
				FileShareProperties: &armstorage.FileShareProperties{ShareQuota: new(int32(0))},
			}, nil)
			require.Error(t, err)
			assert.Equal(t, http.StatusBadRequest, armShareStatus(t, err))

			_, err = s.fileShares.Create(t.Context(), afRG, afAccount, "UPPER", armstorage.FileShare{}, nil)
			require.Error(t, err)
			assert.Equal(t, http.StatusBadRequest, armShareStatus(t, err))
		}},
		{name: "missing share and account", check: func(t *testing.T, s *afStack) {
			t.Helper()

			_, err := s.fileShares.Get(t.Context(), afRG, afAccount, "ghost", nil)
			require.Error(t, err)
			assert.Equal(t, http.StatusNotFound, armShareStatus(t, err))

			_, err = s.fileShares.Update(t.Context(), afRG, afAccount, "ghost", armstorage.FileShare{}, nil)
			require.Error(t, err)
			assert.Equal(t, http.StatusNotFound, armShareStatus(t, err))

			_, err = s.fileShares.Get(t.Context(), afRG, "noacct", "ghost", nil)
			require.Error(t, err)
			assert.Equal(t, http.StatusNotFound, armShareStatus(t, err))
		}},
		{name: "list pages", check: func(t *testing.T, s *afStack) {
			t.Helper()

			for _, n := range []string{"list-c", "list-a", "list-b"} {
				_, err := s.fileShares.Create(t.Context(), afRG, afAccount, n, armstorage.FileShare{}, nil)
				require.NoError(t, err)
			}

			pager := s.fileShares.NewListPager(
				afRG,
				afAccount,
				&armstorage.FileSharesClientListOptions{Maxpagesize: new("2")},
			)

			var names []string

			pages := 0

			for pager.More() {
				page, err := pager.NextPage(t.Context())
				require.NoError(t, err)

				pages++

				for _, it := range page.Value {
					names = append(names, *it.Name)
				}
			}

			assert.Equal(t, []string{"list-a", "list-b", "list-c"}, names)
			assert.Equal(t, 2, pages)
		}},
		{name: "snapshots and delete modes", check: func(t *testing.T, s *afStack) {
			t.Helper()

			_, err := s.fileShares.Create(t.Context(), afRG, afAccount, "snappy", armstorage.FileShare{}, nil)
			require.NoError(t, err)

			snap, err := s.shares.NewShareClient("snappy").CreateSnapshot(t.Context(), nil)
			require.NoError(t, err)

			got, err := s.fileShares.Get(
				t.Context(),
				afRG,
				afAccount,
				"snappy",
				&armstorage.FileSharesClientGetOptions{XMSSnapshot: snap.Snapshot},
			)
			require.NoError(t, err)
			assert.NotNil(t, got.FileShareProperties.SnapshotTime)

			pager := s.fileShares.NewListPager(
				afRG,
				afAccount,
				&armstorage.FileSharesClientListOptions{Expand: new("snapshots")},
			)
			total := 0

			for pager.More() {
				page, perr := pager.NextPage(t.Context())
				require.NoError(t, perr)

				for _, it := range page.Value {
					if *it.Name == "snappy" {
						total++
					}
				}
			}

			assert.Equal(t, 2, total)

			_, err = s.fileShares.Delete(
				t.Context(),
				afRG,
				afAccount,
				"snappy",
				&armstorage.FileSharesClientDeleteOptions{Include: new("none")},
			)
			require.Error(t, err)
			assert.Equal(t, http.StatusConflict, armShareStatus(t, err))

			_, err = s.fileShares.Delete(t.Context(), afRG, afAccount, "snappy", nil)
			require.NoError(t, err)

			_, err = s.fileShares.Get(t.Context(), afRG, afAccount, "snappy", nil)
			assert.Equal(t, http.StatusNotFound, armShareStatus(t, err))

			_, err = s.fileShares.Delete(t.Context(), afRG, afAccount, "snappy", nil)
			require.NoError(t, err)
		}},
		{name: "soft delete lists deleted", check: func(t *testing.T, s *afStack) {
			t.Helper()
			require.NoError(t, s.setRetention(t, true, 5))

			_, err := s.fileShares.Create(t.Context(), afRG, afAccount, "soft", armstorage.FileShare{}, nil)
			require.NoError(t, err)
			_, err = s.fileShares.Delete(t.Context(), afRG, afAccount, "soft", nil)
			require.NoError(t, err)

			pager := s.fileShares.NewListPager(
				afRG,
				afAccount,
				&armstorage.FileSharesClientListOptions{Expand: new("deleted")},
			)
			found := false

			for pager.More() {
				page, perr := pager.NextPage(t.Context())
				require.NoError(t, perr)

				for _, it := range page.Value {
					if *it.Name == "soft" {
						found = true

						assert.True(t, *it.Properties.Deleted)
						assert.NotNil(t, it.Properties.DeletedTime)
						assert.Equal(t, int32(5), *it.Properties.RemainingRetentionDays)
					}
				}
			}

			assert.True(t, found)

			_, err = s.fileShares.Restore(t.Context(), afRG, afAccount, "restored", armstorage.DeletedShare{
				DeletedShareName: new("soft"), DeletedShareVersion: new(deletedVersion(t, s, "soft")),
			}, nil)
			require.NoError(t, err)

			_, err = s.fileShares.Get(t.Context(), afRG, afAccount, "restored", nil)
			require.NoError(t, err)

			_, err = s.fileShares.Restore(t.Context(), afRG, afAccount, "restored", armstorage.DeletedShare{
				DeletedShareName: new("soft"), DeletedShareVersion: new("nope"),
			}, nil)
			require.Error(t, err)
			assert.Equal(t, http.StatusConflict, armShareStatus(t, err))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.check(t, newAFStack(t))
		})
	}
}

func deletedVersion(t *testing.T, s *afStack, name string) string {
	t.Helper()

	pager := s.fileShares.NewListPager(afRG, afAccount, &armstorage.FileSharesClientListOptions{Expand: new("deleted")})

	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		for _, it := range page.Value {
			if *it.Name == name && it.Properties.Version != nil {
				return *it.Properties.Version
			}
		}
	}

	require.FailNow(t, "deleted share not listed")

	return ""
}
