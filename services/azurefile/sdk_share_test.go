package azurefile_test

import (
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/lease"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDK_ShareLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, e *env)
		name  string
	}{
		{name: "create get properties", check: func(t *testing.T, e *env) {
			t.Helper()

			sc := e.svc.NewShareClient("lifecycle")
			_, err := sc.Create(ctx(t), &share.CreateOptions{
				Quota: new(int32(100)), AccessTier: new(share.AccessTierCool),
				Metadata: map[string]*string{"Team": new("core")},
			})
			require.NoError(t, err)

			props, err := sc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, int32(100), *props.Quota)
			assert.Equal(t, "Cool", *props.AccessTier)
			assert.Equal(t, "SMB", *props.EnabledProtocols)
			assert.Equal(t, "core", md(props.Metadata, "team"))
			assert.NotEmpty(t, props.ETag)
			assert.Equal(t, lease.StateTypeAvailable, *props.LeaseState)
			assert.Equal(t, lease.StatusTypeUnlocked, *props.LeaseStatus)
		}},
		{name: "defaults", check: func(t *testing.T, e *env) {
			t.Helper()

			sc := e.svc.NewShareClient("defaults")
			_, err := sc.Create(ctx(t), nil)
			require.NoError(t, err)

			props, err := sc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, int32(5120), *props.Quota)
			assert.Equal(t, "TransactionOptimized", *props.AccessTier)
		}},
		{name: "duplicate create", check: func(t *testing.T, e *env) {
			t.Helper()

			sc := e.svc.NewShareClient("dup-share")
			_, err := sc.Create(ctx(t), nil)
			require.NoError(t, err)

			_, err = sc.Create(ctx(t), nil)
			requireCode(t, err, fileerror.ShareAlreadyExists)
		}},
		{name: "invalid name", check: func(t *testing.T, e *env) {
			t.Helper()

			_, err := e.svc.NewShareClient("Bad_Name").Create(ctx(t), nil)
			requireCode(t, err, fileerror.InvalidResourceName)
		}},
		{name: "invalid quota", check: func(t *testing.T, e *env) {
			t.Helper()

			_, err := e.svc.NewShareClient("quota-bad").Create(ctx(t), &share.CreateOptions{Quota: new(int32(200000))})
			requireCode(t, err, fileerror.InvalidHeaderValue)
		}},
		{name: "nfs protocol", check: func(t *testing.T, e *env) {
			t.Helper()

			sc := e.svc.NewShareClient("nfs-share")
			_, err := sc.Create(
				ctx(t),
				&share.CreateOptions{EnabledProtocols: new("NFS"), RootSquash: new(share.RootSquashRootSquash)},
			)
			require.NoError(t, err)

			props, err := sc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, "NFS", *props.EnabledProtocols)
			assert.Equal(t, share.RootSquashRootSquash, *props.RootSquash)
		}},
		{name: "root squash needs nfs", check: func(t *testing.T, e *env) {
			t.Helper()

			_, err := e.svc.NewShareClient("smb-squash").
				Create(ctx(t), &share.CreateOptions{RootSquash: new(share.RootSquashAllSquash)})
			requireCode(t, err, fileerror.InvalidHeaderValue)
		}},
		{name: "set properties", check: func(t *testing.T, e *env) {
			t.Helper()

			sc := e.svc.NewShareClient("setprops")
			_, err := sc.Create(ctx(t), nil)
			require.NoError(t, err)

			_, err = sc.SetProperties(
				ctx(t),
				&share.SetPropertiesOptions{Quota: new(int32(7)), AccessTier: new(share.AccessTierHot)},
			)
			require.NoError(t, err)

			props, err := sc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, int32(7), *props.Quota)
			assert.Equal(t, "Hot", *props.AccessTier)
		}},
		{name: "set metadata replaces", check: func(t *testing.T, e *env) {
			t.Helper()

			sc := e.svc.NewShareClient("setmeta")
			_, err := sc.Create(ctx(t), &share.CreateOptions{Metadata: map[string]*string{"One": new("1")}})
			require.NoError(t, err)

			_, err = sc.SetMetadata(ctx(t), &share.SetMetadataOptions{Metadata: map[string]*string{"Two": new("2")}})
			require.NoError(t, err)

			props, err := sc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Len(t, props.Metadata, 1)
			assert.Equal(t, "2", md(props.Metadata, "two"))
		}},
		{name: "invalid metadata", check: func(t *testing.T, e *env) {
			t.Helper()

			sc := e.svc.NewShareClient("badmeta")
			_, err := sc.Create(ctx(t), nil)
			require.NoError(t, err)

			_, err = sc.SetMetadata(ctx(t), &share.SetMetadataOptions{Metadata: map[string]*string{"1bad": new("x")}})
			requireCode(t, err, fileerror.InvalidMetadata)
		}},
		{name: "delete", check: func(t *testing.T, e *env) {
			t.Helper()

			sc := e.svc.NewShareClient("deleteme")
			_, err := sc.Create(ctx(t), nil)
			require.NoError(t, err)

			_, err = sc.Delete(ctx(t), nil)
			require.NoError(t, err)

			_, err = sc.GetProperties(ctx(t), nil)
			requireCode(t, err, fileerror.ShareNotFound)

			_, err = sc.Delete(ctx(t), nil)
			requireCode(t, err, fileerror.ShareNotFound)
		}},
		{name: "case insensitive account", check: func(t *testing.T, e *env) {
			t.Helper()

			_, err := e.svc.NewShareClient("casey").Create(ctx(t), nil)
			require.NoError(t, err)

			other, err := service.NewClientWithSharedKeyCredential(e.url+"/DevStoreAccount1/", e.cred, nil)
			require.NoError(t, err)

			_, err = other.NewShareClient("casey").GetProperties(ctx(t), nil)
			require.NoError(t, err)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.check(t, newEnv(t))
		})
	}
}

func TestSDK_ShareAccounts(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	_, err := e.svc.NewShareClient("acct-one").Create(ctx(t), nil)
	require.NoError(t, err)

	other, err := service.NewClientWithSharedKeyCredential(e.url+"/otheraccount/", e.cred, nil)
	require.NoError(t, err)

	_, err = other.NewShareClient("acct-one").GetProperties(ctx(t), nil)
	requireCode(t, err, fileerror.ShareNotFound)

	_, err = other.NewShareClient("acct-one").Create(ctx(t), nil)
	require.NoError(t, err)
}

func TestSDK_ShareSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, e *env, sc *share.Client)
		name  string
	}{
		{name: "snapshot readable and isolated", check: func(t *testing.T, _ *env, sc *share.Client) {
			t.Helper()

			fc := sc.NewRootDirectoryClient().NewFileClient("data.txt")
			_, err := fc.Create(ctx(t), 5, nil)
			require.NoError(t, err)
			_, err = fc.UploadRange(ctx(t), 0, newReadSeekCloser("hello"), nil)
			require.NoError(t, err)

			snap, err := sc.CreateSnapshot(
				ctx(t),
				&share.CreateSnapshotOptions{Metadata: map[string]*string{"K": new("v")}},
			)
			require.NoError(t, err)
			require.NotNil(t, snap.Snapshot)

			_, err = fc.UploadRange(ctx(t), 0, newReadSeekCloser("HELLO"), nil)
			require.NoError(t, err)

			snapShare, err := sc.WithSnapshot(*snap.Snapshot)
			require.NoError(t, err)

			got := downloadAll(t, snapShare.NewRootDirectoryClient().NewFileClient("data.txt"))
			assert.Equal(t, "hello", got)
			assert.Equal(t, "HELLO", downloadAll(t, fc))

			props, err := snapShare.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, "v", md(props.Metadata, "k"))
		}},
		{name: "snapshot is read only", check: func(t *testing.T, _ *env, sc *share.Client) {
			t.Helper()

			snap, err := sc.CreateSnapshot(ctx(t), nil)
			require.NoError(t, err)

			snapShare, err := sc.WithSnapshot(*snap.Snapshot)
			require.NoError(t, err)

			_, err = snapShare.NewDirectoryClient("newdir").Create(ctx(t), nil)
			requireCode(t, err, fileerror.Code("ShareSnapshotOperationNotSupported"))
		}},
		{name: "delete needs include", check: func(t *testing.T, _ *env, sc *share.Client) {
			t.Helper()

			_, err := sc.CreateSnapshot(ctx(t), nil)
			require.NoError(t, err)

			_, err = sc.Delete(ctx(t), nil)
			requireCode(t, err, fileerror.ShareHasSnapshots)

			_, err = sc.Delete(
				ctx(t),
				&share.DeleteOptions{DeleteSnapshots: new(share.DeleteSnapshotsOptionTypeInclude)},
			)
			require.NoError(t, err)
		}},
		{name: "delete one snapshot", check: func(t *testing.T, _ *env, sc *share.Client) {
			t.Helper()

			snap, err := sc.CreateSnapshot(ctx(t), nil)
			require.NoError(t, err)

			snapShare, err := sc.WithSnapshot(*snap.Snapshot)
			require.NoError(t, err)

			_, err = snapShare.Delete(ctx(t), nil)
			require.NoError(t, err)

			_, err = snapShare.GetProperties(ctx(t), nil)
			requireCode(t, err, fileerror.ShareNotFound)

			_, err = sc.Delete(ctx(t), nil)
			require.NoError(t, err)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			sc := e.svc.NewShareClient("snapshare")
			_, err := sc.Create(ctx(t), nil)
			require.NoError(t, err)

			tt.check(t, e, sc)
		})
	}
}

func TestSDK_ShareACLStatsPermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, sc *share.Client)
		name  string
	}{
		{name: "acl round trip", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			start := time.Now().UTC().Truncate(time.Second)
			_, err := sc.SetAccessPolicy(ctx(t), &share.SetAccessPolicyOptions{ShareACL: []*share.SignedIdentifier{{
				ID: new("policy-1"),
				AccessPolicy: &share.AccessPolicy{
					Start: &start, Expiry: new(start.Add(time.Hour)), Permission: new("rwd"),
				},
			}}})
			require.NoError(t, err)

			got, err := sc.GetAccessPolicy(ctx(t), nil)
			require.NoError(t, err)
			require.Len(t, got.SignedIdentifiers, 1)
			assert.Equal(t, "policy-1", *got.SignedIdentifiers[0].ID)
			assert.Equal(t, "rwd", *got.SignedIdentifiers[0].AccessPolicy.Permission)
			assert.True(t, start.Equal(*got.SignedIdentifiers[0].AccessPolicy.Start))
		}},
		{name: "acl cleared", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.SetAccessPolicy(
				ctx(t),
				&share.SetAccessPolicyOptions{ShareACL: []*share.SignedIdentifier{{ID: new("p")}}},
			)
			require.NoError(t, err)

			_, err = sc.SetAccessPolicy(ctx(t), nil)
			require.NoError(t, err)

			got, err := sc.GetAccessPolicy(ctx(t), nil)
			require.NoError(t, err)
			assert.Empty(t, got.SignedIdentifiers)
		}},
		{name: "acl max five", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			ids := make([]*share.SignedIdentifier, 6)
			for i := range ids {
				ids[i] = &share.SignedIdentifier{ID: new(string(rune('a' + i)))}
			}

			_, err := sc.SetAccessPolicy(ctx(t), &share.SetAccessPolicyOptions{ShareACL: ids})
			requireCode(t, err, fileerror.InvalidXMLDocument)
		}},
		{name: "stats sum file sizes", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			root := sc.NewRootDirectoryClient()
			_, err := root.NewFileClient("a.bin").Create(ctx(t), 1000, nil)
			require.NoError(t, err)
			_, err = sc.NewDirectoryClient("sub").Create(ctx(t), nil)
			require.NoError(t, err)
			_, err = sc.NewDirectoryClient("sub").NewFileClient("b.bin").Create(ctx(t), 24, nil)
			require.NoError(t, err)

			stats, err := sc.GetStatistics(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, int64(1024), *stats.ShareUsageBytes)
		}},
		{name: "permission round trip", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			const sddl = "O:S-1-5-32-544G:S-1-5-32-544D:(A;;FA;;;SY)"

			created, err := sc.CreatePermission(ctx(t), sddl, nil)
			require.NoError(t, err)
			require.NotEmpty(t, created.FilePermissionKey)

			again, err := sc.CreatePermission(ctx(t), sddl, nil)
			require.NoError(t, err)
			assert.Equal(t, *created.FilePermissionKey, *again.FilePermissionKey, "same SDDL, same key")

			got, err := sc.GetPermission(ctx(t), *created.FilePermissionKey, nil)
			require.NoError(t, err)
			assert.Equal(t, sddl, *got.Permission)
		}},
		{name: "unknown permission key", check: func(t *testing.T, sc *share.Client) {
			t.Helper()

			_, err := sc.GetPermission(ctx(t), "999*999", nil)
			requireCode(t, err, fileerror.InvalidHeaderValue)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			sc := e.svc.NewShareClient("aclshare")
			_, err := sc.Create(ctx(t), nil)
			require.NoError(t, err)

			tt.check(t, sc)
		})
	}
}

func TestSDK_ShareLease(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, sc *share.Client, lc *lease.ShareClient)
		name  string
	}{
		{name: "acquire blocks delete", check: func(t *testing.T, sc *share.Client, lc *lease.ShareClient) {
			t.Helper()

			_, err := lc.Acquire(ctx(t), -1, nil)
			require.NoError(t, err)

			props, err := sc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, lease.StateTypeLeased, *props.LeaseState)
			assert.Equal(t, lease.DurationTypeInfinite, *props.LeaseDuration)
			assert.Equal(t, lease.StatusTypeLocked, *props.LeaseStatus)

			_, err = sc.Delete(ctx(t), nil)
			requireCode(t, err, fileerror.Code("LeaseIdMissing"))

			_, err = sc.SetMetadata(ctx(t), nil)
			requireCode(t, err, fileerror.Code("LeaseIdMissing"))
		}},
		{name: "delete with lease id", check: func(t *testing.T, sc *share.Client, lc *lease.ShareClient) {
			t.Helper()

			_, err := lc.Acquire(ctx(t), -1, nil)
			require.NoError(t, err)

			_, err = sc.Delete(
				ctx(t),
				&share.DeleteOptions{LeaseAccessConditions: &share.LeaseAccessConditions{LeaseID: lc.LeaseID()}},
			)
			require.NoError(t, err)
		}},
		{name: "wrong lease id", check: func(t *testing.T, sc *share.Client, lc *lease.ShareClient) {
			t.Helper()

			_, err := lc.Acquire(ctx(t), -1, nil)
			require.NoError(t, err)

			_, err = sc.SetMetadata(
				ctx(t),
				&share.SetMetadataOptions{LeaseAccessConditions: &share.LeaseAccessConditions{LeaseID: new("nope")}},
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "LeaseIdMismatch")
		}},
		{name: "double acquire", check: func(t *testing.T, sc *share.Client, lc *lease.ShareClient) {
			t.Helper()

			_, err := lc.Acquire(ctx(t), -1, nil)
			require.NoError(t, err)

			other, err := lease.NewShareClient(
				sc,
				&lease.ShareClientOptions{LeaseID: new("5f0f3a4e-0000-4000-8000-000000000001")},
			)
			require.NoError(t, err)

			_, err = other.Acquire(ctx(t), -1, nil)
			requireCode(t, err, fileerror.Code("LeaseAlreadyPresent"))
		}},
		{name: "fixed duration validated", check: func(t *testing.T, sc *share.Client, lc *lease.ShareClient) {
			t.Helper()

			_, err := lc.Acquire(ctx(t), 5, nil)
			requireCode(t, err, fileerror.InvalidHeaderValue)

			_, err = lc.Acquire(ctx(t), 30, nil)
			require.NoError(t, err)

			props, err := sc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, lease.DurationTypeFixed, *props.LeaseDuration)

			_, err = lc.Renew(ctx(t), nil)
			require.NoError(t, err)
		}},
		{name: "release renew change break", check: func(t *testing.T, sc *share.Client, lc *lease.ShareClient) {
			t.Helper()

			_, err := lc.Acquire(ctx(t), -1, nil)
			require.NoError(t, err)

			changed, err := lc.Change(ctx(t), "6f0f3a4e-0000-4000-8000-000000000002", nil)
			require.NoError(t, err)
			assert.Equal(t, "6f0f3a4e-0000-4000-8000-000000000002", *changed.LeaseID)

			_, err = lc.Break(ctx(t), nil)
			require.NoError(t, err)

			props, err := sc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, lease.StateTypeBroken, *props.LeaseState)

			_, err = lc.Renew(ctx(t), nil)
			requireCode(t, err, fileerror.Code("LeaseIsBrokenAndCannotBeRenewed"))

			_, err = lc.Release(ctx(t), nil)
			require.NoError(t, err)

			_, err = lc.Release(ctx(t), nil)
			requireCode(t, err, fileerror.Code("LeaseNotPresentWithLeaseOperation"))
		}},
		{name: "break with period", check: func(t *testing.T, sc *share.Client, lc *lease.ShareClient) {
			t.Helper()

			_, err := lc.Acquire(ctx(t), 60, nil)
			require.NoError(t, err)

			broke, err := lc.Break(ctx(t), &lease.ShareBreakOptions{BreakPeriod: new(int32(30))})
			require.NoError(t, err)
			assert.Equal(t, int32(30), *broke.LeaseTime)

			props, err := sc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, lease.StateTypeBreaking, *props.LeaseState)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			sc := e.svc.NewShareClient("leaseshare")
			_, err := sc.Create(ctx(t), nil)
			require.NoError(t, err)

			lc, err := lease.NewShareClient(sc, nil)
			require.NoError(t, err)

			tt.check(t, sc, lc)
		})
	}
}

func TestSDK_ShareLeaseExpiry(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	e := newEnv(t, func(h *azurefileHandler) { h.Now = func() time.Time { return now } })

	sc := e.svc.NewShareClient("expiring")
	_, err := sc.Create(ctx(t), nil)
	require.NoError(t, err)

	lc, err := lease.NewShareClient(sc, nil)
	require.NoError(t, err)
	_, err = lc.Acquire(ctx(t), 15, nil)
	require.NoError(t, err)

	now = now.Add(16 * time.Second)

	props, err := sc.GetProperties(ctx(t), nil)
	require.NoError(t, err)
	assert.Equal(t, lease.StateTypeExpired, *props.LeaseState)

	_, err = sc.SetMetadata(ctx(t), nil)
	require.NoError(t, err, "expired lease no longer guards the share")
}

func TestSDK_ShareSoftDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, e *env)
		name  string
	}{
		{name: "restore", check: func(t *testing.T, e *env) {
			t.Helper()

			sc := e.svc.NewShareClient("softshare")
			_, err := sc.Create(ctx(t), &share.CreateOptions{Metadata: map[string]*string{"Keep": new("me")}})
			require.NoError(t, err)

			_, err = sc.Delete(ctx(t), nil)
			require.NoError(t, err)

			version := deletedVersion(t, e, "softshare")

			_, err = e.svc.RestoreShare(ctx(t), "softshare", version, nil)
			require.NoError(t, err)

			props, err := sc.GetProperties(ctx(t), nil)
			require.NoError(t, err)
			assert.Equal(t, "me", md(props.Metadata, "keep"))
		}},
		{name: "restore conflicts with live share", check: func(t *testing.T, e *env) {
			t.Helper()

			sc := e.svc.NewShareClient("softshare")
			_, err := sc.Create(ctx(t), nil)
			require.NoError(t, err)
			_, err = sc.Delete(ctx(t), nil)
			require.NoError(t, err)

			version := deletedVersion(t, e, "softshare")

			_, err = sc.Create(ctx(t), nil)
			require.NoError(t, err)

			_, err = e.svc.RestoreShare(ctx(t), "softshare", version, nil)
			requireCode(t, err, fileerror.ShareAlreadyExists)
		}},
		{name: "restore unknown version", check: func(t *testing.T, e *env) {
			t.Helper()

			_, err := e.svc.RestoreShare(ctx(t), "ghost", "v1", nil)
			requireCode(t, err, fileerror.ShareNotFound)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.setShareRetention(t, 5)
			tt.check(t, e)
		})
	}
}

func TestSDK_HardDeleteWithoutRetention(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sc := e.svc.NewShareClient("hardgone")
	_, err := sc.Create(ctx(t), nil)
	require.NoError(t, err)
	_, err = sc.Delete(ctx(t), nil)
	require.NoError(t, err)

	assert.Empty(t, listNames(t, e, &service.ListSharesOptions{Include: service.ListSharesInclude{Deleted: true}}))
}

func TestSDK_SoftDeleteExpires(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	e := newEnv(t, func(h *azurefileHandler) { h.Now = func() time.Time { return now } })
	e.setShareRetention(t, 2)

	sc := e.svc.NewShareClient("expiring")
	_, err := sc.Create(ctx(t), nil)
	require.NoError(t, err)
	_, err = sc.Delete(ctx(t), nil)
	require.NoError(t, err)

	version := deletedVersion(t, e, "expiring")

	now = now.Add(49 * time.Hour)

	_, err = e.svc.RestoreShare(ctx(t), "expiring", version, nil)
	requireCode(t, err, fileerror.ShareNotFound)
}

func deletedVersion(t *testing.T, e *env, name string) string {
	t.Helper()

	pager := e.svc.NewListSharesPager(&service.ListSharesOptions{Include: service.ListSharesInclude{Deleted: true}})
	for pager.More() {
		page, err := pager.NextPage(ctx(t))
		require.NoError(t, err)

		for _, s := range page.Shares {
			if s.Deleted != nil && *s.Deleted && *s.Name == name {
				require.NotNil(t, s.Version)
				assert.NotNil(t, s.Properties.DeletedTime)
				assert.NotNil(t, s.Properties.RemainingRetentionDays)

				return *s.Version
			}
		}
	}

	t.Fatalf("no deleted share %q listed", name)

	return ""
}
