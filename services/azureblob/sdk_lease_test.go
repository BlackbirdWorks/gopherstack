package azureblob_test

import (
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/lease"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	leaseA = "11111111-1111-1111-1111-111111111111"
	leaseB = "22222222-2222-2222-2222-222222222222"
)

func newBlobLease(t *testing.T, bb *blockblob.Client, id string) *lease.BlobClient {
	t.Helper()

	lc, err := lease.NewBlobClient(bb, &lease.BlobClientOptions{LeaseID: &id})
	require.NoError(t, err)

	return lc
}

func leaseState(t *testing.T, bb *blockblob.Client) (string, string, string) {
	t.Helper()

	props, err := bb.GetProperties(t.Context(), nil)
	require.NoError(t, err)

	dur := ""
	if props.LeaseDuration != nil {
		dur = string(*props.LeaseDuration)
	}

	return string(*props.LeaseState), string(*props.LeaseStatus), dur
}

func TestSDK_Lease_AcquireDurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		duration int32
		wantDur  string
		wantErr  bool
	}{
		{name: "infinite", duration: -1, wantDur: "infinite"},
		{name: "min", duration: 15, wantDur: "fixed"},
		{name: "max", duration: 60, wantDur: "fixed"},
		{name: "too_short", duration: 14, wantErr: true},
		{name: "too_long", duration: 61, wantErr: true},
		{name: "zero", duration: 0, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDKEnv(t)
			bb, _, _ := seedSDKBlob(t, env, "l.txt")
			lc := newBlobLease(t, bb, leaseA)

			resp, err := lc.AcquireLease(t.Context(), tt.duration, nil)
			if tt.wantErr {
				requireStorageErr(t, err, bloberror.InvalidHeaderValue, 400)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, leaseA, *resp.LeaseID)

			state, status, dur := leaseState(t, bb)
			assert.Equal(t, "leased", state)
			assert.Equal(t, "locked", status)
			assert.Equal(t, tt.wantDur, dur)
		})
	}
}

func TestSDK_Lease_FixedLeaseExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		advance   time.Duration
		wantState string
		wantLock  string
	}{
		{name: "before_expiry", advance: 14 * time.Second, wantState: "leased", wantLock: "locked"},
		{name: "at_expiry", advance: 15 * time.Second, wantState: "expired", wantLock: "unlocked"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDKEnv(t)
			bb, _, _ := seedSDKBlob(t, env, "e.txt")
			lc := newBlobLease(t, bb, leaseA)

			_, err := lc.AcquireLease(t.Context(), 15, nil)
			require.NoError(t, err)

			env.clock.Advance(tt.advance)

			state, status, _ := leaseState(t, bb)
			assert.Equal(t, tt.wantState, state)
			assert.Equal(t, tt.wantLock, status)

			_, err = bb.SetMetadata(t.Context(), map[string]*string{"x": new("y")}, nil)
			if tt.wantState == "expired" {
				require.NoError(t, err, "an expired lease no longer gates writes")

				_, err = lc.RenewLease(t.Context(), nil)
				require.NoError(t, err)

				state, _, _ = leaseState(t, bb)
				assert.Equal(t, "leased", state)

				return
			}

			requireStorageErr(t, err, bloberror.LeaseIDMissing, 412)
		})
	}
}

func TestSDK_Lease_WriteGating(t *testing.T) {
	t.Parallel()

	ops := map[string]func(t *testing.T, bb *blockblob.Client, id *string) error{
		"upload": func(t *testing.T, bb *blockblob.Client, id *string) error {
			_, err := bb.Upload(t.Context(), body("n"), &blockblob.UploadOptions{
				AccessConditions: &blob.AccessConditions{LeaseAccessConditions: &blob.LeaseAccessConditions{LeaseID: id}},
			})

			return err
		},
		"metadata": func(t *testing.T, bb *blockblob.Client, id *string) error {
			_, err := bb.SetMetadata(t.Context(), map[string]*string{"a": new("b")}, &blob.SetMetadataOptions{
				AccessConditions: &blob.AccessConditions{LeaseAccessConditions: &blob.LeaseAccessConditions{LeaseID: id}},
			})

			return err
		},
		"properties": func(t *testing.T, bb *blockblob.Client, id *string) error {
			_, err := bb.SetHTTPHeaders(t.Context(), blob.HTTPHeaders{BlobContentType: new("a/b")},
				&blob.SetHTTPHeadersOptions{
					AccessConditions: &blob.AccessConditions{LeaseAccessConditions: &blob.LeaseAccessConditions{LeaseID: id}},
				})

			return err
		},
		"delete": func(t *testing.T, bb *blockblob.Client, id *string) error {
			_, err := bb.Delete(t.Context(), &blob.DeleteOptions{
				AccessConditions: &blob.AccessConditions{LeaseAccessConditions: &blob.LeaseAccessConditions{LeaseID: id}},
			})

			return err
		},
		"stage_block": func(t *testing.T, bb *blockblob.Client, id *string) error {
			_, err := bb.StageBlock(t.Context(), blockID(0), body("n"), &blockblob.StageBlockOptions{
				LeaseAccessConditions: &blob.LeaseAccessConditions{LeaseID: id},
			})

			return err
		},
		"commit": func(t *testing.T, bb *blockblob.Client, id *string) error {
			if _, err := bb.StageBlock(t.Context(), blockID(0), body("n"), &blockblob.StageBlockOptions{
				LeaseAccessConditions: &blob.LeaseAccessConditions{LeaseID: new(leaseA)},
			}); err != nil {
				return err
			}

			_, err := bb.CommitBlockList(t.Context(), []string{blockID(0)}, &blockblob.CommitBlockListOptions{
				AccessConditions: &blob.AccessConditions{LeaseAccessConditions: &blob.LeaseAccessConditions{LeaseID: id}},
			})

			return err
		},
	}

	tests := []struct {
		name     string
		id       *string
		wantCode bloberror.Code
	}{
		{name: "missing", wantCode: bloberror.LeaseIDMissing},
		{name: "mismatch", id: new(leaseB), wantCode: bloberror.LeaseIDMismatchWithBlobOperation},
		{name: "match", id: new(leaseA)},
	}

	for op, run := range ops {
		for _, tt := range tests {
			t.Run(op+"_"+tt.name, func(t *testing.T) {
				t.Parallel()

				env := newSDKEnv(t)
				bb, _, _ := seedSDKBlob(t, env, "g.txt")

				_, err := newBlobLease(t, bb, leaseA).AcquireLease(t.Context(), -1, nil)
				require.NoError(t, err)

				err = run(t, bb, tt.id)
				if tt.wantCode == "" {
					require.NoError(t, err)

					return
				}

				requireStorageErr(t, err, tt.wantCode, 412)
			})
		}
	}
}

func TestSDK_Lease_ReadsAndNoLease(t *testing.T) {
	t.Parallel()

	env := newSDKEnv(t)
	bb, _, _ := seedSDKBlob(t, env, "r.txt")

	_, err := bb.SetMetadata(t.Context(), nil, &blob.SetMetadataOptions{
		AccessConditions: &blob.AccessConditions{LeaseAccessConditions: &blob.LeaseAccessConditions{LeaseID: new(leaseA)}},
	})
	requireStorageErr(t, err, bloberror.LeaseNotPresentWithBlobOperation, 412)

	_, err = newBlobLease(t, bb, leaseA).AcquireLease(t.Context(), -1, nil)
	require.NoError(t, err)

	assert.Equal(t, "data", downloadAll(t, bb.BlobClient()), "reads never need the lease id")

	_, err = bb.UploadBuffer(t.Context(), []byte("v2"), &blockblob.UploadBufferOptions{
		AccessConditions: &blob.AccessConditions{LeaseAccessConditions: &blob.LeaseAccessConditions{LeaseID: new(leaseA)}},
	})
	require.NoError(t, err)

	state, _, _ := leaseState(t, bb)
	assert.Equal(t, "leased", state, "overwriting a leased blob keeps its lease")
}

func TestSDK_Lease_Transitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		run      func(t *testing.T, bb *blockblob.Client)
		wantCode bloberror.Code
		status   int
	}{
		{
			name: "acquire_other_id", wantCode: bloberror.LeaseAlreadyPresent, status: 409,
			run: func(t *testing.T, bb *blockblob.Client) {
				t.Helper()
				_, err := newBlobLease(t, bb, leaseB).AcquireLease(t.Context(), -1, nil)
				requireStorageErr(t, err, bloberror.LeaseAlreadyPresent, 409)
			},
		},
		{
			name: "acquire_same_id_changes_duration",
			run: func(t *testing.T, bb *blockblob.Client) {
				t.Helper()
				_, err := newBlobLease(t, bb, leaseA).AcquireLease(t.Context(), 30, nil)
				require.NoError(t, err)

				_, _, dur := leaseState(t, bb)
				assert.Equal(t, "fixed", dur)
			},
		},
		{
			name: "renew_wrong_id",
			run: func(t *testing.T, bb *blockblob.Client) {
				t.Helper()
				_, err := newBlobLease(t, bb, leaseB).RenewLease(t.Context(), nil)
				requireStorageErr(t, err, bloberror.LeaseIDMismatchWithLeaseOperation, 409)
			},
		},
		{
			name: "change_then_old_id_rejected",
			run: func(t *testing.T, bb *blockblob.Client) {
				t.Helper()
				lc := newBlobLease(t, bb, leaseA)
				resp, err := lc.ChangeLease(t.Context(), leaseB, nil)
				require.NoError(t, err)
				assert.Equal(t, leaseB, *resp.LeaseID)

				_, err = bb.SetMetadata(t.Context(), nil, &blob.SetMetadataOptions{
					AccessConditions: &blob.AccessConditions{
						LeaseAccessConditions: &blob.LeaseAccessConditions{LeaseID: new(leaseA)},
					},
				})
				requireStorageErr(t, err, bloberror.LeaseIDMismatchWithBlobOperation, 412)

				_, err = bb.SetMetadata(t.Context(), nil, &blob.SetMetadataOptions{
					AccessConditions: &blob.AccessConditions{
						LeaseAccessConditions: &blob.LeaseAccessConditions{LeaseID: new(leaseB)},
					},
				})
				require.NoError(t, err)
			},
		},
		{
			name: "release_wrong_id",
			run: func(t *testing.T, bb *blockblob.Client) {
				t.Helper()
				_, err := newBlobLease(t, bb, leaseB).ReleaseLease(t.Context(), nil)
				requireStorageErr(t, err, bloberror.LeaseIDMismatchWithLeaseOperation, 409)
			},
		},
		{
			name: "release_then_available",
			run: func(t *testing.T, bb *blockblob.Client) {
				t.Helper()
				_, err := newBlobLease(t, bb, leaseA).ReleaseLease(t.Context(), nil)
				require.NoError(t, err)

				state, status, _ := leaseState(t, bb)
				assert.Equal(t, "available", state)
				assert.Equal(t, "unlocked", status)

				_, err = newBlobLease(t, bb, leaseA).RenewLease(t.Context(), nil)
				requireStorageErr(t, err, bloberror.LeaseNotPresentWithLeaseOperation, 409)

				_, err = bb.SetMetadata(t.Context(), nil, nil)
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDKEnv(t)
			bb, _, _ := seedSDKBlob(t, env, "t.txt")

			_, err := newBlobLease(t, bb, leaseA).AcquireLease(t.Context(), -1, nil)
			require.NoError(t, err)

			tt.run(t, bb)
		})
	}
}

func TestSDK_Lease_Break(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		duration     int32
		period       *int32
		wantTime     int32
		wantState    string
		advance      time.Duration
		wantAfter    string
	}{
		{name: "fixed_with_period", duration: 60, period: new(int32(10)), wantTime: 10, wantState: "breaking",
			advance: 10 * time.Second, wantAfter: "broken"},
		{name: "fixed_period_capped_by_remaining", duration: 15, period: new(int32(60)), wantTime: 15,
			wantState: "breaking", advance: 15 * time.Second, wantAfter: "broken"},
		{name: "fixed_without_period", duration: 20, wantTime: 20, wantState: "breaking",
			advance: 20 * time.Second, wantAfter: "broken"},
		{name: "infinite_without_period", duration: -1, wantTime: 0, wantState: "broken", wantAfter: "broken"},
		{name: "infinite_zero_period", duration: -1, period: new(int32(0)), wantTime: 0, wantState: "broken",
			wantAfter: "broken"},
		{name: "infinite_with_period", duration: -1, period: new(int32(5)), wantTime: 5, wantState: "breaking",
			advance: 5 * time.Second, wantAfter: "broken"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDKEnv(t)
			bb, _, _ := seedSDKBlob(t, env, "b.txt")
			lc := newBlobLease(t, bb, leaseA)

			_, err := lc.AcquireLease(t.Context(), tt.duration, nil)
			require.NoError(t, err)

			resp, err := lc.BreakLease(t.Context(), &lease.BlobBreakOptions{BreakPeriod: tt.period})
			require.NoError(t, err)
			assert.Equal(t, tt.wantTime, *resp.LeaseTime)

			state, status, _ := leaseState(t, bb)
			assert.Equal(t, tt.wantState, state)

			if tt.wantState == "breaking" {
				assert.Equal(t, "locked", status)

				_, err = newBlobLease(t, bb, leaseB).AcquireLease(t.Context(), -1, nil)
				requireStorageErr(t, err, bloberror.LeaseAlreadyPresent, 409)

				_, err = lc.AcquireLease(t.Context(), -1, nil)
				requireStorageErr(t, err, "LeaseIsBreakingAndCannotBeAcquired", 409)

				_, err = bb.SetMetadata(t.Context(), nil, nil)
				requireStorageErr(t, err, bloberror.LeaseIDMissing, 412)
			}

			env.clock.Advance(tt.advance)

			state, status, _ = leaseState(t, bb)
			assert.Equal(t, tt.wantAfter, state)
			assert.Equal(t, "unlocked", status)

			_, err = bb.SetMetadata(t.Context(), nil, nil)
			require.NoError(t, err, "a broken lease no longer gates writes")

			_, err = lc.RenewLease(t.Context(), nil)
			requireStorageErr(t, err, bloberror.LeaseIsBrokenAndCannotBeRenewed, 409)

			_, err = newBlobLease(t, bb, leaseB).AcquireLease(t.Context(), -1, nil)
			require.NoError(t, err)
		})
	}
}

func TestSDK_Lease_BreakWithoutLease(t *testing.T) {
	t.Parallel()

	env := newSDKEnv(t)
	bb, _, _ := seedSDKBlob(t, env, "n.txt")

	lc, err := lease.NewBlobClient(bb, nil)
	require.NoError(t, err)

	_, err = lc.BreakLease(t.Context(), nil)
	requireStorageErr(t, err, bloberror.LeaseNotPresentWithLeaseOperation, 409)
}

func TestSDK_Lease_Container(t *testing.T) {
	t.Parallel()

	env := newSDKEnv(t)
	ctx := t.Context()

	lc, err := lease.NewContainerClient(env.cc, &lease.ContainerClientOptions{LeaseID: new(leaseA)})
	require.NoError(t, err)

	_, err = lc.AcquireLease(ctx, 30, nil)
	require.NoError(t, err)

	props, err := env.cc.GetProperties(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, lease.StateTypeLeased, *props.LeaseState)
	assert.Equal(t, lease.StatusTypeLocked, *props.LeaseStatus)
	assert.Equal(t, lease.DurationTypeFixed, *props.LeaseDuration)

	pager := env.svc.NewListContainersPager(nil)
	page, err := pager.NextPage(ctx)
	require.NoError(t, err)
	require.Len(t, page.ContainerItems, 1)
	assert.Equal(t, lease.StateTypeLeased, *page.ContainerItems[0].Properties.LeaseState)

	_, err = env.cc.Delete(ctx, nil)
	requireStorageErr(t, err, bloberror.LeaseIDMissing, 412)

	_, err = env.cc.Delete(ctx, &container.DeleteOptions{
		AccessConditions: &container.AccessConditions{
			LeaseAccessConditions: &container.LeaseAccessConditions{LeaseID: new(leaseB)},
		},
	})
	requireStorageErr(t, err, bloberror.LeaseIDMismatchWithContainerOperation, 412)

	// Blob writes are not gated by a container lease.
	_, err = env.cc.NewBlockBlobClient("x").UploadBuffer(ctx, []byte("x"), nil)
	require.NoError(t, err)

	_, err = lc.BreakLease(ctx, &lease.ContainerBreakOptions{BreakPeriod: new(int32(0))})
	require.NoError(t, err)

	env.clock.Advance(time.Second)

	_, err = env.cc.Delete(ctx, nil)
	require.NoError(t, err)
}

func TestSDK_Lease_ContainerNoLeaseWithID(t *testing.T) {
	t.Parallel()

	env := newSDKEnv(t)

	_, err := env.cc.Delete(t.Context(), &container.DeleteOptions{
		AccessConditions: &container.AccessConditions{
			LeaseAccessConditions: &container.LeaseAccessConditions{LeaseID: new(leaseA)},
		},
	})
	requireStorageErr(t, err, bloberror.LeaseNotPresentWithContainerOperation, 412)
}

func TestBackend_LeasePersists(t *testing.T) {
	t.Parallel()

	env := newSDKEnv(t)
	bb, _, _ := seedSDKBlob(t, env, "persist.txt")

	_, err := newBlobLease(t, bb, leaseA).AcquireLease(t.Context(), 30, nil)
	require.NoError(t, err)

	snap := env.backend.Snapshot(t.Context())

	restored := newRestored(t, env, snap)
	info, err := restored.HeadBlobAt("sdkenv", "persist.txt", "")
	require.NoError(t, err)
	assert.Equal(t, "leased", info.Lease.State)

	env.clock.Advance(31 * time.Second)

	info, err = restored.HeadBlobAt("sdkenv", "persist.txt", "")
	require.NoError(t, err)
	assert.Equal(t, "expired", info.Lease.State)
}
