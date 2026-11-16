package timestreaminfluxdb_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb"
	"github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContinuousBackupPointInTimeRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		restoreAt func(first, second time.Time) time.Time
		name      string
		wantFirst bool
		wantErr   bool
	}{
		{
			name:      "between snapshots",
			restoreAt: func(a, b time.Time) time.Time { return a.Add(b.Sub(a) / 2) },
			wantFirst: true,
		},
		{name: "after last snapshot", restoreAt: func(_, b time.Time) time.Time { return b.Add(time.Minute) }},
		{
			name:      "before first snapshot",
			restoreAt: func(a, _ time.Time) time.Time { return a.Add(-time.Minute) },
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newOpsEnv(t)

			in := instanceInput("pitr-db")
			in.DbBackupConfigurations = []types.DbBackupConfiguration{
				{Enabled: aws.Bool(true), RetentionDays: aws.Int32(7), Type: types.AutomatedDbBackupTypeContinuous},
			}
			out, err := e.client.CreateDbInstance(t.Context(), in)
			require.NoError(t, err)
			e.backend.WaitIdle()
			require.Equal(t, types.StatusAvailable, status(t, e.fixture, out.Id))
			id := out.Id

			ids := make([]*string, 0, 2)
			times := make([]time.Time, 0, 2)

			for range 2 {
				e.clock.advance(2 * time.Hour)
				e.backend.RunScheduled(t.Context())

				list, lerr := e.client.ListDbBackups(t.Context(), &sdk.ListDbBackupsInput{DbResourceId: id})
				require.NoError(t, lerr)
				require.Len(t, list.Items, len(ids)+1)

				var snap types.DbBackupSummary

				for _, it := range list.Items {
					if len(ids) == 0 || aws.ToString(it.Id) != aws.ToString(ids[0]) {
						snap = it
					}
				}

				assert.Equal(t, "CONTINUOUS", string(snap.Type))
				ids = append(ids, snap.Id)
				times = append(times, e.clock.now())

				e.backend.WaitIdle()
				require.Equal(t, types.DbBackupStatusCompleted, e.backupStatus(t, snap.Id))
			}

			e.clock.advance(time.Hour)

			res, err := e.client.RestoreFromDbBackup(t.Context(), &sdk.RestoreFromDbBackupInput{
				DbBackupId:    ids[1],
				Name:          aws.String("restored"),
				RestoreToTime: aws.Time(tt.restoreAt(times[0], times[1])),
			})
			if tt.wantErr {
				assert.Equal(t, "ValidationException", apiCode(err))

				return
			}

			require.NoError(t, err)
			e.backend.WaitIdle()
			require.Equal(t, types.StatusAvailable, status(t, e.fixture, res.RestoredDbResourceId))

			specs, _, _, _ := e.eng.snapshot()
			require.Len(t, specs, 2)

			want := ids[1]
			if tt.wantFirst {
				want = ids[0]
			}

			assert.Equal(t, aws.ToString(want), specs[1].RestoreFrom)
		})
	}
}
