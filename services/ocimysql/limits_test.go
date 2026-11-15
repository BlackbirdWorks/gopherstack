package ocimysql_test

import (
	"net/http"
	"testing"

	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ocimysql"
)

func TestServiceLimits(t *testing.T) {
	t.Parallel()

	t.Run("shape instances", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		e.backend.Configure(ocimysql.Settings{MaxShapeInstances: 1})
		e.mkDB(t, "one", nil)

		_, err := e.db.CreateDbSystem(t.Context(), my.CreateDbSystemRequest{CreateDbSystemDetails: dbDetails("two")})
		se := svcErr(t, err)
		assert.Equal(t, "LimitExceeded", se.GetCode())
		assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())

		other := dbDetails("other-shape")
		other.ShapeName = new(shapeBig)
		e.createDB(t, other)
	})

	t.Run("big shape is a tenth", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		e.backend.Configure(ocimysql.Settings{MaxShapeInstances: 20})

		mk := func(name string) error {
			d := dbDetails(name)
			d.ShapeName = new("MySQL.256")
			_, err := e.db.CreateDbSystem(t.Context(), my.CreateDbSystemRequest{CreateDbSystemDetails: d})

			return err
		}

		require.NoError(t, mk("a"))
		require.NoError(t, mk("b"))
		assert.Equal(t, "LimitExceeded", svcErr(t, mk("c")).GetCode())
	})

	t.Run("storage", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		e.backend.Configure(ocimysql.Settings{MaxStorageGB: 160})

		mk := func(name string, gb int) error {
			d := dbDetails(name)
			d.DataStorageSizeInGBs = new(gb)
			_, err := e.db.CreateDbSystem(t.Context(), my.CreateDbSystemRequest{CreateDbSystemDetails: d})

			return err
		}

		require.NoError(t, mk("a", 100))
		assert.Equal(t, "LimitExceeded", svcErr(t, mk("b", 100)).GetCode())
		require.NoError(t, mk("c", 50))
	})

	t.Run("manual backups", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		e.backend.Configure(ocimysql.Settings{MaxManualBackups: 1})
		db := e.mkDB(t, "src", nil)
		e.mkBackup(t, db.Id, "first")

		_, err := e.bk.CreateBackup(t.Context(), my.CreateBackupRequest{
			DbSystemId: db.Id, DisplayName: new("second"),
		})
		assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())
	})

	t.Run("parallel copies", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		e.backend.Configure(ocimysql.Settings{MaxParallelCopies: 1})
		db := e.mkDB(t, "src", nil)
		src := e.mkBackup(t, db.Id, "original")

		cp := func() error {
			_, err := e.bk.CopyBackup(
				t.Context(),
				my.CopyBackupRequest{
					CompartmentId:  new(testCompartment),
					SourceBackupId: src.Id,
					SourceRegion:   new(remoteRegion),
				},
			)

			return err
		}

		require.NoError(t, cp())
		assert.Equal(t, "LimitExceeded", svcErr(t, cp()).GetCode())

		e.settle()
		require.NoError(t, cp())
	})
}
