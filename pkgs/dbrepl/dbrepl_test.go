package dbrepl_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/dbrepl"
)

func TestInterpretMySQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		row  map[string]string
		want dbrepl.Status
	}{
		{
			name: "running with lag",
			row: map[string]string{
				"replica_io_running": "Yes", "replica_sql_running": "Yes", "seconds_behind_source": "3",
			},
			want: dbrepl.Status{Replicating: true, LagSeconds: 3},
		},
		{
			name: "legacy column names",
			row: map[string]string{
				"slave_io_running":      "Yes",
				"slave_sql_running":     "Yes",
				"seconds_behind_master": "1",
			},
			want: dbrepl.Status{Replicating: true, LagSeconds: 1},
		},
		{
			name: "io thread error",
			row:  map[string]string{"replica_io_running": "No", "replica_sql_running": "Yes", "last_io_error": "boom"},
			want: dbrepl.Status{Message: "boom"},
		},
		{
			name: "stopped",
			row:  map[string]string{"replica_io_running": "No", "replica_sql_running": "No"},
			want: dbrepl.Status{Message: "replication threads are not running"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, dbrepl.InterpretMySQL(tt.row))
		})
	}
}

func TestStatements(t *testing.T) {
	t.Parallel()

	assert.Contains(t, dbrepl.PGReplicaScript("u1"), "user=u1 password=$"+dbrepl.PasswordEnv)
	assert.Contains(t, dbrepl.PGHBAScript("u1"), "host replication u1 samenet")
	assert.Contains(t, dbrepl.PGReplicaRoleSQL("u1", "pw"), "CREATE ROLE u1 WITH LOGIN REPLICATION PASSWORD 'pw'")
	assert.Contains(t, dbrepl.MySQLReplicaUserSQL("u1", "pw"), "GRANT REPLICATION SLAVE ON *.* TO 'u1'@'%'")
	assert.Contains(t, dbrepl.MySQLChangeSourceSQL("h", 3306, "u1", "pw"), "SOURCE_HOST='h', SOURCE_PORT=3306")
}
