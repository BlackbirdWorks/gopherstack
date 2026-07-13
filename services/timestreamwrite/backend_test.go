package timestreamwrite_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

func newBackend() *timestreamwrite.InMemoryBackend {
	return timestreamwrite.NewInMemoryBackend()
}

func TestInMemoryBackend_CreateDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		name    string
		dbName  string
		wantErr bool
	}{
		{
			name:    "success",
			dbName:  "my-db",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			db, err := b.CreateDatabase(tt.dbName, "", nil)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.dbName, db.DatabaseName)
			assert.NotEmpty(t, db.ARN)
		})
	}
}

func TestInMemoryBackend_CreateDatabase_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateDatabase("dup-db", "", nil)
	require.NoError(t, err)

	_, err = b.CreateDatabase("dup-db", "", nil)
	require.Error(t, err)
	require.ErrorIs(t, err, awserr.ErrConflict)
}

func TestInMemoryBackend_DescribeDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		name    string
		dbName  string
		create  bool
		wantErr bool
	}{
		{
			name:    "success",
			dbName:  "test-db",
			create:  true,
			wantErr: false,
		},
		{
			name:    "not found",
			dbName:  "missing-db",
			create:  false,
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.create {
				_, err := b.CreateDatabase(tt.dbName, "", nil)
				require.NoError(t, err)
			}

			db, err := b.DescribeDatabase(tt.dbName)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.dbName, db.DatabaseName)
		})
	}
}

func TestInMemoryBackend_ListDatabases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		creates []string
		wantLen int
	}{
		{
			name:    "empty",
			creates: nil,
			wantLen: 0,
		},
		{
			name:    "multiple databases",
			creates: []string{"db-a", "db-b", "db-c"},
			wantLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			for _, name := range tt.creates {
				_, err := b.CreateDatabase(name, "", nil)
				require.NoError(t, err)
			}

			dbs := b.ListDatabases()
			assert.Len(t, dbs, tt.wantLen)
		})
	}
}

func TestInMemoryBackend_DeleteDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		name    string
		dbName  string
		create  bool
		wantErr bool
	}{
		{
			name:    "success",
			dbName:  "del-db",
			create:  true,
			wantErr: false,
		},
		{
			name:    "not found",
			dbName:  "missing-db",
			create:  false,
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.create {
				_, err := b.CreateDatabase(tt.dbName, "", nil)
				require.NoError(t, err)
			}

			err := b.DeleteDatabase(tt.dbName)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)

			_, descErr := b.DescribeDatabase(tt.dbName)
			require.Error(t, descErr)
		})
	}
}

func TestInMemoryBackend_UpdateDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs    error
		name     string
		dbName   string
		kmsKeyID string
		create   bool
		wantErr  bool
	}{
		{
			name:     "success",
			dbName:   "update-db",
			kmsKeyID: "arn:aws:kms:us-east-1:000000000000:key/test",
			create:   true,
			wantErr:  false,
		},
		{
			name:    "not found",
			dbName:  "missing-db",
			create:  false,
			wantErr: true,
			errIs:   awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.create {
				_, err := b.CreateDatabase(tt.dbName, "", nil)
				require.NoError(t, err)
			}

			db, err := b.UpdateDatabase(tt.dbName, tt.kmsKeyID)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.kmsKeyID, db.KmsKeyID)
		})
	}
}

func TestInMemoryBackend_CreateTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs    error
		name     string
		dbName   string
		tblName  string
		createDB bool
		wantErr  bool
	}{
		{
			name:     "success",
			dbName:   "my-db",
			tblName:  "my-table",
			createDB: true,
			wantErr:  false,
		},
		{
			name:     "database not found",
			dbName:   "missing-db",
			tblName:  "my-table",
			createDB: false,
			wantErr:  true,
			errIs:    awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.createDB {
				_, err := b.CreateDatabase(tt.dbName, "", nil)
				require.NoError(t, err)
			}

			tbl, err := b.CreateTable(tt.dbName, tt.tblName, nil, nil)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.tblName, tbl.TableName)
			assert.Equal(t, "ACTIVE", tbl.TableStatus)
			assert.NotEmpty(t, tbl.ARN)
		})
	}
}

func TestInMemoryBackend_CreateTable_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateDatabase("db", "", nil)
	require.NoError(t, err)

	_, err = b.CreateTable("db", "dup-table", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateTable("db", "dup-table", nil, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, awserr.ErrConflict)
}

func TestInMemoryBackend_DescribeTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs     error
		name      string
		dbName    string
		tblName   string
		createDB  bool
		createTbl bool
		wantErr   bool
	}{
		{
			name:      "success",
			dbName:    "db",
			tblName:   "tbl",
			createDB:  true,
			createTbl: true,
			wantErr:   false,
		},
		{
			name:      "table not found",
			dbName:    "db",
			tblName:   "missing",
			createDB:  true,
			createTbl: false,
			wantErr:   true,
			errIs:     awserr.ErrNotFound,
		},
		{
			name:     "database not found",
			dbName:   "missing-db",
			tblName:  "tbl",
			createDB: false,
			wantErr:  true,
			errIs:    awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.createDB {
				_, err := b.CreateDatabase(tt.dbName, "", nil)
				require.NoError(t, err)
			}

			if tt.createTbl {
				_, err := b.CreateTable(tt.dbName, tt.tblName, nil, nil)
				require.NoError(t, err)
			}

			tbl, err := b.DescribeTable(tt.dbName, tt.tblName)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.tblName, tbl.TableName)
		})
	}
}

func TestInMemoryBackend_ListTables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tables  []string
		wantLen int
		wantErr bool
	}{
		{
			name:    "empty",
			tables:  nil,
			wantLen: 0,
			wantErr: false,
		},
		{
			name:    "multiple tables",
			tables:  []string{"tbl-a", "tbl-b"},
			wantLen: 2,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.CreateDatabase("db", "", nil)
			require.NoError(t, err)

			for _, name := range tt.tables {
				_, err = b.CreateTable("db", name, nil, nil)
				require.NoError(t, err)
			}

			tbls, err := b.ListTables("db")

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, tbls, tt.wantLen)
		})
	}
}

func TestInMemoryBackend_ListTables_DatabaseNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.ListTables("missing-db")
	require.Error(t, err)
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_DeleteTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs     error
		name      string
		tblName   string
		createTbl bool
		wantErr   bool
	}{
		{
			name:      "success",
			tblName:   "del-tbl",
			createTbl: true,
			wantErr:   false,
		},
		{
			name:      "not found",
			tblName:   "missing-tbl",
			createTbl: false,
			wantErr:   true,
			errIs:     awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.CreateDatabase("db", "", nil)
			require.NoError(t, err)

			if tt.createTbl {
				_, err = b.CreateTable("db", tt.tblName, nil, nil)
				require.NoError(t, err)
			}

			err = b.DeleteTable("db", tt.tblName)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)

			_, descErr := b.DescribeTable("db", tt.tblName)
			require.Error(t, descErr)
		})
	}
}

func TestInMemoryBackend_UpdateTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs     error
		name      string
		tblName   string
		createTbl bool
		wantErr   bool
	}{
		{
			name:      "success",
			tblName:   "upd-tbl",
			createTbl: true,
			wantErr:   false,
		},
		{
			name:      "not found",
			tblName:   "missing-tbl",
			createTbl: false,
			wantErr:   true,
			errIs:     awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.CreateDatabase("db", "", nil)
			require.NoError(t, err)

			if tt.createTbl {
				_, err = b.CreateTable("db", tt.tblName, nil, nil)
				require.NoError(t, err)
			}

			tbl, err := b.UpdateTable("db", tt.tblName, nil)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.tblName, tbl.TableName)
		})
	}
}

func TestInMemoryBackend_WriteRecords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs     error
		name      string
		dbName    string
		tblName   string
		records   []timestreamwrite.Record
		createDB  bool
		createTbl bool
		wantErr   bool
	}{
		{
			name:      "success",
			dbName:    "db",
			tblName:   "tbl",
			createDB:  true,
			createTbl: true,
			records: []timestreamwrite.Record{
				{MeasureName: "cpu", MeasureValue: "98.5", MeasureValueType: "DOUBLE"},
			},
			wantErr: false,
		},
		{
			name:      "table not found",
			dbName:    "db",
			tblName:   "missing",
			createDB:  true,
			createTbl: false,
			records:   []timestreamwrite.Record{{MeasureName: "cpu"}},
			wantErr:   true,
			errIs:     awserr.ErrNotFound,
		},
		{
			name:      "database not found",
			dbName:    "missing-db",
			tblName:   "tbl",
			createDB:  false,
			createTbl: false,
			records:   []timestreamwrite.Record{{MeasureName: "cpu"}},
			wantErr:   true,
			errIs:     awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.createDB {
				_, err := b.CreateDatabase(tt.dbName, "", nil)
				require.NoError(t, err)
			}

			if tt.createTbl {
				_, err := b.CreateTable(tt.dbName, tt.tblName, nil, nil)
				require.NoError(t, err)
			}

			_, err := b.WriteRecords(tt.dbName, tt.tblName, tt.records)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_Tags(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateDatabase("my-db", "", nil)
	require.NoError(t, err)

	arn := "arn:aws:timestream:us-east-1:000000000000:database/my-db"

	err = b.TagResource(arn, map[string]string{"env": "test", "team": "infra"})
	require.NoError(t, err)

	tags := b.ListTagsForResource(arn)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "infra", tags["team"])

	err = b.UntagResource(arn, []string{"team"})
	require.NoError(t, err)

	tags = b.ListTagsForResource(arn)
	assert.Equal(t, "test", tags["env"])
	_, hasTeam := tags["team"]
	assert.False(t, hasTeam)
}

func TestInMemoryBackend_TableCount(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateDatabase("db", "", nil)
	require.NoError(t, err)

	_, err = b.CreateTable("db", "t1", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateTable("db", "t2", nil, nil)
	require.NoError(t, err)

	db, err := b.DescribeDatabase("db")
	require.NoError(t, err)
	assert.Equal(t, 2, db.TableCount)

	err = b.DeleteTable("db", "t1")
	require.NoError(t, err)

	db, err = b.DescribeDatabase("db")
	require.NoError(t, err)
	assert.Equal(t, 1, db.TableCount)
}

func TestInMemoryBackend_DeleteDatabase_CleansUpTags(t *testing.T) {
	t.Parallel()

	b := newBackend()

	_, err := b.CreateDatabase("cleanup-db", "", nil)
	require.NoError(t, err)

	_, err = b.CreateTable("cleanup-db", "t1", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateTable("cleanup-db", "t2", nil, nil)
	require.NoError(t, err)

	dbARN := "arn:aws:timestream:us-east-1:000000000000:database/cleanup-db"
	t1ARN := "arn:aws:timestream:us-east-1:000000000000:database/cleanup-db/table/t1"
	t2ARN := "arn:aws:timestream:us-east-1:000000000000:database/cleanup-db/table/t2"

	err = b.TagResource(dbARN, map[string]string{"key": "dbval"})
	require.NoError(t, err)

	err = b.TagResource(t1ARN, map[string]string{"key": "t1val"})
	require.NoError(t, err)

	err = b.TagResource(t2ARN, map[string]string{"key": "t2val"})
	require.NoError(t, err)

	err = b.DeleteDatabase("cleanup-db")
	require.NoError(t, err)

	assert.Empty(t, b.ListTagsForResource(dbARN))
	assert.Empty(t, b.ListTagsForResource(t1ARN))
	assert.Empty(t, b.ListTagsForResource(t2ARN))
}

func TestInMemoryBackend_DeleteTable_CleansUpTags(t *testing.T) {
	t.Parallel()

	b := newBackend()

	_, err := b.CreateDatabase("db", "", nil)
	require.NoError(t, err)

	_, err = b.CreateTable("db", "tbl", nil, nil)
	require.NoError(t, err)

	tblARN := "arn:aws:timestream:us-east-1:000000000000:database/db/table/tbl"

	err = b.TagResource(tblARN, map[string]string{"env": "prod"})
	require.NoError(t, err)

	assert.Equal(t, "prod", b.ListTagsForResource(tblARN)["env"])

	err = b.DeleteTable("db", "tbl")
	require.NoError(t, err)

	assert.Empty(t, b.ListTagsForResource(tblARN))
}

func TestInMemoryBackend_DeleteDatabase_CleansUpTableMutexes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tableNames []string
	}{
		{name: "no tables", tableNames: nil},
		{name: "single table", tableNames: []string{"only"}},
		{name: "multiple tables", tableNames: []string{"t1", "t2", "t3"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.CreateDatabase("db", "", nil)
			require.NoError(t, err)

			for _, name := range tt.tableNames {
				_, err = b.CreateTable("db", name, nil, nil)
				require.NoError(t, err)
			}

			require.Equal(t, len(tt.tableNames), timestreamwrite.TableMutexCount(b))

			require.NoError(t, b.DeleteDatabase("db"))

			assert.Equal(t, 0, timestreamwrite.TableMutexCount(b))
		})
	}
}

// TestInMemoryBackend_WriteRecords_CrossTableRace exercises concurrent
// WriteRecords for different tables in the SAME database. Under the previous
// design these would each acquire a different per-table mutex but still write
// to the shared inner b.records[dbName] map (b.records[dbName][tblName] = append(...)),
// which is a fatal "concurrent map writes" race in Go even on different keys.
// The fix moves each table's slice into a *tableRecords slot pointer so the
// append never touches the enclosing map.
func TestInMemoryBackend_WriteRecords_CrossTableRace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tableCount int
		iterations int
	}{
		{name: "two tables few writes", tableCount: 2, iterations: 50},
		{name: "eight tables many writes", tableCount: 8, iterations: 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.CreateDatabase("db", "", nil)
			require.NoError(t, err)

			for i := range tt.tableCount {
				_, err = b.CreateTable("db", "tbl"+strconv.Itoa(i), nil, nil)
				require.NoError(t, err)
			}

			rec := []timestreamwrite.Record{{
				MeasureName:  "m",
				MeasureValue: "1",
				Time:         "1700000000",
				TimeUnit:     "SECONDS",
			}}

			done := make(chan struct{})
			for i := range tt.tableCount {
				go func() {
					tbl := "tbl" + strconv.Itoa(i)
					for range tt.iterations {
						_, _ = b.WriteRecords("db", tbl, rec)
					}
					done <- struct{}{}
				}()
			}

			for range tt.tableCount {
				<-done
			}
		})
	}
}

// TestInMemoryBackend_WriteRecords_SnapshotRace concurrently writes and snapshots
// the backend; with the race detector enabled this catches any concurrent map
// read/write between WriteRecords (under global RLock + per-table WLock) and
// Snapshot (which previously also held only RLock and iterated b.records).
func TestInMemoryBackend_WriteRecords_SnapshotRace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		writers    int
		iterations int
	}{
		{name: "single writer", writers: 1, iterations: 50},
		{name: "four writers", writers: 4, iterations: 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.CreateDatabase("db", "", nil)
			require.NoError(t, err)
			_, err = b.CreateTable("db", "tbl", nil, nil)
			require.NoError(t, err)

			rec := []timestreamwrite.Record{{
				MeasureName:  "m",
				MeasureValue: "1",
				Time:         "1700000000",
				TimeUnit:     "SECONDS",
			}}

			done := make(chan struct{})
			for range tt.writers {
				go func() {
					for range tt.iterations {
						_, _ = b.WriteRecords("db", "tbl", rec)
					}
					done <- struct{}{}
				}()
			}

			go func() {
				for range tt.iterations {
					_, _ = b.Snapshot()
				}
				done <- struct{}{}
			}()

			for range tt.writers + 1 {
				<-done
			}
		})
	}
}
