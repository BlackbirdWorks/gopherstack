package timestreamwrite_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

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

// TestInMemoryBackend_TableCountExport verifies TableCount export.
func TestInMemoryBackend_TableCountExport(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	b.AddDatabaseInternal(&timestreamwrite.Database{
		DatabaseName: "db1",
		ARN:          "arn:aws:timestream:us-east-1:000000000000:database/db1",
	})
	b.AddTableInternal(&timestreamwrite.Table{
		DatabaseName: "db1",
		TableName:    "tbl1",
		ARN:          "arn:aws:timestream:us-east-1:000000000000:database/db1/table/tbl1",
	})
	assert.Equal(t, 1, timestreamwrite.TableCount(b))
}

// TestInMemoryBackend_AddTableInternal seeds a table directly.
func TestInMemoryBackend_AddTableInternal(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	now := time.Now()
	b.AddDatabaseInternal(&timestreamwrite.Database{
		DatabaseName:    "seed-db",
		ARN:             "arn:aws:timestream:us-east-1:000000000000:database/seed-db",
		CreationTime:    now,
		LastUpdatedTime: now,
	})
	b.AddTableInternal(&timestreamwrite.Table{
		DatabaseName:    "seed-db",
		TableName:       "seed-tbl",
		ARN:             "arn:aws:timestream:us-east-1:000000000000:database/seed-db/table/seed-tbl",
		TableStatus:     "ACTIVE",
		CreationTime:    now,
		LastUpdatedTime: now,
	})

	tbl, err := b.DescribeTable("seed-db", "seed-tbl")
	require.NoError(t, err)
	assert.Equal(t, "seed-tbl", tbl.TableName)
	assert.Equal(t, 1, timestreamwrite.TableCount(b))
}

// TestInMemoryBackend_CreateTable_DefaultRetentionProperties verifies that
// CreateTable applies the real AWS retention defaults (6h / 73d) whenever the
// caller does not fully specify RetentionProperties.
func TestInMemoryBackend_CreateTable_DefaultRetentionProperties(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name      string
		inp       *timestreamwrite.CreateTableInput
		wantHours int64
		wantDays  int64
	}{
		{
			name:      "nil_input_returns_defaults",
			inp:       nil,
			wantHours: 6,
			wantDays:  73,
		},
		{
			name:      "non_nil_input_nil_retention_returns_defaults",
			inp:       &timestreamwrite.CreateTableInput{},
			wantHours: 6,
			wantDays:  73,
		},
		{
			name: "explicit_retention_preserved",
			inp: &timestreamwrite.CreateTableInput{
				RetentionProperties: &timestreamwrite.RetentionProperties{
					MemoryStoreRetentionPeriodInHours:  24,
					MagneticStoreRetentionPeriodInDays: 365,
				},
			},
			wantHours: 24,
			wantDays:  365,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := timestreamwrite.NewInMemoryBackend()
			_, err := b.CreateDatabase("def-b-db", "", nil)
			require.NoError(t, err)

			tbl, err := b.CreateTable("def-b-db", "def-b-tbl", nil, tt.inp)
			require.NoError(t, err)
			require.NotNil(t, tbl.RetentionProperties)
			assert.Equal(t, tt.wantHours, tbl.RetentionProperties.MemoryStoreRetentionPeriodInHours)
			assert.Equal(t, tt.wantDays, tbl.RetentionProperties.MagneticStoreRetentionPeriodInDays)
		})
	}
}

// TestInMemoryBackend_CreateTable_RetentionPropertiesRoundTrip verifies the
// backend stores and returns RetentionProperties correctly, including via a
// subsequent DescribeTable call.
func TestInMemoryBackend_CreateTable_RetentionPropertiesRoundTrip(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("rp-db", "", nil)
	require.NoError(t, err)

	inp := &timestreamwrite.CreateTableInput{
		RetentionProperties: &timestreamwrite.RetentionProperties{
			MemoryStoreRetentionPeriodInHours:  12,
			MagneticStoreRetentionPeriodInDays: 180,
		},
	}

	tbl, err := b.CreateTable("rp-db", "rp-tbl", nil, inp)
	require.NoError(t, err)
	require.NotNil(t, tbl.RetentionProperties)
	assert.Equal(t, int64(12), tbl.RetentionProperties.MemoryStoreRetentionPeriodInHours)
	assert.Equal(t, int64(180), tbl.RetentionProperties.MagneticStoreRetentionPeriodInDays)

	// Verify DescribeTable also returns the properties.
	described, err := b.DescribeTable("rp-db", "rp-tbl")
	require.NoError(t, err)
	require.NotNil(t, described.RetentionProperties)
	assert.Equal(t, int64(12), described.RetentionProperties.MemoryStoreRetentionPeriodInHours)
}

// TestInMemoryBackend_UpdateTable_MagneticStoreWriteProperties verifies
// UpdateTable modifies MagneticStoreWriteProperties.
func TestInMemoryBackend_UpdateTable_MagneticStoreWriteProperties(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("ms2-db", "", nil)
	require.NoError(t, err)

	_, err = b.CreateTable("ms2-db", "ms2-tbl", nil, &timestreamwrite.CreateTableInput{
		MagneticStoreWriteProperties: &timestreamwrite.MagneticStoreWriteProperties{EnableMagneticStoreWrites: false},
	})
	require.NoError(t, err)

	updated, err := b.UpdateTable("ms2-db", "ms2-tbl", &timestreamwrite.UpdateTableInput{
		MagneticStoreWriteProperties: &timestreamwrite.MagneticStoreWriteProperties{EnableMagneticStoreWrites: true},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.MagneticStoreWriteProperties)
	assert.True(t, updated.MagneticStoreWriteProperties.EnableMagneticStoreWrites)
}

// TestPartitionKeyTypeConstants verifies the exported PartitionKeyType and
// PartitionKeyEnforcementLevel constants match the real AWS values.
func TestPartitionKeyTypeConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "DIMENSION", timestreamwrite.PartitionKeyTypeDimension)
	assert.Equal(t, "MEASURE", timestreamwrite.PartitionKeyTypeMeasure)
	assert.Equal(t, "REQUIRED", timestreamwrite.PartitionKeyEnforcementRequired)
	assert.Equal(t, "OPTIONAL", timestreamwrite.PartitionKeyEnforcementOptional)
}
