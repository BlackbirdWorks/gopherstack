package timestreamwrite_test

import (
	"testing"
	"time"

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

// TestInMemoryBackend_UpdateDatabase_ClearsKmsKeyID verifies that calling
// UpdateDatabase with an empty KmsKeyId clears a previously set key.
func TestInMemoryBackend_UpdateDatabase_ClearsKmsKeyID(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("clr-kms-db", "", nil)
	require.NoError(t, err)

	// First set a KMS key.
	_, err = b.UpdateDatabase("clr-kms-db", "arn:aws:kms:us-east-1:000000000000:key/abc")
	require.NoError(t, err)

	db, err := b.DescribeDatabase("clr-kms-db")
	require.NoError(t, err)
	assert.NotEmpty(t, db.KmsKeyID, "KmsKeyID should be set after UpdateDatabase")

	// Now clear it.
	_, err = b.UpdateDatabase("clr-kms-db", "")
	require.NoError(t, err)

	db, err = b.DescribeDatabase("clr-kms-db")
	require.NoError(t, err)
	assert.Empty(t, db.KmsKeyID, "KmsKeyID should be cleared after UpdateDatabase with empty string")
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

// TestInMemoryBackend_DatabaseCountExport verifies the DatabaseCount export
// helper reflects databases added via AddDatabaseInternal.
func TestInMemoryBackend_DatabaseCountExport(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	assert.Equal(t, 0, timestreamwrite.DatabaseCount(b))

	b.AddDatabaseInternal(&timestreamwrite.Database{
		DatabaseName: "db1",
		ARN:          "arn:aws:timestream:us-east-1:000000000000:database/db1",
	})
	assert.Equal(t, 1, timestreamwrite.DatabaseCount(b))
}

// TestInMemoryBackend_AddDatabaseInternal seeds data directly for test setup.
func TestInMemoryBackend_AddDatabaseInternal(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	now := time.Now()
	b.AddDatabaseInternal(&timestreamwrite.Database{
		DatabaseName:    "seeded-db",
		ARN:             "arn:aws:timestream:us-east-1:000000000000:database/seeded-db",
		CreationTime:    now,
		LastUpdatedTime: now,
	})

	db, err := b.DescribeDatabase("seeded-db")
	require.NoError(t, err)
	assert.Equal(t, "seeded-db", db.DatabaseName)
}
