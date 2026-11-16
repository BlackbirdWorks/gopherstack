package gcpspanner_test

import (
	"testing"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestInsertDatabase_RechecksConfigAfterKeyCheck(t *testing.T) {
	t.Parallel()

	const inst = parent + "/instances/kms"

	tests := []struct {
		name    string
		enc     *databasepb.EncryptionConfig
		checked string
		want    codes.Code
	}{
		{"same config", &databasepb.EncryptionConfig{KmsKeyName: "k"}, configRef, codes.OK},
		{
			"moved meanwhile",
			&databasepb.EncryptionConfig{KmsKeyName: "k"},
			parent + "/instanceConfigs/nam3",
			codes.Aborted,
		},
		{"no key, nothing to recheck", nil, parent + "/instanceConfigs/nam3", codes.OK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			require.Equal(t, configRef, e.createInstance(t, "kms").GetConfig())

			err := e.handler.Backend.InsertDatabaseDirect(
				inst,
				"db1",
				&databasepb.Database{EncryptionConfig: tt.enc},
				tt.checked,
			)
			assert.Equal(t, tt.want, status.Code(err))
		})
	}
}
