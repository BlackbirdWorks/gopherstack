package rdsdata_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsdatasdk "github.com/aws/aws-sdk-go-v2/service/rdsdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rdsdata"
)

func TestExecuteStatementDatabaseSelectsDistinctDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		createDB  string
		queryDB   string
		wantFound bool
	}{
		{name: "same_database", createDB: "alpha", queryDB: "alpha", wantFound: true},
		{name: "other_database", createDB: "alpha", queryDB: "beta", wantFound: false},
		{name: "default_vs_named", createDB: "", queryDB: "beta", wantFound: false},
		{name: "default_default", createDB: "", queryDB: "", wantFound: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
			t.Cleanup(backend.Close)
			client := newRoundTripClient(t, rdsdata.NewHandler(backend))

			exec := func(db, stmt string) (*rdsdatasdk.ExecuteStatementOutput, error) {
				in := &rdsdatasdk.ExecuteStatementInput{
					ResourceArn: aws.String(columnOriginResourceARN),
					SecretArn:   aws.String(columnOriginSecretARN),
					Sql:         aws.String(stmt),
				}
				if db != "" {
					in.Database = aws.String(db)
				}

				return client.ExecuteStatement(t.Context(), in)
			}

			_, err := exec(tt.createDB, "CREATE TABLE things (id INTEGER)")
			require.NoError(t, err)
			_, err = exec(tt.createDB, "INSERT INTO things (id) VALUES (7)")
			require.NoError(t, err)

			out, err := exec(tt.queryDB, "SELECT id FROM things")
			require.NoError(t, err)

			if tt.wantFound {
				require.Len(t, out.Records, 1)

				return
			}

			assert.Empty(t, out.Records)
		})
	}
}
