package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	gluesdktypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateSchema_Checkpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		number         *gluesdktypes.SchemaVersionNumber
		name           string
		wantCheckpoint int64
		wantErr        bool
	}{
		{name: "no_version", wantCheckpoint: 1},
		{
			name:           "explicit_version",
			number:         &gluesdktypes.SchemaVersionNumber{VersionNumber: aws.Int64(2)},
			wantCheckpoint: 2,
		},
		{name: "latest_version", number: &gluesdktypes.SchemaVersionNumber{LatestVersion: true}, wantCheckpoint: 2},
		{
			name:    "unknown_version",
			number:  &gluesdktypes.SchemaVersionNumber{VersionNumber: aws.Int64(9)},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateRegistry(ctx, &gluesdk.CreateRegistryInput{RegistryName: aws.String("reg")})
			require.NoError(t, err)

			schemaID := &gluesdktypes.SchemaId{RegistryName: aws.String("reg"), SchemaName: aws.String("sch")}

			_, err = client.CreateSchema(ctx, &gluesdk.CreateSchemaInput{
				SchemaName: aws.String("sch"), RegistryId: &gluesdktypes.RegistryId{RegistryName: aws.String("reg")},
				DataFormat: gluesdktypes.DataFormatJson, SchemaDefinition: aws.String(`{"type":"object"}`),
				Compatibility: gluesdktypes.CompatibilityNone,
			})
			require.NoError(t, err)

			_, err = client.RegisterSchemaVersion(ctx, &gluesdk.RegisterSchemaVersionInput{
				SchemaId: schemaID, SchemaDefinition: aws.String(`{"type":"string"}`),
			})
			require.NoError(t, err)

			_, err = client.UpdateSchema(ctx, &gluesdk.UpdateSchemaInput{
				SchemaId: schemaID, SchemaVersionNumber: tt.number,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			got, err := client.GetSchema(ctx, &gluesdk.GetSchemaInput{SchemaId: schemaID})
			require.NoError(t, err)
			assert.Equal(t, tt.wantCheckpoint, aws.ToInt64(got.SchemaCheckpoint))
		})
	}
}
