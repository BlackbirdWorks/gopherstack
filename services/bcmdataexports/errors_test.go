package bcmdataexports_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bcmsdk "github.com/aws/aws-sdk-go-v2/service/bcmdataexports"
	bcmtypes "github.com/aws/aws-sdk-go-v2/service/bcmdataexports/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceNotFoundBodyFields(t *testing.T) {
	t.Parallel()

	const missing = "arn:aws:bcm-data-exports:us-east-1:123456789012:export/none-0"

	tests := []struct {
		call     func(f *fixture) error
		name     string
		wantType string
		wantID   string
	}{
		{
			name: "export",
			call: func(f *fixture) error {
				_, err := f.client.GetExport(t.Context(), &bcmsdk.GetExportInput{ExportArn: aws.String(missing)})

				return err
			},
			wantType: "Export", wantID: missing,
		},
		{
			name: "table",
			call: func(f *fixture) error {
				_, err := f.client.GetTable(t.Context(), &bcmsdk.GetTableInput{TableName: aws.String("NOPE")})

				return err
			},
			wantType: "Table", wantID: "NOPE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.call(newFixture(t))

			nf, ok := errors.AsType[*bcmtypes.ResourceNotFoundException](err)
			require.True(t, ok, "got %v", err)
			assert.NotEmpty(t, aws.ToString(nf.Message))
			assert.Equal(t, tc.wantType, aws.ToString(nf.ResourceType))
			assert.Equal(t, tc.wantID, aws.ToString(nf.ResourceId))
		})
	}
}

func TestValidationBodyFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call       func(f *fixture) error
		name       string
		wantReason bcmtypes.ValidationExceptionReason
		wantField  string
	}{
		{
			name: "bad name",
			call: func(f *fixture) error {
				_, err := f.client.CreateExport(
					t.Context(),
					&bcmsdk.CreateExportInput{Export: export("bad name!", curQuery)},
				)

				return err
			},
			wantReason: bcmtypes.ValidationExceptionReasonFieldValidationFailed, wantField: "Export.Name",
		},
		{
			name: "bad query",
			call: func(f *fixture) error {
				e := export("e1", "DELETE FROM x")
				_, err := f.client.CreateExport(t.Context(), &bcmsdk.CreateExportInput{Export: e})

				return err
			},
			wantReason: bcmtypes.ValidationExceptionReasonOther,
		},
		{
			name: "max results",
			call: func(f *fixture) error {
				_, err := f.client.ListExports(t.Context(), &bcmsdk.ListExportsInput{MaxResults: aws.Int32(1000)})

				return err
			},
			wantReason: bcmtypes.ValidationExceptionReasonFieldValidationFailed, wantField: "MaxResults",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.call(newFixture(t))

			ve, ok := errors.AsType[*bcmtypes.ValidationException](err)
			require.True(t, ok, "got %v", err)
			assert.Equal(t, tc.wantReason, ve.Reason)

			if tc.wantField == "" {
				assert.Empty(t, ve.Fields)

				return
			}

			require.Len(t, ve.Fields, 1)
			assert.Equal(t, tc.wantField, aws.ToString(ve.Fields[0].Name))
			assert.NotEmpty(t, aws.ToString(ve.Fields[0].Message))
		})
	}
}
