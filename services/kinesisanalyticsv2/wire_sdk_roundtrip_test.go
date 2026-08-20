package kinesisanalyticsv2_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	kinesisanalyticsv2sdk "github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2"
	kav2types "github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

const (
	kav2RTRegion    = "us-east-1"
	kav2RTAccountID = "000000000000"
)

// newTestKAV2SDKClient stands up the real aws-sdk-go-v2 kinesisanalyticsv2
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production -- so a
// fix is verified by the real client's own deserializer, not gopherstack's
// own JSON tags.
func newTestKAV2SDKClient(t *testing.T, h *kinesisanalyticsv2.Handler) *kinesisanalyticsv2sdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(kav2RTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return kinesisanalyticsv2sdk.NewFromConfig(cfg, func(o *kinesisanalyticsv2sdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestAddApplicationInput_InputSchema_SDKRoundTrip proves that
// AddApplicationInput's InputSchema and InputParallelism -- required and
// optional members of real AWS's Input shape (aws-sdk-go-v2/service/
// kinesisanalyticsv2@v1.41.4 types/types.go:1125 "Input", InputSchema is
// "This member is required") -- survive to DescribeApplication's
// InputDescription, and that InAppStreamNames is populated per the
// documented "<NamePrefix>_NNN" convention (types/types.go:1141, Input's
// NamePrefix doc comment) instead of being silently dropped.
func TestAddApplicationInput_InputSchema_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := kinesisanalyticsv2.NewInMemoryBackend(kav2RTAccountID, kav2RTRegion)
	client := newTestKAV2SDKClient(t, kinesisanalyticsv2.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateApplication(ctx, &kinesisanalyticsv2sdk.CreateApplicationInput{
		ApplicationName:      aws.String("input-schema-app"),
		RuntimeEnvironment:   kav2types.RuntimeEnvironmentSql10,
		ServiceExecutionRole: aws.String("arn:aws:iam::000000000000:role/kav2-role"),
	})
	require.NoError(t, err)

	_, err = client.AddApplicationInput(ctx, &kinesisanalyticsv2sdk.AddApplicationInputInput{
		ApplicationName:             aws.String("input-schema-app"),
		CurrentApplicationVersionId: aws.Int64(1),
		Input: &kav2types.Input{
			NamePrefix: aws.String("SOURCE"),
			KinesisStreamsInput: &kav2types.KinesisStreamsInput{
				ResourceARN: aws.String("arn:aws:kinesis:us-east-1:000000000000:stream/src"),
			},
			InputParallelism: &kav2types.InputParallelism{Count: aws.Int32(2)},
			InputSchema: &kav2types.SourceSchema{
				RecordEncoding: aws.String("UTF-8"),
				RecordFormat: &kav2types.RecordFormat{
					RecordFormatType: kav2types.RecordFormatTypeJson,
				},
				RecordColumns: []kav2types.RecordColumn{
					{Name: aws.String("ticker"), SqlType: aws.String("VARCHAR(4)"), Mapping: aws.String("$.ticker")},
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeApplication(ctx, &kinesisanalyticsv2sdk.DescribeApplicationInput{
		ApplicationName: aws.String("input-schema-app"),
	})
	require.NoError(t, err)

	sqlDesc := out.ApplicationDetail.ApplicationConfigurationDescription.SqlApplicationConfigurationDescription
	require.NotNil(t, sqlDesc)
	require.Len(t, sqlDesc.InputDescriptions, 1)

	desc := sqlDesc.InputDescriptions[0]
	require.NotNil(t, desc.InputSchema, "InputSchema silently dropped by the real client's deserializer")
	require.NotNil(t, desc.InputSchema.RecordFormat)
	require.Equal(t, kav2types.RecordFormatTypeJson, desc.InputSchema.RecordFormat.RecordFormatType)
	require.Len(t, desc.InputSchema.RecordColumns, 1)
	require.Equal(t, "ticker", aws.ToString(desc.InputSchema.RecordColumns[0].Name))

	require.NotNil(t, desc.InputParallelism)
	require.Equal(t, int32(2), aws.ToInt32(desc.InputParallelism.Count))

	require.Equal(t, []string{"SOURCE_001", "SOURCE_002"}, desc.InAppStreamNames)
}

// TestAddApplicationReferenceDataSource_ReferenceSchema_SDKRoundTrip proves
// AddApplicationReferenceDataSource's ReferenceSchema -- a required member
// of real AWS's ReferenceDataSource shape (types/types.go:2048
// "ReferenceDataSource") -- survives to
// DescribeApplication's ReferenceDataSourceDescriptions.
func TestAddApplicationReferenceDataSource_ReferenceSchema_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := kinesisanalyticsv2.NewInMemoryBackend(kav2RTAccountID, kav2RTRegion)
	client := newTestKAV2SDKClient(t, kinesisanalyticsv2.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateApplication(ctx, &kinesisanalyticsv2sdk.CreateApplicationInput{
		ApplicationName:      aws.String("ref-schema-app"),
		RuntimeEnvironment:   kav2types.RuntimeEnvironmentSql10,
		ServiceExecutionRole: aws.String("arn:aws:iam::000000000000:role/kav2-role"),
	})
	require.NoError(t, err)

	_, err = client.AddApplicationReferenceDataSource(
		ctx,
		&kinesisanalyticsv2sdk.AddApplicationReferenceDataSourceInput{
			ApplicationName:             aws.String("ref-schema-app"),
			CurrentApplicationVersionId: aws.Int64(1),
			ReferenceDataSource: &kav2types.ReferenceDataSource{
				TableName: aws.String("ref_table"),
				S3ReferenceDataSource: &kav2types.S3ReferenceDataSource{
					BucketARN: aws.String("arn:aws:s3:::ref-bucket"),
					FileKey:   aws.String("ref.csv"),
				},
				ReferenceSchema: &kav2types.SourceSchema{
					RecordFormat: &kav2types.RecordFormat{RecordFormatType: kav2types.RecordFormatTypeCsv},
					RecordColumns: []kav2types.RecordColumn{
						{Name: aws.String("id"), SqlType: aws.String("INTEGER")},
					},
				},
			},
		},
	)
	require.NoError(t, err)

	out, err := client.DescribeApplication(ctx, &kinesisanalyticsv2sdk.DescribeApplicationInput{
		ApplicationName: aws.String("ref-schema-app"),
	})
	require.NoError(t, err)

	refs := out.ApplicationDetail.ApplicationConfigurationDescription.
		SqlApplicationConfigurationDescription.ReferenceDataSourceDescriptions
	require.Len(t, refs, 1)
	require.NotNil(t, refs[0].ReferenceSchema, "ReferenceSchema silently dropped by the real client's deserializer")
	require.Equal(t, kav2types.RecordFormatTypeCsv, refs[0].ReferenceSchema.RecordFormat.RecordFormatType)
	require.Len(t, refs[0].ReferenceSchema.RecordColumns, 1)
}

// TestUpdateApplication_InputSchemaUpdate_SDKRoundTrip proves
// UpdateApplication's InputUpdate.InputSchemaUpdate/InputParallelismUpdate
// (types/types.go:1374 "InputUpdate") apply to the existing input and that
// InAppStreamNames is regenerated for the new parallelism count.
func TestUpdateApplication_InputSchemaUpdate_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := kinesisanalyticsv2.NewInMemoryBackend(kav2RTAccountID, kav2RTRegion)
	client := newTestKAV2SDKClient(t, kinesisanalyticsv2.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateApplication(ctx, &kinesisanalyticsv2sdk.CreateApplicationInput{
		ApplicationName:      aws.String("input-update-app"),
		RuntimeEnvironment:   kav2types.RuntimeEnvironmentSql10,
		ServiceExecutionRole: aws.String("arn:aws:iam::000000000000:role/kav2-role"),
	})
	require.NoError(t, err)

	addOut, err := client.AddApplicationInput(ctx, &kinesisanalyticsv2sdk.AddApplicationInputInput{
		ApplicationName:             aws.String("input-update-app"),
		CurrentApplicationVersionId: aws.Int64(1),
		Input: &kav2types.Input{
			NamePrefix: aws.String("SOURCE"),
			KinesisStreamsInput: &kav2types.KinesisStreamsInput{
				ResourceARN: aws.String("arn:aws:kinesis:us-east-1:000000000000:stream/src"),
			},
			InputSchema: &kav2types.SourceSchema{
				RecordFormat:  &kav2types.RecordFormat{RecordFormatType: kav2types.RecordFormatTypeJson},
				RecordColumns: []kav2types.RecordColumn{{Name: aws.String("a"), SqlType: aws.String("VARCHAR(4)")}},
			},
		},
	})
	require.NoError(t, err)

	inputID := addOut.InputDescriptions[0].InputId

	_, err = client.UpdateApplication(ctx, &kinesisanalyticsv2sdk.UpdateApplicationInput{
		ApplicationName:             aws.String("input-update-app"),
		CurrentApplicationVersionId: addOut.ApplicationVersionId,
		ApplicationConfigurationUpdate: &kav2types.ApplicationConfigurationUpdate{
			SqlApplicationConfigurationUpdate: &kav2types.SqlApplicationConfigurationUpdate{
				InputUpdates: []kav2types.InputUpdate{
					{
						InputId:                inputID,
						InputParallelismUpdate: &kav2types.InputParallelismUpdate{CountUpdate: aws.Int32(3)},
						InputSchemaUpdate: &kav2types.InputSchemaUpdate{
							RecordFormatUpdate: &kav2types.RecordFormat{
								RecordFormatType: kav2types.RecordFormatTypeCsv,
							},
							RecordColumnUpdates: []kav2types.RecordColumn{
								{Name: aws.String("b"), SqlType: aws.String("INTEGER")},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeApplication(ctx, &kinesisanalyticsv2sdk.DescribeApplicationInput{
		ApplicationName: aws.String("input-update-app"),
	})
	require.NoError(t, err)

	desc := out.ApplicationDetail.ApplicationConfigurationDescription.
		SqlApplicationConfigurationDescription.InputDescriptions[0]
	require.NotNil(t, desc.InputSchema)
	require.Equal(t, kav2types.RecordFormatTypeCsv, desc.InputSchema.RecordFormat.RecordFormatType)
	require.Len(t, desc.InputSchema.RecordColumns, 1)
	require.Equal(t, "b", aws.ToString(desc.InputSchema.RecordColumns[0].Name))
	require.NotNil(t, desc.InputParallelism)
	require.Equal(t, int32(3), aws.ToInt32(desc.InputParallelism.Count))
	require.Equal(t, []string{"SOURCE_001", "SOURCE_002", "SOURCE_003"}, desc.InAppStreamNames)
}

// TestUpdateApplication_ReferenceSchemaUpdate_SDKRoundTrip proves
// ReferenceDataSourceUpdate.ReferenceSchemaUpdate applies -- note this
// member is typed plain "SourceSchema" in real AWS (types/types.go:2106
// "ReferenceDataSourceUpdate"), unlike InputUpdate.InputSchemaUpdate's own
// Update-suffixed shape, so this also exercises the asymmetric wire shape.
func TestUpdateApplication_ReferenceSchemaUpdate_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := kinesisanalyticsv2.NewInMemoryBackend(kav2RTAccountID, kav2RTRegion)
	client := newTestKAV2SDKClient(t, kinesisanalyticsv2.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateApplication(ctx, &kinesisanalyticsv2sdk.CreateApplicationInput{
		ApplicationName:      aws.String("ref-update-app"),
		RuntimeEnvironment:   kav2types.RuntimeEnvironmentSql10,
		ServiceExecutionRole: aws.String("arn:aws:iam::000000000000:role/kav2-role"),
	})
	require.NoError(t, err)

	addOut, err := client.AddApplicationReferenceDataSource(
		ctx,
		&kinesisanalyticsv2sdk.AddApplicationReferenceDataSourceInput{
			ApplicationName:             aws.String("ref-update-app"),
			CurrentApplicationVersionId: aws.Int64(1),
			ReferenceDataSource: &kav2types.ReferenceDataSource{
				TableName: aws.String("ref_table"),
				S3ReferenceDataSource: &kav2types.S3ReferenceDataSource{
					BucketARN: aws.String("arn:aws:s3:::ref-bucket"),
					FileKey:   aws.String("ref.csv"),
				},
				ReferenceSchema: &kav2types.SourceSchema{
					RecordFormat:  &kav2types.RecordFormat{RecordFormatType: kav2types.RecordFormatTypeCsv},
					RecordColumns: []kav2types.RecordColumn{{Name: aws.String("id"), SqlType: aws.String("INTEGER")}},
				},
			},
		},
	)
	require.NoError(t, err)

	refID := addOut.ReferenceDataSourceDescriptions[0].ReferenceId

	_, err = client.UpdateApplication(ctx, &kinesisanalyticsv2sdk.UpdateApplicationInput{
		ApplicationName:             aws.String("ref-update-app"),
		CurrentApplicationVersionId: addOut.ApplicationVersionId,
		ApplicationConfigurationUpdate: &kav2types.ApplicationConfigurationUpdate{
			SqlApplicationConfigurationUpdate: &kav2types.SqlApplicationConfigurationUpdate{
				ReferenceDataSourceUpdates: []kav2types.ReferenceDataSourceUpdate{
					{
						ReferenceId: refID,
						ReferenceSchemaUpdate: &kav2types.SourceSchema{
							RecordFormat: &kav2types.RecordFormat{RecordFormatType: kav2types.RecordFormatTypeJson},
							RecordColumns: []kav2types.RecordColumn{
								{Name: aws.String("name"), SqlType: aws.String("VARCHAR(32)")},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeApplication(ctx, &kinesisanalyticsv2sdk.DescribeApplicationInput{
		ApplicationName: aws.String("ref-update-app"),
	})
	require.NoError(t, err)

	ref := out.ApplicationDetail.ApplicationConfigurationDescription.
		SqlApplicationConfigurationDescription.ReferenceDataSourceDescriptions[0]
	require.NotNil(t, ref.ReferenceSchema)
	require.Equal(t, kav2types.RecordFormatTypeJson, ref.ReferenceSchema.RecordFormat.RecordFormatType)
	require.Len(t, ref.ReferenceSchema.RecordColumns, 1)
	require.Equal(t, "name", aws.ToString(ref.ReferenceSchema.RecordColumns[0].Name))
}
