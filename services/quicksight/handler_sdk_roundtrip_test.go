package quicksight_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// newTestQuickSightClient stands up the real aws-sdk-go-v2 QuickSight client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production. See redshift's
// handler_sdk_roundtrip_test.go for why this matters over doRequest/
// httptest.NewRequest: the SDK's own deserializer -- not a handler-shaped
// fixture -- is what proves a response is wire-compatible, and the SDK's own
// serializer/validators are what prove a request was actually exercised the
// way a real client sends it (e.g. CreateDataSetInput's client-side
// PhysicalTableMap-required check in validators.go).
func newTestQuickSightClient(t *testing.T, h *quicksight.Handler) *quicksightsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtQSTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return quicksightsdk.NewFromConfig(cfg, func(o *quicksightsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

const rtQSTestRegion = "us-east-1"

// TestSDKRoundTrip_DataSetPhysicalTableMap locks the fix for gopherstack-2qk4:
// CreateDataSet/UpdateDataSet never read PhysicalTableMap (required at
// quicksight@v1.123.1 api_op_CreateDataSet.go:55, api_op_UpdateDataSet.go:55),
// so a dataset reported success with no tables behind it. Driving the real
// SDK client is what proves this: the client's own validators.go
// (validateOpCreateDataSetInput) refuses to send a request missing
// PhysicalTableMap at all, so unlike a hand-built JSON fixture this test
// cannot accidentally omit the field the way the original bug did.
func TestSDKRoundTrip_DataSetPhysicalTableMap(t *testing.T) {
	t.Parallel()

	backend := quicksight.NewInMemoryBackend("000000000000", rtQSTestRegion)
	h := quicksight.NewHandler(backend)
	client := newTestQuickSightClient(t, h)
	ctx := t.Context()

	physicalTableMap := map[string]types.PhysicalTable{
		"pt1": &types.PhysicalTableMemberRelationalTable{
			Value: types.RelationalTable{
				DataSourceArn: aws.String("arn:aws:quicksight:us-east-1:000000000000:datasource/src1"),
				Name:          aws.String("orders"),
				Schema:        aws.String("public"),
				InputColumns: []types.InputColumn{
					{Name: aws.String("id"), Type: types.InputColumnDataTypeInteger},
					{Name: aws.String("total"), Type: types.InputColumnDataTypeDecimal},
				},
			},
		},
	}
	logicalTableMap := map[string]types.LogicalTable{
		"lt1": {
			Alias:  aws.String("orders_logical"),
			Source: &types.LogicalTableSource{PhysicalTableId: aws.String("pt1")},
		},
	}

	_, err := client.CreateDataSet(ctx, &quicksightsdk.CreateDataSetInput{
		AwsAccountId:     aws.String("000000000000"),
		DataSetId:        aws.String("rt-ds1"),
		Name:             aws.String("RoundTrip DataSet"),
		ImportMode:       types.DataSetImportModeDirectQuery,
		PhysicalTableMap: physicalTableMap,
		LogicalTableMap:  logicalTableMap,
	})
	require.NoError(t, err)

	out, err := client.DescribeDataSet(ctx, &quicksightsdk.DescribeDataSetInput{
		AwsAccountId: aws.String("000000000000"),
		DataSetId:    aws.String("rt-ds1"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DataSet)

	require.Len(t, out.DataSet.PhysicalTableMap, 1, "the dataset created above declared exactly one physical table")
	member, ok := out.DataSet.PhysicalTableMap["pt1"].(*types.PhysicalTableMemberRelationalTable)
	require.True(t, ok, "PhysicalTableMap[pt1] must round-trip as the RelationalTable variant it was created with")
	assert.Equal(
		t,
		"arn:aws:quicksight:us-east-1:000000000000:datasource/src1",
		aws.ToString(member.Value.DataSourceArn),
	)
	assert.Equal(t, "orders", aws.ToString(member.Value.Name))
	assert.Equal(t, "public", aws.ToString(member.Value.Schema))
	require.Len(t, member.Value.InputColumns, 2)
	assert.Equal(t, "id", aws.ToString(member.Value.InputColumns[0].Name))
	assert.Equal(t, types.InputColumnDataTypeInteger, member.Value.InputColumns[0].Type)

	require.Len(t, out.DataSet.LogicalTableMap, 1, "the dataset created above also declared one logical table")
	lt := out.DataSet.LogicalTableMap["lt1"]
	assert.Equal(t, "orders_logical", aws.ToString(lt.Alias))
	require.NotNil(t, lt.Source)
	assert.Equal(t, "pt1", aws.ToString(lt.Source.PhysicalTableId))
}

// TestSDKRoundTrip_UpdateDataSetPhysicalTableMap proves UpdateDataSet also
// threads PhysicalTableMap through (it shares the same required-field bug
// CreateDataSet had) and that a changed table definition is observable on
// the next DescribeDataSet, not just a 2xx from the update call.
func TestSDKRoundTrip_UpdateDataSetPhysicalTableMap(t *testing.T) {
	t.Parallel()

	backend := quicksight.NewInMemoryBackend("000000000000", rtQSTestRegion)
	h := quicksight.NewHandler(backend)
	client := newTestQuickSightClient(t, h)
	ctx := t.Context()

	firstTable := map[string]types.PhysicalTable{
		"pt1": &types.PhysicalTableMemberRelationalTable{
			Value: types.RelationalTable{
				DataSourceArn: aws.String("arn:aws:quicksight:us-east-1:000000000000:datasource/src1"),
				Name:          aws.String("orders"),
				InputColumns: []types.InputColumn{
					{Name: aws.String("id"), Type: types.InputColumnDataTypeInteger},
				},
			},
		},
	}
	_, err := client.CreateDataSet(ctx, &quicksightsdk.CreateDataSetInput{
		AwsAccountId:     aws.String("000000000000"),
		DataSetId:        aws.String("rt-ds2"),
		Name:             aws.String("Original"),
		ImportMode:       types.DataSetImportModeDirectQuery,
		PhysicalTableMap: firstTable,
	})
	require.NoError(t, err)

	replacementTable := map[string]types.PhysicalTable{
		"pt2": &types.PhysicalTableMemberCustomSql{
			Value: types.CustomSql{
				DataSourceArn: aws.String("arn:aws:quicksight:us-east-1:000000000000:datasource/src1"),
				Name:          aws.String("custom_orders"),
				SqlQuery:      aws.String("SELECT * FROM orders"),
			},
		},
	}
	_, err = client.UpdateDataSet(ctx, &quicksightsdk.UpdateDataSetInput{
		AwsAccountId:     aws.String("000000000000"),
		DataSetId:        aws.String("rt-ds2"),
		Name:             aws.String("Updated"),
		ImportMode:       types.DataSetImportModeDirectQuery,
		PhysicalTableMap: replacementTable,
	})
	require.NoError(t, err)

	out, err := client.DescribeDataSet(ctx, &quicksightsdk.DescribeDataSetInput{
		AwsAccountId: aws.String("000000000000"),
		DataSetId:    aws.String("rt-ds2"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DataSet)
	require.Len(t, out.DataSet.PhysicalTableMap, 1, "UpdateDataSet must replace, not merge, PhysicalTableMap")

	member, ok := out.DataSet.PhysicalTableMap["pt2"].(*types.PhysicalTableMemberCustomSql)
	require.True(t, ok, "the updated dataset's only physical table must be the CustomSql variant just sent")
	assert.Equal(t, "SELECT * FROM orders", aws.ToString(member.Value.SqlQuery))
	assert.NotContains(t, out.DataSet.PhysicalTableMap, "pt1", "the old table id must not survive the update")
}
