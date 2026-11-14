package bcmdataexports_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	bcmsdk "github.com/aws/aws-sdk-go-v2/service/bcmdataexports"
	bcmtypes "github.com/aws/aws-sdk-go-v2/service/bcmdataexports/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaxResultsBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		code string
		name string
		max  int32
	}{
		{name: "min", max: 1},
		{name: "max", max: 300},
		{name: "zero", max: 0, code: "ValidationException"},
		{name: "over", max: 301, code: "ValidationException"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			arn := f.create(t, export("e1", "SELECT line_item_usage_type FROM COST_AND_USAGE_REPORT"))

			_, err := f.client.ListExports(t.Context(), &bcmsdk.ListExportsInput{MaxResults: aws.Int32(tc.max)})
			assert.Equal(t, tc.code, apiCode(err))

			_, err = f.client.ListExecutions(t.Context(), &bcmsdk.ListExecutionsInput{
				ExportArn: aws.String(arn), MaxResults: aws.Int32(tc.max),
			})
			assert.Equal(t, tc.code, apiCode(err))

			_, err = f.client.ListTables(t.Context(), &bcmsdk.ListTablesInput{MaxResults: aws.Int32(tc.max)})
			assert.Equal(t, tc.code, apiCode(err))

			_, err = f.client.ListTagsForResource(t.Context(), &bcmsdk.ListTagsForResourceInput{
				ResourceArn: aws.String(arn), MaxResults: aws.Int32(tc.max),
			})
			assert.Equal(t, tc.code, apiCode(err))
		})
	}
}

func TestExportQuotaPerTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		limit int
	}{
		{name: "cur", query: "SELECT line_item_usage_type FROM COST_AND_USAGE_REPORT", limit: 5},
		{name: "focus 1.2", query: "SELECT BilledCost FROM FOCUS_1_2_AWS", limit: 2},
		{name: "focus 1.0", query: "SELECT BilledCost FROM FOCUS_1_0_AWS", limit: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)

			for i := range tc.limit {
				f.create(t, export("e"+strconv.Itoa(i), tc.query))
			}

			_, err := f.client.CreateExport(t.Context(), &bcmsdk.CreateExportInput{
				Export: export("over", tc.query),
			})
			require.Error(t, err)

			var q *bcmtypes.ServiceQuotaExceededException

			require.ErrorAs(t, err, &q)
			assert.Equal(t, "AWSBillingAndCostManagementDataExports", aws.ToString(q.ServiceCode))
			assert.Equal(t, "Export", aws.ToString(q.ResourceType))
			assert.Equal(t, "over", aws.ToString(q.ResourceId))

			other := "SELECT usage_type FROM COST_AND_USAGE_DASHBOARD"
			f.create(t, export("other", other))
		})
	}
}

func TestTableSchemas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props   map[string]string
		name    string
		table   string
		present []string
		absent  []string
	}{
		{
			name:  "cur default",
			table: "COST_AND_USAGE_REPORT",
			present: []string{
				"bill_payer_account_name", "line_item_usage_account_name", "pricing_rate_code", "product_instance_type",
				"reservation_reservation_a_r_n", "savings_plan_savings_plan_a_r_n", "discount_total_discount", "tags",
				"resource_tags", "cost_category", "line_item_net_unblended_cost",
			},
			absent: []string{
				"line_item_resource_id",
				"split_line_item_split_cost",
				"capacity_reservation_capacity_reservation_arn",
			},
		},
		{
			name: "cur options", table: "COST_AND_USAGE_REPORT",
			props: map[string]string{
				"INCLUDE_RESOURCES": "TRUE", "INCLUDE_SPLIT_COST_ALLOCATION_DATA": "TRUE",
				"INCLUDE_CAPACITY_RESERVATION_DATA": "TRUE", "INCLUDE_IAM_PRINCIPAL_DATA": "TRUE",
				"INCLUDE_MANUAL_DISCOUNT_COMPATIBILITY": "TRUE",
			},
			present: []string{
				"line_item_resource_id",
				"split_line_item_actual_usage",
				"capacity_reservation_capacity_reservation_type",
				"line_item_iam_principal",
				"discount_bundled_discount",
			},
			absent: []string{"discount", "discount_total_discount"},
		},
		{
			name: "focus 1.2", table: "FOCUS_1_2_AWS",
			present: []string{
				"BillingAccountType", "CapacityReservationId", "CommitmentDiscountUnit", "InvoiceId", "SkuMeter",
				"PricingCurrencyEffectiveCost", "ServiceSubcategory", "SubAccountType", "x_Operation", "x_ServiceCode",
				"x_Discounts",
			},
			absent: []string{"x_UsageType", "x_CostCategories"},
		},
		{
			name: "focus 1.0", table: "FOCUS_1_0_AWS",
			present: []string{"AvailabilityZone", "CommitmentDiscountStatus", "x_UsageType", "x_CostCategories"},
			absent:  []string{"InvoiceId", "SkuMeter"},
		},
		{
			name:    "dashboard",
			table:   "COST_AND_USAGE_DASHBOARD",
			present: []string{"ri_sp_arn", "usage_date", "public_cost"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)

			out, err := f.client.GetTable(t.Context(), &bcmsdk.GetTableInput{
				TableName: aws.String(tc.table), TableProperties: tc.props,
			})
			require.NoError(t, err)

			cols := map[string]bool{}
			for _, c := range out.Schema {
				cols[aws.ToString(c.Name)] = true
			}

			for _, c := range tc.present {
				assert.True(t, cols[c], c)
			}

			for _, c := range tc.absent {
				assert.False(t, cols[c], c)
			}
		})
	}
}

func TestDeliveryLedgerDimensionsAndManifest(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetCostSource(fakeCosts{{
		Day: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), Service: "Amazon Elastic Compute Cloud - Compute",
		Region: "us-east-1", UsageType: "BoxUsage:t3.medium", Account: testAccount, AvailabilityZone: "us-east-1a",
		Operation: "RunInstances", ChargeType: "Tax", Blended: 2, Unblended: 2, Usage: 4,
	}})
	e := export("dims", "SELECT x_Operation, AvailabilityZone, ChargeCategory, ServiceName FROM FOCUS_1_2_AWS")
	e.DestinationConfigurations.S3Destination.S3OutputConfigurations.Overwrite = bcmtypes.OverwriteOptionCreateNewReport
	e.DestinationConfigurations.S3Destination.S3OutputConfigurations.Compression = bcmtypes.CompressionOptionGzip
	f.create(t, e)
	f.settle(t)

	var manifests []string

	for _, k := range f.keys(t) {
		if strings.HasSuffix(k, "-Manifest.json") {
			manifests = append(manifests, k)
		}
	}

	var data string

	for _, k := range f.keys(t) {
		if strings.HasSuffix(k, ".csv.gz") {
			zr, err := gzip.NewReader(bytes.NewReader(f.object(t, k)))
			require.NoError(t, err)

			raw, err := io.ReadAll(zr)
			require.NoError(t, err)

			data = string(raw)
		}
	}

	lines := strings.Split(strings.TrimSpace(data), "\n")
	require.Len(t, lines, 25)
	assert.Equal(t, "RunInstances,us-east-1a,Tax,Amazon Elastic Compute Cloud - Compute", lines[1])
	assert.Contains(t, lines[0], "x_Operation")

	require.Len(t, manifests, 2)
	assert.Contains(t, manifests, "out/dims/metadata/BILLING_PERIOD=2026-03/dims-Manifest.json")
	assert.Contains(t, string(f.object(t, manifests[0])), `"additionalOutputFiles":[]`)
}
