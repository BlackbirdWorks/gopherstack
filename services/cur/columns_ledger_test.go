package cur_test

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"slices"
	"strings"
	"testing"
	"time"

	cursdk "github.com/aws/aws-sdk-go-v2/service/costandusagereportservice"
	curtypes "github.com/aws/aws-sdk-go-v2/service/costandusagereportservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDelivery_LedgerDimensionColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want     map[string]string
		name     string
		elements []curtypes.SchemaElement
		present  []string
		absent   []string
	}{
		{
			name:     "dimensions",
			elements: []curtypes.SchemaElement{curtypes.SchemaElementResources},
			present:  []string{"resourceTags/user:team", "discount/TotalDiscount"},
			want: map[string]string{
				"lineItem/AvailabilityZone": "us-east-1a",
				"lineItem/Operation":        "RunInstances",
				"product/instanceType":      "t3.medium",
				"product/productFamily":     "Compute Instance",
				"lineItem/LineItemType":     "Tax",
				"lineItem/ResourceId":       "i-0abc",
				"resourceTags/user:team":    "blue",
			},
		},
		{
			name:     "manual discount",
			elements: []curtypes.SchemaElement{curtypes.SchemaElementManualDiscountCompatibility},
			absent:   []string{"discount/TotalDiscount", "discount/EdpDiscount"},
			present:  []string{"discount/BundledDiscount"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.bucket(t)
			f.backend.SetCostSource(fakeCosts{{
				Day: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), Service: "Amazon Elastic Compute Cloud - Compute",
				Region: "us-east-1", UsageType: "BoxUsage:t3.medium", Account: testAccount,
				AvailabilityZone: "us-east-1a", Operation: "RunInstances", InstanceType: "t3.medium",
				ProductFamily: "Compute Instance", ChargeType: "Tax", ResourceID: "i-0abc",
				Tags: map[string]string{"team": "blue"}, Blended: 2, Unblended: 2, Usage: 4,
			}})

			d := def("dims")
			d.AdditionalSchemaElements = tc.elements
			_, err := f.client.PutReportDefinition(t.Context(), &cursdk.PutReportDefinitionInput{ReportDefinition: d})
			require.NoError(t, err)

			f.backend.DeliverDue(t.Context())

			var rows [][]string

			for _, k := range f.keys(t) {
				if !strings.HasSuffix(k, ".csv.gz") {
					continue
				}

				zr, gerr := gzip.NewReader(bytes.NewReader(f.object(t, k)))
				require.NoError(t, gerr)

				rows, err = csv.NewReader(zr).ReadAll()
				require.NoError(t, err)
			}

			require.Len(t, rows, 2)

			for _, c := range tc.present {
				assert.Contains(t, rows[0], c)
			}

			for _, c := range tc.absent {
				assert.NotContains(t, rows[0], c)
			}

			for col, want := range tc.want {
				i := slices.Index(rows[0], col)
				require.GreaterOrEqual(t, i, 0, col)
				assert.Equal(t, want, rows[1][i], col)
			}
		})
	}
}
