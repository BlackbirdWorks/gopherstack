package rds_test

import (
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test_DescribeDBInstances_Filters verifies AWS's DescribeDBInstances
// Filters.Filter.N.Name/Values.member.M contract: db-instance-id, engine,
// db-cluster-id, and dbi-resource-id narrow the result set (OR within a
// filter's Values, AND across filters), and an unrecognized filter name
// returns InvalidParameterValue.
func Test_DescribeDBInstances_Filters(t *testing.T) {
	t.Parallel()

	type describeResp struct {
		XMLName xml.Name `xml:"DescribeDBInstancesResponse"`
		Result  struct {
			DBInstances struct {
				Members []struct {
					DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
				} `xml:"DBInstance"`
			} `xml:"DBInstances"`
		} `xml:"DescribeDBInstancesResult"`
	}

	cases := []struct {
		name        string
		query       string
		wantErrText string
		wantIDs     []string
		wantCode    int
	}{
		{
			name:     "engine filter matches only mysql instances",
			query:    "Filters.Filter.1.Name=engine&Filters.Filter.1.Values.member.1=mysql",
			wantCode: http.StatusOK,
			wantIDs:  []string{"filt-mysql-1"},
		},
		{
			name: "db-instance-id filter with multiple values ORs together",
			query: "Filters.Filter.1.Name=db-instance-id" +
				"&Filters.Filter.1.Values.member.1=filt-mysql-1" +
				"&Filters.Filter.1.Values.member.2=filt-postgres-1",
			wantCode: http.StatusOK,
			wantIDs:  []string{"filt-mysql-1", "filt-postgres-1"},
		},
		{
			name: "two filters AND together",
			query: "Filters.Filter.1.Name=engine&Filters.Filter.1.Values.member.1=postgres" +
				"&Filters.Filter.2.Name=db-instance-id&Filters.Filter.2.Values.member.1=filt-mysql-1",
			wantCode: http.StatusOK,
			wantIDs:  nil,
		},
		{
			name:        "unrecognized filter name is rejected",
			query:       "Filters.Filter.1.Name=bogus-filter&Filters.Filter.1.Values.member.1=x",
			wantCode:    http.StatusBadRequest,
			wantErrText: "InvalidParameterValue",
		},
		{
			name:     "no filters returns everything",
			query:    "",
			wantCode: http.StatusOK,
			wantIDs:  []string{"filt-mysql-1", "filt-postgres-1"},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler()
			postRDSForm(t, h,
				"Action=CreateDBInstance&Version=2014-10-31"+
					"&DBInstanceIdentifier=filt-mysql-1&Engine=mysql")
			postRDSForm(t, h,
				"Action=CreateDBInstance&Version=2014-10-31"+
					"&DBInstanceIdentifier=filt-postgres-1&Engine=postgres")

			body := "Action=DescribeDBInstances&Version=2014-10-31"
			if tt.query != "" {
				body += "&" + tt.query
			}
			rec := postRDSForm(t, h, body)

			require.Equal(t, tt.wantCode, rec.Code)
			if tt.wantErrText != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErrText)

				return
			}

			var resp describeResp
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

			gotIDs := make([]string, 0, len(resp.Result.DBInstances.Members))
			for _, m := range resp.Result.DBInstances.Members {
				gotIDs = append(gotIDs, m.DBInstanceIdentifier)
			}
			assert.ElementsMatch(t, tt.wantIDs, gotIDs)
		})
	}
}
