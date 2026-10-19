package networkfirewall_test

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/networkfirewall"
	"github.com/aws/aws-sdk-go-v2/service/networkfirewall/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// retime rewrites timeField on every record that has idField, giving the i-th id (sorted) times[i].
func retime(t *testing.T, snap []byte, timeField string, times []string) []byte {
	t.Helper()

	var root map[string]json.RawMessage

	require.NoError(t, json.Unmarshal(snap, &root))

	var tables map[string]json.RawMessage

	require.NoError(t, json.Unmarshal(root["tables"], &tables))

	for name, raw := range tables {
		var rows []map[string]any
		if json.Unmarshal(raw, &rows) != nil {
			continue
		}

		var ids []string

		for _, r := range rows {
			if _, ok := r[timeField]; ok {
				ids = append(ids, r["id"].(string))
			}
		}

		if len(ids) == 0 {
			continue
		}

		slices.Sort(ids)

		for _, r := range rows {
			if _, ok := r[timeField]; ok {
				r[timeField] = times[slices.Index(ids, r["id"].(string))]
			}
		}

		b, err := json.Marshal(rows)
		require.NoError(t, err)

		tables[name] = b
	}

	tb, err := json.Marshal(tables)
	require.NoError(t, err)

	root["tables"] = tb

	out, err := json.Marshal(root)
	require.NoError(t, err)

	return out
}

func TestListOrderingSubSecond(t *testing.T) {
	t.Parallel()

	const (
		longer  = "2024-01-01T00:00:05.12Z"
		shorter = "2024-01-01T00:00:05.1Z"
	)

	tests := []struct {
		run  func(t *testing.T, f *fixture, fwARN *string) (got []string, ids []string)
		name string
		want []int
	}{
		{name: "analysis reports newest first", want: []int{0, 1}, run: func(
			t *testing.T, f *fixture, _ *string,
		) ([]string, []string) {
			t.Helper()

			ctx := t.Context()

			_, err := f.client.UpdateFirewallAnalysisSettings(ctx, &sdk.UpdateFirewallAnalysisSettingsInput{
				FirewallName:         aws.String("fw"),
				EnabledAnalysisTypes: []types.EnabledAnalysisType{types.EnabledAnalysisTypeTlsSni},
			})
			require.NoError(t, err)

			for range 2 {
				_, err = f.client.StartAnalysisReport(ctx, &sdk.StartAnalysisReportInput{
					FirewallName: aws.String("fw"), AnalysisType: types.EnabledAnalysisTypeTlsSni,
				})
				require.NoError(t, err)
			}

			dst := newFixture(t)
			require.NoError(t, dst.handler.Restore(ctx, retime(t, f.handler.Snapshot(ctx), "reportTime",
				[]string{longer, shorter})))

			out, err := dst.client.ListAnalysisReports(
				ctx, &sdk.ListAnalysisReportsInput{FirewallName: aws.String("fw")},
			)
			require.NoError(t, err)

			got := make([]string, 0, len(out.AnalysisReports))
			for _, r := range out.AnalysisReports {
				got = append(got, aws.ToString(r.AnalysisReportId))
			}

			all, err := f.client.ListAnalysisReports(ctx, &sdk.ListAnalysisReportsInput{FirewallName: aws.String("fw")})
			require.NoError(t, err)

			ids := make([]string, 0, len(all.AnalysisReports))
			for _, r := range all.AnalysisReports {
				ids = append(ids, aws.ToString(r.AnalysisReportId))
			}

			slices.Sort(ids)

			return got, ids
		}},
		{name: "flow operations oldest first", want: []int{1, 0}, run: func(
			t *testing.T, f *fixture, fwARN *string,
		) ([]string, []string) {
			t.Helper()

			ctx := t.Context()
			filters := []types.FlowFilter{{
				SourceAddress: &types.Address{AddressDefinition: aws.String("10.0.0.0/8")}, Protocols: []string{"6"},
			}}

			for range 2 {
				_, err := f.client.StartFlowFlush(ctx, &sdk.StartFlowFlushInput{
					FirewallArn: fwARN, AvailabilityZone: aws.String(testRegion + "a"), FlowFilters: filters,
				})
				require.NoError(t, err)
			}

			dst := newFixture(t)
			require.NoError(t, dst.handler.Restore(ctx, retime(t, f.handler.Snapshot(ctx), "requested",
				[]string{longer, shorter})))

			out, err := dst.client.ListFlowOperations(ctx, &sdk.ListFlowOperationsInput{FirewallArn: fwARN})
			require.NoError(t, err)

			got := make([]string, 0, len(out.FlowOperations))
			for _, o := range out.FlowOperations {
				got = append(got, aws.ToString(o.FlowOperationId))
			}

			all, err := f.client.ListFlowOperations(ctx, &sdk.ListFlowOperationsInput{FirewallArn: fwARN})
			require.NoError(t, err)

			ids := make([]string, 0, len(all.FlowOperations))
			for _, o := range all.FlowOperations {
				ids = append(ids, aws.ToString(o.FlowOperationId))
			}

			slices.Sort(ids)

			return got, ids
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			pol := f.policy(t, "pol")
			fw := f.firewall(t, "fw", aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn))

			got, ids := tt.run(t, f, fw.Firewall.FirewallArn)
			require.Len(t, ids, 2)
			require.Len(t, got, 2)

			assert.Equal(t, []string{ids[tt.want[0]], ids[tt.want[1]]}, got)
		})
	}
}

func TestAssociateSubnetsReusesFreedZone(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	pol := f.policy(t, "pol")
	f.firewall(t, "fw", aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn))

	_, err := f.client.AssociateSubnets(ctx, &sdk.AssociateSubnetsInput{
		FirewallName: aws.String("fw"), SubnetMappings: []types.SubnetMapping{{SubnetId: aws.String("subnet-bbbb")}},
	})
	require.NoError(t, err)

	_, err = f.client.DisassociateSubnets(ctx, &sdk.DisassociateSubnetsInput{
		FirewallName: aws.String("fw"), SubnetIds: []string{"subnet-aaaa"},
	})
	require.NoError(t, err)

	out, err := f.client.AssociateSubnets(ctx, &sdk.AssociateSubnetsInput{
		FirewallName: aws.String("fw"), SubnetMappings: []types.SubnetMapping{{SubnetId: aws.String("subnet-cccc")}},
	})
	require.NoError(t, err)
	assert.Len(t, out.SubnetMappings, 2)
}
