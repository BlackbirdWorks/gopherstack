package networkfirewall_test

import (
	"testing"
	"testing/synctest"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/networkfirewall"
	"github.com/aws/aws-sdk-go-v2/service/networkfirewall/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (f *fixture) endpointAssoc(t *testing.T, fwARN *string) *sdk.CreateVpcEndpointAssociationOutput {
	t.Helper()

	out, err := f.client.CreateVpcEndpointAssociation(t.Context(), &sdk.CreateVpcEndpointAssociationInput{
		FirewallArn:   fwARN,
		VpcId:         aws.String("vpc-0aaaaaaaaaaaaaaaa"),
		SubnetMapping: &types.SubnetMapping{SubnetId: aws.String("subnet-bbbb")},
		Description:   aws.String("assoc"),
		Tags:          []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
	})
	require.NoError(t, err)

	return out
}

func TestVpcEndpointAssociations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "lifecycle", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			pol := f.policy(t, "pol")
			fw := f.firewall(t, "fw", aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn))

			created := f.endpointAssoc(t, fw.Firewall.FirewallArn)
			assoc := created.VpcEndpointAssociation
			assert.Contains(t, aws.ToString(assoc.VpcEndpointAssociationArn), ":vpc-endpoint-association/")
			assert.Equal(t, types.FirewallStatusValueProvisioning, created.VpcEndpointAssociationStatus.Status)
			require.Len(t, created.VpcEndpointAssociationStatus.AssociationSyncState, 1)

			require.Eventually(t, func() bool {
				d, err := f.client.DescribeVpcEndpointAssociation(ctx, &sdk.DescribeVpcEndpointAssociationInput{
					VpcEndpointAssociationArn: assoc.VpcEndpointAssociationArn,
				})

				return err == nil && d.VpcEndpointAssociationStatus.Status == types.FirewallStatusValueReady
			}, 5e9, 10e6)

			d, err := f.client.DescribeVpcEndpointAssociation(ctx, &sdk.DescribeVpcEndpointAssociationInput{
				VpcEndpointAssociationArn: assoc.VpcEndpointAssociationArn,
			})
			require.NoError(t, err)

			for _, st := range d.VpcEndpointAssociationStatus.AssociationSyncState {
				assert.Equal(t, types.AttachmentStatusReady, st.Attachment.Status)
				assert.Equal(t, "subnet-bbbb", aws.ToString(st.Attachment.SubnetId))
			}

			assert.Equal(t, "assoc", aws.ToString(d.VpcEndpointAssociation.Description))
			require.Len(t, d.VpcEndpointAssociation.Tags, 1)

			tagged, err := f.client.ListTagsForResource(ctx, &sdk.ListTagsForResourceInput{
				ResourceArn: assoc.VpcEndpointAssociationArn,
			})
			require.NoError(t, err)
			assert.Len(t, tagged.Tags, 1)

			_, err = f.client.CreateVpcEndpointAssociation(ctx, &sdk.CreateVpcEndpointAssociationInput{
				FirewallArn: fw.Firewall.FirewallArn, VpcId: aws.String("vpc-0aaaaaaaaaaaaaaaa"),
				SubnetMapping: &types.SubnetMapping{SubnetId: aws.String("subnet-bbbb")},
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			list, err := f.client.ListVpcEndpointAssociations(ctx, &sdk.ListVpcEndpointAssociationsInput{
				FirewallArn: fw.Firewall.FirewallArn,
			})
			require.NoError(t, err)
			require.Len(t, list.VpcEndpointAssociations, 1)

			other, err := f.client.ListVpcEndpointAssociations(ctx, &sdk.ListVpcEndpointAssociationsInput{
				FirewallArn: aws.String("arn:aws:network-firewall:us-east-1:123456789012:firewall/other"),
			})
			require.NoError(t, err)
			assert.Empty(t, other.VpcEndpointAssociations)

			del, err := f.client.DeleteVpcEndpointAssociation(ctx, &sdk.DeleteVpcEndpointAssociationInput{
				VpcEndpointAssociationArn: assoc.VpcEndpointAssociationArn,
			})
			require.NoError(t, err)
			assert.Equal(t, types.FirewallStatusValueDeleting, del.VpcEndpointAssociationStatus.Status)

			require.Eventually(t, func() bool {
				_, derr := f.client.DescribeVpcEndpointAssociation(ctx, &sdk.DescribeVpcEndpointAssociationInput{
					VpcEndpointAssociationArn: assoc.VpcEndpointAssociationArn,
				})

				return derr != nil
			}, 5e9, 10e6)
		}},
		{name: "validation and cascade", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			_, err := f.client.CreateVpcEndpointAssociation(ctx, &sdk.CreateVpcEndpointAssociationInput{
				FirewallArn: aws.String("arn:aws:network-firewall:us-east-1:123456789012:firewall/ghost"),
				VpcId:       aws.String("vpc-1"), SubnetMapping: &types.SubnetMapping{SubnetId: aws.String("subnet-1")},
			})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))

			_, err = f.client.DescribeVpcEndpointAssociation(ctx, &sdk.DescribeVpcEndpointAssociationInput{
				VpcEndpointAssociationArn: aws.String(
					"arn:aws:network-firewall:us-east-1:123456789012:vpc-endpoint-association/x",
				),
			})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))

			pol := f.policy(t, "pol")
			fw := f.firewall(t, "fw", aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn))
			assoc := f.endpointAssoc(t, fw.Firewall.FirewallArn)

			_, err = f.client.DeleteFirewall(ctx, &sdk.DeleteFirewallInput{FirewallName: aws.String("fw")})
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				_, derr := f.client.DescribeVpcEndpointAssociation(ctx, &sdk.DescribeVpcEndpointAssociationInput{
					VpcEndpointAssociationArn: assoc.VpcEndpointAssociation.VpcEndpointAssociationArn,
				})

				return derr != nil
			}, 5e9, 10e6)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				tt.run(t, newFixture(t))
			})
		})
	}
}

func TestFlowOperations(t *testing.T) {
	t.Parallel()

	filters := []types.FlowFilter{{
		SourceAddress: &types.Address{AddressDefinition: aws.String("10.0.0.0/8")}, Protocols: []string{"6"},
	}}

	tests := []struct {
		run  func(t *testing.T, f *fixture, fwARN *string)
		name string
	}{
		{name: "capture and flush", run: func(t *testing.T, f *fixture, fwARN *string) {
			t.Helper()

			ctx := t.Context()
			zone := testRegion + "a"

			capture, err := f.client.StartFlowCapture(ctx, &sdk.StartFlowCaptureInput{
				FirewallArn: fwARN, AvailabilityZone: aws.String(zone), FlowFilters: filters,
				MinimumFlowAgeInSeconds: aws.Int32(30),
			})
			require.NoError(t, err)
			assert.Equal(t, types.FlowOperationStatusInProgress, capture.FlowOperationStatus)

			flush, err := f.client.StartFlowFlush(ctx, &sdk.StartFlowFlushInput{
				FirewallArn: fwARN, AvailabilityZone: aws.String(zone), FlowFilters: filters,
			})
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				d, derr := f.client.DescribeFlowOperation(ctx, &sdk.DescribeFlowOperationInput{
					FirewallArn: fwARN, FlowOperationId: capture.FlowOperationId,
				})

				return derr == nil && d.FlowOperationStatus == types.FlowOperationStatusCompleted
			}, 5e9, 10e6)

			d, err := f.client.DescribeFlowOperation(ctx, &sdk.DescribeFlowOperationInput{
				FirewallArn: fwARN, FlowOperationId: capture.FlowOperationId,
			})
			require.NoError(t, err)
			assert.Equal(t, types.FlowOperationTypeFlowCapture, d.FlowOperationType)
			assert.EqualValues(t, 30, aws.ToInt32(d.FlowOperation.MinimumFlowAgeInSeconds))
			require.Len(t, d.FlowOperation.FlowFilters, 1)
			assert.Equal(t, "10.0.0.0/8", aws.ToString(d.FlowOperation.FlowFilters[0].SourceAddress.AddressDefinition))
			assert.Equal(t, zone, aws.ToString(d.AvailabilityZone))
			assert.NotNil(t, d.FlowRequestTimestamp)

			all, err := f.client.ListFlowOperations(ctx, &sdk.ListFlowOperationsInput{FirewallArn: fwARN})
			require.NoError(t, err)
			assert.Len(t, all.FlowOperations, 2)

			flushes, err := f.client.ListFlowOperations(ctx, &sdk.ListFlowOperationsInput{
				FirewallArn: fwARN, FlowOperationType: types.FlowOperationTypeFlowFlush,
			})
			require.NoError(t, err)
			require.Len(t, flushes.FlowOperations, 1)
			assert.Equal(
				t,
				aws.ToString(flush.FlowOperationId),
				aws.ToString(flushes.FlowOperations[0].FlowOperationId),
			)

			res, err := f.client.ListFlowOperationResults(ctx, &sdk.ListFlowOperationResultsInput{
				FirewallArn: fwARN, FlowOperationId: capture.FlowOperationId,
			})
			require.NoError(t, err)
			assert.Empty(t, res.Flows)
			assert.Equal(t, types.FlowOperationStatusCompleted, res.FlowOperationStatus)
		}},
		{name: "validation", run: func(t *testing.T, f *fixture, fwARN *string) {
			t.Helper()

			ctx := t.Context()

			_, err := f.client.StartFlowCapture(
				ctx,
				&sdk.StartFlowCaptureInput{FirewallArn: fwARN, FlowFilters: filters},
			)
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.client.StartFlowCapture(ctx, &sdk.StartFlowCaptureInput{
				FirewallArn: fwARN, FlowFilters: filters, AvailabilityZone: aws.String("mars-1a"),
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.client.StartFlowFlush(ctx, &sdk.StartFlowFlushInput{
				FirewallArn: fwARN,
				FlowFilters: filters,
				VpcEndpointAssociationArn: aws.String(
					"arn:aws:network-firewall:us-east-1:123456789012:vpc-endpoint-association/x",
				),
			})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))

			_, err = f.client.DescribeFlowOperation(ctx, &sdk.DescribeFlowOperationInput{
				FirewallArn: fwARN, FlowOperationId: aws.String("ghost"),
			})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))

			_, err = f.client.StartFlowFlush(ctx, &sdk.StartFlowFlushInput{
				FirewallArn: aws.String("arn:aws:network-firewall:us-east-1:123456789012:firewall/ghost"),
				FlowFilters: filters, AvailabilityZone: aws.String(testRegion + "a"),
			})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))
		}},
		{name: "association target", run: func(t *testing.T, f *fixture, fwARN *string) {
			t.Helper()

			ctx := t.Context()
			assoc := f.endpointAssoc(t, fwARN)

			var endpointID *string
			for _, st := range assoc.VpcEndpointAssociationStatus.AssociationSyncState {
				endpointID = st.Attachment.EndpointId
			}

			op, err := f.client.StartFlowCapture(ctx, &sdk.StartFlowCaptureInput{
				FirewallArn: fwARN, FlowFilters: filters,
				VpcEndpointAssociationArn: assoc.VpcEndpointAssociation.VpcEndpointAssociationArn,
				VpcEndpointId:             endpointID,
			})
			require.NoError(t, err)

			d, err := f.client.DescribeFlowOperation(ctx, &sdk.DescribeFlowOperationInput{
				FirewallArn: fwARN, FlowOperationId: op.FlowOperationId, VpcEndpointId: endpointID,
			})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(endpointID), aws.ToString(d.VpcEndpointId))

			_, err = f.client.DescribeFlowOperation(ctx, &sdk.DescribeFlowOperationInput{
				FirewallArn: fwARN, FlowOperationId: op.FlowOperationId, VpcEndpointId: aws.String("vpce-other"),
			})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t)
				pol := f.policy(t, "pol")
				fw := f.firewall(t, "fw", aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn))
				tt.run(t, f, fw.Firewall.FirewallArn)
			})
		})
	}
}

func TestAnalysisReports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "report lifecycle", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			pol := f.policy(t, "pol")
			polARN := aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn)
			f.firewall(t, "fw", polARN)

			_, err := f.client.StartAnalysisReport(ctx, &sdk.StartAnalysisReportInput{
				FirewallName: aws.String("fw"), AnalysisType: types.EnabledAnalysisTypeTlsSni,
			})
			assert.Equal(t, "InvalidOperationException", errCode(t, err))

			set, err := f.client.UpdateFirewallAnalysisSettings(ctx, &sdk.UpdateFirewallAnalysisSettingsInput{
				FirewallName:         aws.String("fw"),
				EnabledAnalysisTypes: []types.EnabledAnalysisType{types.EnabledAnalysisTypeTlsSni},
			})
			require.NoError(t, err)
			assert.Equal(t, []types.EnabledAnalysisType{types.EnabledAnalysisTypeTlsSni}, set.EnabledAnalysisTypes)

			d, err := f.client.DescribeFirewall(ctx, &sdk.DescribeFirewallInput{FirewallName: aws.String("fw")})
			require.NoError(t, err)
			assert.Equal(t, set.EnabledAnalysisTypes, d.Firewall.EnabledAnalysisTypes)
			assert.Equal(t, aws.ToString(set.UpdateToken), aws.ToString(d.UpdateToken))

			started, err := f.client.StartAnalysisReport(ctx, &sdk.StartAnalysisReportInput{
				FirewallName: aws.String("fw"), AnalysisType: types.EnabledAnalysisTypeTlsSni,
			})
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				l, lerr := f.client.ListAnalysisReports(
					ctx,
					&sdk.ListAnalysisReportsInput{FirewallName: aws.String("fw")},
				)

				return lerr == nil && len(l.AnalysisReports) == 1 &&
					aws.ToString(l.AnalysisReports[0].Status) == "COMPLETED"
			}, 5e9, 10e6)

			l, err := f.client.ListAnalysisReports(ctx, &sdk.ListAnalysisReportsInput{FirewallName: aws.String("fw")})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(started.AnalysisReportId), aws.ToString(l.AnalysisReports[0].AnalysisReportId))
			assert.Equal(t, types.EnabledAnalysisTypeTlsSni, l.AnalysisReports[0].AnalysisType)
			assert.NotNil(t, l.AnalysisReports[0].ReportTime)

			res, err := f.client.GetAnalysisReportResults(ctx, &sdk.GetAnalysisReportResultsInput{
				FirewallName: aws.String("fw"), AnalysisReportId: started.AnalysisReportId,
			})
			require.NoError(t, err)
			assert.Empty(t, res.AnalysisReportResults)
			assert.Equal(t, types.EnabledAnalysisTypeTlsSni, res.AnalysisType)
			assert.NotNil(t, res.StartTime)

			_, err = f.client.GetAnalysisReportResults(ctx, &sdk.GetAnalysisReportResultsInput{
				FirewallName: aws.String("fw"), AnalysisReportId: aws.String("ghost"),
			})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))

			_, err = f.client.UpdateFirewallAnalysisSettings(ctx, &sdk.UpdateFirewallAnalysisSettingsInput{
				FirewallName: aws.String("fw"), UpdateToken: aws.String("stale"),
			})
			assert.Equal(t, "InvalidTokenException", errCode(t, err))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				tt.run(t, newFixture(t))
			})
		})
	}
}
