package inspector2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const memberAccount = "222222222222"

func TestEnableDisable_AccountIDs(t *testing.T) {
	t.Parallel()

	_, client := newRealClient(t)
	ctx := t.Context()

	_, err := client.AssociateMember(ctx, &inspector2sdk.AssociateMemberInput{AccountId: aws.String(memberAccount)})
	require.NoError(t, err)

	out, err := client.Enable(ctx, &inspector2sdk.EnableInput{
		AccountIds:    []string{memberAccount, "333333333333"},
		ResourceTypes: []types.ResourceScanType{types.ResourceScanTypeEcr},
	})
	require.NoError(t, err)
	require.Len(t, out.Accounts, 1)
	assert.Equal(t, memberAccount, aws.ToString(out.Accounts[0].AccountId))
	assert.Equal(t, types.StatusEnabled, out.Accounts[0].ResourceStatus.Ecr)
	require.Len(t, out.FailedAccounts, 1)
	assert.Equal(t, "333333333333", aws.ToString(out.FailedAccounts[0].AccountId))
	assert.Equal(t, types.ErrorCodeResourceNotFound, out.FailedAccounts[0].ErrorCode)

	status, err := client.BatchGetAccountStatus(ctx, &inspector2sdk.BatchGetAccountStatusInput{
		AccountIds: []string{memberAccount, rtTestAccountID},
	})
	require.NoError(t, err)
	require.Len(t, status.Accounts, 2)

	byID := map[string]types.AccountState{}
	for _, a := range status.Accounts {
		byID[aws.ToString(a.AccountId)] = a
	}

	assert.Equal(t, types.StatusEnabled, byID[memberAccount].ResourceState.Ecr.Status)
	assert.Equal(t, types.StatusDisabled, byID[rtTestAccountID].ResourceState.Ecr.Status, "own account untouched")

	_, err = client.Disable(ctx, &inspector2sdk.DisableInput{
		AccountIds:    []string{memberAccount},
		ResourceTypes: []types.ResourceScanType{types.ResourceScanTypeEcr},
	})
	require.NoError(t, err)

	status, err = client.BatchGetAccountStatus(ctx, &inspector2sdk.BatchGetAccountStatusInput{
		AccountIds: []string{memberAccount, "444444444444"},
	})
	require.NoError(t, err)
	require.Len(t, status.Accounts, 1)
	assert.Equal(t, types.StatusDisabled, status.Accounts[0].ResourceState.Ecr.Status)
	require.Len(t, status.FailedAccounts, 1)
	assert.Equal(t, "444444444444", aws.ToString(status.FailedAccounts[0].AccountId))
}

func TestEnable_ClientTokenReplay(t *testing.T) {
	t.Parallel()

	_, client := newRealClient(t)
	ctx := t.Context()
	in := &inspector2sdk.EnableInput{
		ClientToken:   aws.String("tok-1"),
		ResourceTypes: []types.ResourceScanType{types.ResourceScanTypeEc2},
	}

	_, err := client.Enable(ctx, in)
	require.NoError(t, err)

	_, err = client.Disable(ctx, &inspector2sdk.DisableInput{
		ResourceTypes: []types.ResourceScanType{types.ResourceScanTypeEc2},
	})
	require.NoError(t, err)

	out, err := client.Enable(ctx, in)
	require.NoError(t, err)
	require.Len(t, out.Accounts, 1)

	status, err := client.BatchGetAccountStatus(ctx, &inspector2sdk.BatchGetAccountStatusInput{})
	require.NoError(t, err)
	assert.Equal(t, types.StatusDisabled, status.Accounts[0].ResourceState.Ec2.Status,
		"a replayed token must not re-apply the enable")

	in.ClientToken = aws.String("tok-2")
	_, err = client.Enable(ctx, in)
	require.NoError(t, err)

	status, err = client.BatchGetAccountStatus(ctx, &inspector2sdk.BatchGetAccountStatusInput{})
	require.NoError(t, err)
	assert.Equal(t, types.StatusEnabled, status.Accounts[0].ResourceState.Ec2.Status)

	in.ResourceTypes = []types.ResourceScanType{types.ResourceScanTypeEcr}
	_, err = client.Enable(ctx, in)
	require.Error(t, err, "tok-2 reused with different parameters")
}

func TestGetCisScanReport_ReportFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		format  types.CisReportFormat
		wantErr bool
	}{
		{name: "pdf", format: types.CisReportFormatPdf},
		{name: "csv", format: types.CisReportFormatCsv},
		{name: "unset"},
		{name: "bogus", format: types.CisReportFormat("XML"), wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newRealClient(t)

			_, err := client.GetCisScanReport(t.Context(), &inspector2sdk.GetCisScanReportInput{
				ScanArn:      aws.String("arn:aws:inspector2:us-east-1:123456789012:cis-scan/x"),
				ReportFormat: tc.format,
			})
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
