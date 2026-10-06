package macie2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"
	"github.com/aws/aws-sdk-go-v2/service/macie2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetFindingStatistics_AppliesSortAndSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sort *types.FindingStatisticsSortCriteria
		size *int32
		want []string
	}{
		{
			name: "default key asc",
			want: []string{
				"Policy:IAMUser/S3BlockPublicAccessDisabled",
				"SensitiveData:S3Object/Credentials",
				"SensitiveData:S3Object/Personal",
			},
		},
		{
			name: "key desc",
			sort: &types.FindingStatisticsSortCriteria{
				AttributeName: types.FindingStatisticsSortAttributeNameGroupKey, OrderBy: types.OrderByDesc,
			},
			want: []string{
				"SensitiveData:S3Object/Personal",
				"SensitiveData:S3Object/Credentials",
				"Policy:IAMUser/S3BlockPublicAccessDisabled",
			},
		},
		{
			name: "count desc then key",
			sort: &types.FindingStatisticsSortCriteria{
				AttributeName: types.FindingStatisticsSortAttributeNameCount, OrderBy: types.OrderByDesc,
			},
			want: []string{
				"SensitiveData:S3Object/Personal",
				"Policy:IAMUser/S3BlockPublicAccessDisabled",
				"SensitiveData:S3Object/Credentials",
			},
		},
		{
			name: "size caps",
			size: aws.Int32(2),
			want: []string{"Policy:IAMUser/S3BlockPublicAccessDisabled", "SensitiveData:S3Object/Credentials"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newMacie2Client(t)
			ctx := t.Context()

			_, err := client.CreateSampleFindings(ctx, &macie2sdk.CreateSampleFindingsInput{
				FindingTypes: []types.FindingType{
					types.FindingTypeSensitiveDataS3ObjectPersonal,
					types.FindingTypeSensitiveDataS3ObjectPersonal,
					types.FindingTypeSensitiveDataS3ObjectCredentials,
					types.FindingTypePolicyIAMUserS3BlockPublicAccessDisabled,
				},
			})
			require.NoError(t, err)

			out, err := client.GetFindingStatistics(ctx, &macie2sdk.GetFindingStatisticsInput{
				GroupBy: types.GroupByType, SortCriteria: tt.sort, Size: tt.size,
			})
			require.NoError(t, err)

			got := make([]string, 0, len(out.CountsByGroup))
			for _, g := range out.CountsByGroup {
				got = append(got, aws.ToString(g.GroupKey))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetFindings_AppliesSortCriteria(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		order types.OrderBy
		want  []string
	}{
		{
			name:  "type asc",
			order: types.OrderByAsc,
			want:  []string{"Policy:IAMUser/S3BlockPublicAccessDisabled", "SensitiveData:S3Object/Personal"},
		},
		{
			name:  "type desc",
			order: types.OrderByDesc,
			want:  []string{"SensitiveData:S3Object/Personal", "Policy:IAMUser/S3BlockPublicAccessDisabled"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newMacie2Client(t)
			ctx := t.Context()

			_, err := client.CreateSampleFindings(ctx, &macie2sdk.CreateSampleFindingsInput{
				FindingTypes: []types.FindingType{
					types.FindingTypeSensitiveDataS3ObjectPersonal,
					types.FindingTypePolicyIAMUserS3BlockPublicAccessDisabled,
				},
			})
			require.NoError(t, err)

			ids, err := client.ListFindings(ctx, &macie2sdk.ListFindingsInput{})
			require.NoError(t, err)

			out, err := client.GetFindings(ctx, &macie2sdk.GetFindingsInput{
				FindingIds:   ids.FindingIds,
				SortCriteria: &types.SortCriteria{AttributeName: aws.String("type"), OrderBy: tt.order},
			})
			require.NoError(t, err)

			got := make([]string, 0, len(out.Findings))
			for _, f := range out.Findings {
				got = append(got, string(f.Type))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRevealConfiguration_RetrievalConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		update   *types.UpdateRetrievalConfiguration
		wantMode types.RetrievalMode
		wantRole string
		wantErr  bool
	}{
		{name: "default", wantMode: types.RetrievalModeCallerCredentials},
		{
			name: "assume role",
			update: &types.UpdateRetrievalConfiguration{
				RetrievalMode: types.RetrievalModeAssumeRole,
				RoleName:      aws.String("MacieReveal"),
			},
			wantMode: types.RetrievalModeAssumeRole, wantRole: "MacieReveal",
		},
		{
			name: "caller credentials clears role",
			update: &types.UpdateRetrievalConfiguration{
				RetrievalMode: types.RetrievalModeCallerCredentials,
				RoleName:      aws.String("ignored"),
			},
			wantMode: types.RetrievalModeCallerCredentials,
		},
		{
			name:    "assume role needs a role name",
			update:  &types.UpdateRetrievalConfiguration{RetrievalMode: types.RetrievalModeAssumeRole},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newMacie2Client(t)
			ctx := t.Context()

			if tt.update != nil {
				out, err := client.UpdateRevealConfiguration(ctx, &macie2sdk.UpdateRevealConfigurationInput{
					Configuration:          &types.RevealConfiguration{Status: types.RevealStatusEnabled},
					RetrievalConfiguration: tt.update,
				})
				if tt.wantErr {
					require.Error(t, err)

					return
				}

				require.NoError(t, err)
				require.NotNil(t, out.RetrievalConfiguration)
				assert.Equal(t, tt.wantMode, out.RetrievalConfiguration.RetrievalMode)
			}

			got, err := client.GetRevealConfiguration(ctx, &macie2sdk.GetRevealConfigurationInput{})
			require.NoError(t, err)
			require.NotNil(t, got.RetrievalConfiguration)
			assert.Equal(t, tt.wantMode, got.RetrievalConfiguration.RetrievalMode)
			assert.Equal(t, tt.wantRole, aws.ToString(got.RetrievalConfiguration.RoleName))

			if tt.wantMode == types.RetrievalModeAssumeRole {
				assert.NotEmpty(t, aws.ToString(got.RetrievalConfiguration.ExternalId))
			} else {
				assert.Nil(t, got.RetrievalConfiguration.ExternalId)
			}
		})
	}
}

func TestAcceptInvitation_MasterAccountAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input *macie2sdk.AcceptInvitationInput
		want  string
	}{
		{
			name: "administrator",
			input: &macie2sdk.AcceptInvitationInput{
				AdministratorAccountId: aws.String("111111111111"),
				InvitationId:           aws.String("inv-1"),
			},
			want: "111111111111",
		},
		{
			name: "legacy master account",
			input: &macie2sdk.AcceptInvitationInput{
				MasterAccount: aws.String("222222222222"),
				InvitationId:  aws.String("inv-2"),
			},
			want: "222222222222",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newMacie2Client(t)
			ctx := t.Context()

			_, err := client.AcceptInvitation(ctx, tt.input)
			require.NoError(t, err)

			got, err := client.GetAdministratorAccount(ctx, &macie2sdk.GetAdministratorAccountInput{})
			require.NoError(t, err)
			require.NotNil(t, got.Administrator)
			assert.Equal(t, tt.want, aws.ToString(got.Administrator.AccountId))
		})
	}
}

func TestListAutomatedDiscoveryAccounts_AccountIdsFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ids  []string
		want []string
	}{
		{name: "all", want: []string{"111111111111", "222222222222"}},
		{name: "one", ids: []string{"222222222222"}, want: []string{"222222222222"}},
		{name: "unknown", ids: []string{"333333333333"}, want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newMacie2Client(t)
			ctx := t.Context()

			_, err := client.BatchUpdateAutomatedDiscoveryAccounts(
				ctx,
				&macie2sdk.BatchUpdateAutomatedDiscoveryAccountsInput{
					Accounts: []types.AutomatedDiscoveryAccountUpdate{
						{AccountId: aws.String("111111111111"), Status: types.AutomatedDiscoveryAccountStatusEnabled},
						{AccountId: aws.String("222222222222"), Status: types.AutomatedDiscoveryAccountStatusEnabled},
					},
				},
			)
			require.NoError(t, err)

			out, err := client.ListAutomatedDiscoveryAccounts(
				ctx,
				&macie2sdk.ListAutomatedDiscoveryAccountsInput{AccountIds: tt.ids},
			)
			require.NoError(t, err)

			got := make([]string, 0, len(out.Items))
			for _, a := range out.Items {
				got = append(got, aws.ToString(a.AccountId))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
