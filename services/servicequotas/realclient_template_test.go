package servicequotas_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqsdk "github.com/aws/aws-sdk-go-v2/service/servicequotas"
	sqtypes "github.com/aws/aws-sdk-go-v2/service/servicequotas/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/servicequotas"
)

func orgFixture(t *testing.T, info servicequotas.OrgInfo) *fixture {
	t.Helper()

	f := newFixture(t)
	f.backend.SetOrgDirectory(fakeOrgs{awsmeta.DefaultAccount: info})

	return f
}

func goodOrg() servicequotas.OrgInfo {
	return servicequotas.OrgInfo{ManagementAccountID: awsmeta.DefaultAccount, AllFeatures: true, ServiceAccess: true}
}

func putItem(t *testing.T, f *fixture, desired float64) {
	t.Helper()

	_, err := f.client.PutServiceQuotaIncreaseRequestIntoTemplate(t.Context(),
		&sqsdk.PutServiceQuotaIncreaseRequestIntoTemplateInput{
			AwsRegion: aws.String("us-west-2"), ServiceCode: aws.String("ec2"),
			QuotaCode: aws.String("L-1216C47A"), DesiredValue: aws.Float64(desired),
		})
	require.NoError(t, err)
}

func TestTemplate_OrganizationGate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		org  *servicequotas.OrgInfo
		name string
		code string
	}{
		{name: "no_directory", org: nil, code: "NoAvailableOrganizationException"},
		{
			name: "not_all_features",
			org:  &servicequotas.OrgInfo{ManagementAccountID: awsmeta.DefaultAccount, ServiceAccess: true},
			code: "OrganizationNotInAllFeaturesModeException",
		},
		{
			name: "service_access_off",
			org:  &servicequotas.OrgInfo{ManagementAccountID: awsmeta.DefaultAccount, AllFeatures: true},
			code: "AWSServiceAccessNotEnabledException",
		},
		{
			name: "member_account",
			org:  &servicequotas.OrgInfo{ManagementAccountID: "999999999999", AllFeatures: true, ServiceAccess: true},
			code: "AccessDeniedException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			if tt.org != nil {
				f.backend.SetOrgDirectory(fakeOrgs{awsmeta.DefaultAccount: *tt.org})
			}

			_, err := f.client.AssociateServiceQuotaTemplate(t.Context(), &sqsdk.AssociateServiceQuotaTemplateInput{})
			assert.Equal(t, tt.code, apiCode(err))

			_, err = f.client.GetAssociationForServiceQuotaTemplate(
				t.Context(),
				&sqsdk.GetAssociationForServiceQuotaTemplateInput{},
			)
			assert.Equal(t, tt.code, apiCode(err))

			_, err = f.client.ListServiceQuotaIncreaseRequestsInTemplate(t.Context(),
				&sqsdk.ListServiceQuotaIncreaseRequestsInTemplateInput{})
			assert.Equal(t, tt.code, apiCode(err))
		})
	}
}

func TestTemplate_Entries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{
			name: "put_get_list_delete",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				putItem(t, f, 20)
				putItem(t, f, 40)

				got, err := f.client.GetServiceQuotaIncreaseRequestFromTemplate(t.Context(),
					&sqsdk.GetServiceQuotaIncreaseRequestFromTemplateInput{
						AwsRegion: aws.String(
							"us-west-2",
						),
						ServiceCode: aws.String("ec2"),
						QuotaCode:   aws.String("L-1216C47A"),
					})
				require.NoError(t, err)

				item := got.ServiceQuotaIncreaseRequestInTemplate
				assert.InDelta(t, 40, aws.ToFloat64(item.DesiredValue), 0)
				assert.Equal(t, "us-west-2", aws.ToString(item.AwsRegion))
				assert.Contains(t, aws.ToString(item.QuotaName), "On-Demand Standard")

				list, err := f.client.ListServiceQuotaIncreaseRequestsInTemplate(t.Context(),
					&sqsdk.ListServiceQuotaIncreaseRequestsInTemplateInput{ServiceCode: aws.String("ec2")})
				require.NoError(t, err)
				assert.Len(t, list.ServiceQuotaIncreaseRequestInTemplateList, 1)

				other, err := f.client.ListServiceQuotaIncreaseRequestsInTemplate(t.Context(),
					&sqsdk.ListServiceQuotaIncreaseRequestsInTemplateInput{AwsRegion: aws.String("eu-west-1")})
				require.NoError(t, err)
				assert.Empty(t, other.ServiceQuotaIncreaseRequestInTemplateList)

				_, err = f.client.DeleteServiceQuotaIncreaseRequestFromTemplate(t.Context(),
					&sqsdk.DeleteServiceQuotaIncreaseRequestFromTemplateInput{
						AwsRegion: aws.String(
							"us-west-2",
						),
						ServiceCode: aws.String("ec2"),
						QuotaCode:   aws.String("L-1216C47A"),
					})
				require.NoError(t, err)

				_, err = f.client.GetServiceQuotaIncreaseRequestFromTemplate(t.Context(),
					&sqsdk.GetServiceQuotaIncreaseRequestFromTemplateInput{
						AwsRegion: aws.String(
							"us-west-2",
						),
						ServiceCode: aws.String("ec2"),
						QuotaCode:   aws.String("L-1216C47A"),
					})
				assert.Equal(t, "NoSuchResourceException", apiCode(err))
			},
		},
		{
			name: "desired_must_exceed_default",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.client.PutServiceQuotaIncreaseRequestIntoTemplate(t.Context(),
					&sqsdk.PutServiceQuotaIncreaseRequestIntoTemplateInput{
						AwsRegion: aws.String("us-west-2"), ServiceCode: aws.String("ec2"),
						QuotaCode: aws.String("L-1216C47A"), DesiredValue: aws.Float64(5),
					})
				assert.Equal(t, "IllegalArgumentException", apiCode(err))
			},
		},
		{
			name: "invalid_region",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.client.PutServiceQuotaIncreaseRequestIntoTemplate(t.Context(),
					&sqsdk.PutServiceQuotaIncreaseRequestIntoTemplateInput{
						AwsRegion: aws.String("mars"), ServiceCode: aws.String("ec2"),
						QuotaCode: aws.String("L-1216C47A"), DesiredValue: aws.Float64(50),
					})
				assert.Equal(t, "IllegalArgumentException", apiCode(err))
			},
		},
		{
			name: "delete_missing",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.client.DeleteServiceQuotaIncreaseRequestFromTemplate(t.Context(),
					&sqsdk.DeleteServiceQuotaIncreaseRequestFromTemplateInput{
						AwsRegion: aws.String(
							"us-west-2",
						),
						ServiceCode: aws.String("ec2"),
						QuotaCode:   aws.String("L-1216C47A"),
					})
				assert.Equal(t, "NoSuchResourceException", apiCode(err))
			},
		},
		{
			name: "association_lifecycle",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				status := func() sqtypes.ServiceQuotaTemplateAssociationStatus {
					out, err := f.client.GetAssociationForServiceQuotaTemplate(t.Context(),
						&sqsdk.GetAssociationForServiceQuotaTemplateInput{})
					require.NoError(t, err)

					return out.ServiceQuotaTemplateAssociationStatus
				}

				assert.Equal(t, sqtypes.ServiceQuotaTemplateAssociationStatusDisassociated, status())

				_, err := f.client.DisassociateServiceQuotaTemplate(
					t.Context(),
					&sqsdk.DisassociateServiceQuotaTemplateInput{},
				)
				assert.Equal(t, "ServiceQuotaTemplateNotInUseException", apiCode(err))

				_, err = f.client.AssociateServiceQuotaTemplate(
					t.Context(),
					&sqsdk.AssociateServiceQuotaTemplateInput{},
				)
				require.NoError(t, err)
				assert.Equal(t, sqtypes.ServiceQuotaTemplateAssociationStatusAssociated, status())

				_, err = f.client.DisassociateServiceQuotaTemplate(
					t.Context(),
					&sqsdk.DisassociateServiceQuotaTemplateInput{},
				)
				require.NoError(t, err)
				assert.Equal(t, sqtypes.ServiceQuotaTemplateAssociationStatusDisassociated, status())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, orgFixture(t, goodOrg()))
		})
	}
}
