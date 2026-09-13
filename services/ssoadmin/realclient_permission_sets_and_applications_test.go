package ssoadmin_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssoadminsdk "github.com/aws/aws-sdk-go-v2/service/ssoadmin"
	"github.com/aws/aws-sdk-go-v2/service/ssoadmin/document"
	ssoadmintypes "github.com/aws/aws-sdk-go-v2/service/ssoadmin/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

// newRealClient is a top-level (not closure-local) client
// constructor: cmd/clientcoverage's census only traces a var's SDK-module
// binding through a named top-level function with a declared *pkg.Client
// return type, not a local func-literal variable, so keeping this as a
// top-level func is load-bearing for accurate typed-coverage measurement.
func newRealClient(t *testing.T) *ssoadminsdk.Client {
	t.Helper()

	backend := ssoadmin.NewInMemoryBackend("000000000000", tagsRTRegion)

	return newTestSSOAdminClient(t, ssoadmin.NewHandler(backend))
}

// TestRealClient_PermissionSetsAndApplications drives ssoadmin's typed-
// coverage-blind ops (gopherstack-n3zi) through the real aws-sdk-go-v2
// ssoadmin client, one case per named priority family, asserting decoded
// values.
func TestRealClient_PermissionSetsAndApplications(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "permission set provisioning",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				inst, err := client.CreateInstance(
					t.Context(),
					&ssoadminsdk.CreateInstanceInput{Name: aws.String("s9-ps-inst")},
				)
				require.NoError(t, err)

				ps, err := client.CreatePermissionSet(t.Context(), &ssoadminsdk.CreatePermissionSetInput{
					InstanceArn: inst.InstanceArn,
					Name:        aws.String("s9-permission-set"),
				})
				require.NoError(t, err)
				psArn := ps.PermissionSet.PermissionSetArn

				descOut, err := client.DescribePermissionSet(
					t.Context(),
					&ssoadminsdk.DescribePermissionSetInput{
						InstanceArn:      inst.InstanceArn,
						PermissionSetArn: psArn,
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "s9-permission-set", aws.ToString(descOut.PermissionSet.Name))

				provOut, err := client.ProvisionPermissionSet(
					t.Context(),
					&ssoadminsdk.ProvisionPermissionSetInput{
						InstanceArn:      inst.InstanceArn,
						PermissionSetArn: psArn,
						TargetType:       ssoadmintypes.ProvisionTargetTypeAllProvisionedAccounts,
					},
				)
				require.NoError(t, err)
				requestID := provOut.PermissionSetProvisioningStatus.RequestId
				assert.Equal(t, psArn, provOut.PermissionSetProvisioningStatus.PermissionSetArn)

				statusOut, err := client.DescribePermissionSetProvisioningStatus(
					t.Context(),
					&ssoadminsdk.DescribePermissionSetProvisioningStatusInput{
						InstanceArn:                     inst.InstanceArn,
						ProvisionPermissionSetRequestId: requestID,
					},
				)
				require.NoError(t, err)
				assert.Equal(
					t,
					aws.ToString(requestID),
					aws.ToString(statusOut.PermissionSetProvisioningStatus.RequestId),
				)

				listStatusOut, err := client.ListPermissionSetProvisioningStatus(
					t.Context(),
					&ssoadminsdk.ListPermissionSetProvisioningStatusInput{InstanceArn: inst.InstanceArn},
				)
				require.NoError(t, err)
				assert.NotEmpty(t, listStatusOut.PermissionSetsProvisioningStatus)

				provisionedToOut, err := client.ListPermissionSetsProvisionedToAccount(
					t.Context(),
					&ssoadminsdk.ListPermissionSetsProvisionedToAccountInput{
						InstanceArn: inst.InstanceArn,
						AccountId:   aws.String("111111111111"),
					},
				)
				require.NoError(t, err)
				assert.NotNil(t, provisionedToOut.PermissionSets)

				accountsForOut, err := client.ListAccountsForProvisionedPermissionSet(
					t.Context(),
					&ssoadminsdk.ListAccountsForProvisionedPermissionSetInput{
						InstanceArn:      inst.InstanceArn,
						PermissionSetArn: psArn,
					},
				)
				require.NoError(t, err)
				assert.NotNil(t, accountsForOut.AccountIds)
			},
		},
		{
			name: "permission set policies",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				inst, err := client.CreateInstance(
					t.Context(),
					&ssoadminsdk.CreateInstanceInput{Name: aws.String("s9-pol-inst")},
				)
				require.NoError(t, err)

				ps, err := client.CreatePermissionSet(t.Context(), &ssoadminsdk.CreatePermissionSetInput{
					InstanceArn: inst.InstanceArn,
					Name:        aws.String("s9-pol-permission-set"),
				})
				require.NoError(t, err)
				psArn := ps.PermissionSet.PermissionSetArn

				_, err = client.AttachManagedPolicyToPermissionSet(
					t.Context(),
					&ssoadminsdk.AttachManagedPolicyToPermissionSetInput{
						InstanceArn:      inst.InstanceArn,
						PermissionSetArn: psArn,
						ManagedPolicyArn: aws.String("arn:aws:iam::aws:policy/ReadOnlyAccess"),
					},
				)
				require.NoError(t, err)

				listManaged, err := client.ListManagedPoliciesInPermissionSet(
					t.Context(),
					&ssoadminsdk.ListManagedPoliciesInPermissionSetInput{
						InstanceArn:      inst.InstanceArn,
						PermissionSetArn: psArn,
					},
				)
				require.NoError(t, err)
				require.Len(t, listManaged.AttachedManagedPolicies, 1)
				assert.Equal(t, "ReadOnlyAccess", aws.ToString(listManaged.AttachedManagedPolicies[0].Name))

				_, err = client.DetachManagedPolicyFromPermissionSet(
					t.Context(),
					&ssoadminsdk.DetachManagedPolicyFromPermissionSetInput{
						InstanceArn:      inst.InstanceArn,
						PermissionSetArn: psArn,
						ManagedPolicyArn: aws.String("arn:aws:iam::aws:policy/ReadOnlyAccess"),
					},
				)
				require.NoError(t, err)

				_, err = client.AttachCustomerManagedPolicyReferenceToPermissionSet(
					t.Context(),
					&ssoadminsdk.AttachCustomerManagedPolicyReferenceToPermissionSetInput{
						InstanceArn:      inst.InstanceArn,
						PermissionSetArn: psArn,
						CustomerManagedPolicyReference: &ssoadmintypes.CustomerManagedPolicyReference{
							Name: aws.String("my-cmp"),
							Path: aws.String("/"),
						},
					},
				)
				require.NoError(t, err)

				listCMP, err := client.ListCustomerManagedPolicyReferencesInPermissionSet(
					t.Context(),
					&ssoadminsdk.ListCustomerManagedPolicyReferencesInPermissionSetInput{
						InstanceArn:      inst.InstanceArn,
						PermissionSetArn: psArn,
					},
				)
				require.NoError(t, err)
				require.Len(t, listCMP.CustomerManagedPolicyReferences, 1)
				assert.Equal(t, "my-cmp", aws.ToString(listCMP.CustomerManagedPolicyReferences[0].Name))

				_, err = client.DetachCustomerManagedPolicyReferenceFromPermissionSet(
					t.Context(),
					&ssoadminsdk.DetachCustomerManagedPolicyReferenceFromPermissionSetInput{
						InstanceArn:      inst.InstanceArn,
						PermissionSetArn: psArn,
						CustomerManagedPolicyReference: &ssoadmintypes.CustomerManagedPolicyReference{
							Name: aws.String("my-cmp"),
							Path: aws.String("/"),
						},
					},
				)
				require.NoError(t, err)

				_, err = client.PutInlinePolicyToPermissionSet(
					t.Context(),
					&ssoadminsdk.PutInlinePolicyToPermissionSetInput{
						InstanceArn:      inst.InstanceArn,
						PermissionSetArn: psArn,
						InlinePolicy:     aws.String(`{"Version":"2012-10-17","Statement":[]}`),
					},
				)
				require.NoError(t, err)

				inlineOut, err := client.GetInlinePolicyForPermissionSet(
					t.Context(),
					&ssoadminsdk.GetInlinePolicyForPermissionSetInput{
						InstanceArn:      inst.InstanceArn,
						PermissionSetArn: psArn,
					},
				)
				require.NoError(t, err)
				assert.JSONEq(
					t,
					`{"Version":"2012-10-17","Statement":[]}`,
					aws.ToString(inlineOut.InlinePolicy),
				)

				_, err = client.DeleteInlinePolicyFromPermissionSet(
					t.Context(),
					&ssoadminsdk.DeleteInlinePolicyFromPermissionSetInput{
						InstanceArn:      inst.InstanceArn,
						PermissionSetArn: psArn,
					},
				)
				require.NoError(t, err)

				_, err = client.PutPermissionsBoundaryToPermissionSet(
					t.Context(),
					&ssoadminsdk.PutPermissionsBoundaryToPermissionSetInput{
						InstanceArn:      inst.InstanceArn,
						PermissionSetArn: psArn,
						PermissionsBoundary: &ssoadmintypes.PermissionsBoundary{
							ManagedPolicyArn: aws.String("arn:aws:iam::aws:policy/ReadOnlyAccess"),
						},
					},
				)
				require.NoError(t, err)

				boundaryOut, err := client.GetPermissionsBoundaryForPermissionSet(
					t.Context(),
					&ssoadminsdk.GetPermissionsBoundaryForPermissionSetInput{
						InstanceArn:      inst.InstanceArn,
						PermissionSetArn: psArn,
					},
				)
				require.NoError(t, err)
				assert.Equal(
					t,
					"arn:aws:iam::aws:policy/ReadOnlyAccess",
					aws.ToString(boundaryOut.PermissionsBoundary.ManagedPolicyArn),
				)

				_, err = client.DeletePermissionsBoundaryFromPermissionSet(
					t.Context(),
					&ssoadminsdk.DeletePermissionsBoundaryFromPermissionSetInput{
						InstanceArn:      inst.InstanceArn,
						PermissionSetArn: psArn,
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "account assignments",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				inst, err := client.CreateInstance(
					t.Context(),
					&ssoadminsdk.CreateInstanceInput{Name: aws.String("s9-aa-inst")},
				)
				require.NoError(t, err)

				ps, err := client.CreatePermissionSet(t.Context(), &ssoadminsdk.CreatePermissionSetInput{
					InstanceArn: inst.InstanceArn,
					Name:        aws.String("s9-aa-permission-set"),
				})
				require.NoError(t, err)
				psArn := ps.PermissionSet.PermissionSetArn

				createOut, err := client.CreateAccountAssignment(
					t.Context(),
					&ssoadminsdk.CreateAccountAssignmentInput{
						InstanceArn:      inst.InstanceArn,
						PermissionSetArn: psArn,
						TargetId:         aws.String("222222222222"),
						TargetType:       ssoadmintypes.TargetTypeAwsAccount,
						PrincipalId:      aws.String("11111111-1111-1111-1111-111111111111"),
						PrincipalType:    ssoadmintypes.PrincipalTypeUser,
					},
				)
				require.NoError(t, err)
				createRequestID := createOut.AccountAssignmentCreationStatus.RequestId

				createStatusOut, err := client.DescribeAccountAssignmentCreationStatus(
					t.Context(),
					&ssoadminsdk.DescribeAccountAssignmentCreationStatusInput{
						InstanceArn:                        inst.InstanceArn,
						AccountAssignmentCreationRequestId: createRequestID,
					},
				)
				require.NoError(t, err)
				assert.Equal(
					t,
					aws.ToString(createRequestID),
					aws.ToString(createStatusOut.AccountAssignmentCreationStatus.RequestId),
				)

				listCreateStatus, err := client.ListAccountAssignmentCreationStatus(
					t.Context(),
					&ssoadminsdk.ListAccountAssignmentCreationStatusInput{InstanceArn: inst.InstanceArn},
				)
				require.NoError(t, err)
				assert.NotEmpty(t, listCreateStatus.AccountAssignmentsCreationStatus)

				listOut, err := client.ListAccountAssignments(
					t.Context(),
					&ssoadminsdk.ListAccountAssignmentsInput{
						InstanceArn:      inst.InstanceArn,
						AccountId:        aws.String("222222222222"),
						PermissionSetArn: psArn,
					},
				)
				require.NoError(t, err)
				require.Len(t, listOut.AccountAssignments, 1)
				assert.Equal(
					t,
					"11111111-1111-1111-1111-111111111111",
					aws.ToString(listOut.AccountAssignments[0].PrincipalId),
				)

				listForPrincipal, err := client.ListAccountAssignmentsForPrincipal(
					t.Context(),
					&ssoadminsdk.ListAccountAssignmentsForPrincipalInput{
						InstanceArn:   inst.InstanceArn,
						PrincipalId:   aws.String("11111111-1111-1111-1111-111111111111"),
						PrincipalType: ssoadmintypes.PrincipalTypeUser,
					},
				)
				require.NoError(t, err)
				require.Len(t, listForPrincipal.AccountAssignments, 1)
				assert.Equal(
					t,
					"222222222222",
					aws.ToString(listForPrincipal.AccountAssignments[0].AccountId),
				)

				deleteOut, err := client.DeleteAccountAssignment(
					t.Context(),
					&ssoadminsdk.DeleteAccountAssignmentInput{
						InstanceArn:      inst.InstanceArn,
						PermissionSetArn: psArn,
						TargetId:         aws.String("222222222222"),
						TargetType:       ssoadmintypes.TargetTypeAwsAccount,
						PrincipalId:      aws.String("11111111-1111-1111-1111-111111111111"),
						PrincipalType:    ssoadmintypes.PrincipalTypeUser,
					},
				)
				require.NoError(t, err)
				deleteRequestID := deleteOut.AccountAssignmentDeletionStatus.RequestId

				deleteStatusOut, err := client.DescribeAccountAssignmentDeletionStatus(
					t.Context(),
					&ssoadminsdk.DescribeAccountAssignmentDeletionStatusInput{
						InstanceArn:                        inst.InstanceArn,
						AccountAssignmentDeletionRequestId: deleteRequestID,
					},
				)
				require.NoError(t, err)
				assert.Equal(
					t,
					aws.ToString(deleteRequestID),
					aws.ToString(deleteStatusOut.AccountAssignmentDeletionStatus.RequestId),
				)

				listDeleteStatus, err := client.ListAccountAssignmentDeletionStatus(
					t.Context(),
					&ssoadminsdk.ListAccountAssignmentDeletionStatusInput{InstanceArn: inst.InstanceArn},
				)
				require.NoError(t, err)
				assert.NotEmpty(t, listDeleteStatus.AccountAssignmentsDeletionStatus)
			},
		},
		{
			name: "applications",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				inst, err := client.CreateInstance(
					t.Context(),
					&ssoadminsdk.CreateInstanceInput{Name: aws.String("s9-app-inst")},
				)
				require.NoError(t, err)

				appOut, err := client.CreateApplication(t.Context(), &ssoadminsdk.CreateApplicationInput{
					InstanceArn:            inst.InstanceArn,
					ApplicationProviderArn: aws.String("arn:aws:sso::aws:applicationProvider/custom"),
					Name:                   aws.String("s9-application"),
				})
				require.NoError(t, err)
				appArn := appOut.ApplicationArn

				descOut, err := client.DescribeApplication(
					t.Context(),
					&ssoadminsdk.DescribeApplicationInput{
						ApplicationArn: appArn,
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "s9-application", aws.ToString(descOut.Name))

				_, err = client.UpdateApplication(t.Context(), &ssoadminsdk.UpdateApplicationInput{
					ApplicationArn: appArn,
					Name:           aws.String("s9-application-renamed"),
				})
				require.NoError(t, err)

				descAfter, err := client.DescribeApplication(
					t.Context(),
					&ssoadminsdk.DescribeApplicationInput{
						ApplicationArn: appArn,
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "s9-application-renamed", aws.ToString(descAfter.Name))

				listOut, err := client.ListApplications(t.Context(), &ssoadminsdk.ListApplicationsInput{
					InstanceArn: inst.InstanceArn,
				})
				require.NoError(t, err)
				require.Len(t, listOut.Applications, 1)
				assert.Equal(t, aws.ToString(appArn), aws.ToString(listOut.Applications[0].ApplicationArn))

				providerOut, err := client.DescribeApplicationProvider(
					t.Context(),
					&ssoadminsdk.DescribeApplicationProviderInput{
						ApplicationProviderArn: aws.String("arn:aws:sso::aws:applicationProvider/custom"),
					},
				)
				require.NoError(t, err)
				assert.Equal(
					t,
					"arn:aws:sso::aws:applicationProvider/custom",
					aws.ToString(providerOut.ApplicationProviderArn),
				)

				providersOut, err := client.ListApplicationProviders(
					t.Context(),
					&ssoadminsdk.ListApplicationProvidersInput{},
				)
				require.NoError(t, err)
				assert.NotEmpty(t, providersOut.ApplicationProviders)

				_, err = client.DeleteApplication(
					t.Context(),
					&ssoadminsdk.DeleteApplicationInput{ApplicationArn: appArn},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "application assignments",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				inst, err := client.CreateInstance(
					t.Context(),
					&ssoadminsdk.CreateInstanceInput{Name: aws.String("s9-aas-inst")},
				)
				require.NoError(t, err)

				appOut, err := client.CreateApplication(t.Context(), &ssoadminsdk.CreateApplicationInput{
					InstanceArn:            inst.InstanceArn,
					ApplicationProviderArn: aws.String("arn:aws:sso::aws:applicationProvider/custom"),
					Name:                   aws.String("s9-aas-application"),
				})
				require.NoError(t, err)
				appArn := appOut.ApplicationArn

				_, err = client.CreateApplicationAssignment(
					t.Context(),
					&ssoadminsdk.CreateApplicationAssignmentInput{
						ApplicationArn: appArn,
						PrincipalId:    aws.String("22222222-2222-2222-2222-222222222222"),
						PrincipalType:  ssoadmintypes.PrincipalTypeUser,
					},
				)
				require.NoError(t, err)

				descOut, err := client.DescribeApplicationAssignment(
					t.Context(),
					&ssoadminsdk.DescribeApplicationAssignmentInput{
						ApplicationArn: appArn,
						PrincipalId:    aws.String("22222222-2222-2222-2222-222222222222"),
						PrincipalType:  ssoadmintypes.PrincipalTypeUser,
					},
				)
				require.NoError(t, err)
				assert.Equal(t, appArn, descOut.ApplicationArn)

				listOut, err := client.ListApplicationAssignments(
					t.Context(),
					&ssoadminsdk.ListApplicationAssignmentsInput{
						ApplicationArn: appArn,
					},
				)
				require.NoError(t, err)
				require.Len(t, listOut.ApplicationAssignments, 1)

				listForPrincipal, err := client.ListApplicationAssignmentsForPrincipal(
					t.Context(),
					&ssoadminsdk.ListApplicationAssignmentsForPrincipalInput{
						InstanceArn:   inst.InstanceArn,
						PrincipalId:   aws.String("22222222-2222-2222-2222-222222222222"),
						PrincipalType: ssoadmintypes.PrincipalTypeUser,
					},
				)
				require.NoError(t, err)
				require.Len(t, listForPrincipal.ApplicationAssignments, 1)

				_, err = client.DeleteApplicationAssignment(
					t.Context(),
					&ssoadminsdk.DeleteApplicationAssignmentInput{
						ApplicationArn: appArn,
						PrincipalId:    aws.String("22222222-2222-2222-2222-222222222222"),
						PrincipalType:  ssoadmintypes.PrincipalTypeUser,
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "application access scopes",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				inst, err := client.CreateInstance(
					t.Context(),
					&ssoadminsdk.CreateInstanceInput{Name: aws.String("s9-scope-inst")},
				)
				require.NoError(t, err)

				appOut, err := client.CreateApplication(t.Context(), &ssoadminsdk.CreateApplicationInput{
					InstanceArn:            inst.InstanceArn,
					ApplicationProviderArn: aws.String("arn:aws:sso::aws:applicationProvider/custom"),
					Name:                   aws.String("s9-scope-application"),
				})
				require.NoError(t, err)
				appArn := appOut.ApplicationArn

				_, err = client.PutApplicationAccessScope(
					t.Context(),
					&ssoadminsdk.PutApplicationAccessScopeInput{
						ApplicationArn:    appArn,
						Scope:             aws.String("sso:account:access"),
						AuthorizedTargets: []string{"arn:aws:sso:::account/222222222222"},
					},
				)
				require.NoError(t, err)

				getOut, err := client.GetApplicationAccessScope(
					t.Context(),
					&ssoadminsdk.GetApplicationAccessScopeInput{
						ApplicationArn: appArn,
						Scope:          aws.String("sso:account:access"),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, []string{"arn:aws:sso:::account/222222222222"}, getOut.AuthorizedTargets)

				listOut, err := client.ListApplicationAccessScopes(
					t.Context(),
					&ssoadminsdk.ListApplicationAccessScopesInput{
						ApplicationArn: appArn,
					},
				)
				require.NoError(t, err)
				require.Len(t, listOut.Scopes, 1)
				assert.Equal(t, "sso:account:access", aws.ToString(listOut.Scopes[0].Scope))

				_, err = client.DeleteApplicationAccessScope(
					t.Context(),
					&ssoadminsdk.DeleteApplicationAccessScopeInput{
						ApplicationArn: appArn,
						Scope:          aws.String("sso:account:access"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "application authentication methods",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				inst, err := client.CreateInstance(
					t.Context(),
					&ssoadminsdk.CreateInstanceInput{Name: aws.String("s9-auth-inst")},
				)
				require.NoError(t, err)

				appOut, err := client.CreateApplication(t.Context(), &ssoadminsdk.CreateApplicationInput{
					InstanceArn:            inst.InstanceArn,
					ApplicationProviderArn: aws.String("arn:aws:sso::aws:applicationProvider/custom"),
					Name:                   aws.String("s9-auth-application"),
				})
				require.NoError(t, err)
				appArn := appOut.ApplicationArn

				_, err = client.PutApplicationAuthenticationMethod(
					t.Context(),
					&ssoadminsdk.PutApplicationAuthenticationMethodInput{
						ApplicationArn:           appArn,
						AuthenticationMethodType: ssoadmintypes.AuthenticationMethodTypeIam,
						AuthenticationMethod: &ssoadmintypes.AuthenticationMethodMemberIam{
							Value: ssoadmintypes.IamAuthenticationMethod{
								ActorPolicy: document.NewLazyDocument(map[string]any{
									"Version": "2012-10-17",
									"Statement": []any{
										map[string]any{
											"Effect": "Allow",
											"Principal": map[string]any{
												"AWS": "arn:aws:iam::222222222222:root",
											},
											"Action": "sso-oauth:CreateTokenWithIAM",
										},
									},
								}),
							},
						},
					},
				)
				require.NoError(t, err)

				getOut, err := client.GetApplicationAuthenticationMethod(
					t.Context(),
					&ssoadminsdk.GetApplicationAuthenticationMethodInput{
						ApplicationArn:           appArn,
						AuthenticationMethodType: ssoadmintypes.AuthenticationMethodTypeIam,
					},
				)
				require.NoError(t, err)
				iamMethod, ok := getOut.AuthenticationMethod.(*ssoadmintypes.AuthenticationMethodMemberIam)
				require.True(t, ok)
				require.NotNil(t, iamMethod.Value.ActorPolicy)

				listOut, err := client.ListApplicationAuthenticationMethods(
					t.Context(),
					&ssoadminsdk.ListApplicationAuthenticationMethodsInput{ApplicationArn: appArn},
				)
				require.NoError(t, err)
				require.Len(t, listOut.AuthenticationMethods, 1)
				assert.Equal(
					t,
					ssoadmintypes.AuthenticationMethodTypeIam,
					listOut.AuthenticationMethods[0].AuthenticationMethodType,
				)

				_, err = client.DeleteApplicationAuthenticationMethod(
					t.Context(),
					&ssoadminsdk.DeleteApplicationAuthenticationMethodInput{
						ApplicationArn:           appArn,
						AuthenticationMethodType: ssoadmintypes.AuthenticationMethodTypeIam,
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "application grants",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				inst, err := client.CreateInstance(
					t.Context(),
					&ssoadminsdk.CreateInstanceInput{Name: aws.String("s9-grant-inst")},
				)
				require.NoError(t, err)

				appOut, err := client.CreateApplication(t.Context(), &ssoadminsdk.CreateApplicationInput{
					InstanceArn:            inst.InstanceArn,
					ApplicationProviderArn: aws.String("arn:aws:sso::aws:applicationProvider/custom"),
					Name:                   aws.String("s9-grant-application"),
				})
				require.NoError(t, err)
				appArn := appOut.ApplicationArn

				_, err = client.PutApplicationGrant(t.Context(), &ssoadminsdk.PutApplicationGrantInput{
					ApplicationArn: appArn,
					GrantType:      ssoadmintypes.GrantTypeRefreshToken,
					Grant: &ssoadmintypes.GrantMemberRefreshToken{
						Value: ssoadmintypes.RefreshTokenGrant{},
					},
				})
				require.NoError(t, err)

				getOut, err := client.GetApplicationGrant(
					t.Context(),
					&ssoadminsdk.GetApplicationGrantInput{
						ApplicationArn: appArn,
						GrantType:      ssoadmintypes.GrantTypeRefreshToken,
					},
				)
				require.NoError(t, err)
				_, ok := getOut.Grant.(*ssoadmintypes.GrantMemberRefreshToken)
				assert.True(t, ok)

				listOut, err := client.ListApplicationGrants(
					t.Context(),
					&ssoadminsdk.ListApplicationGrantsInput{
						ApplicationArn: appArn,
					},
				)
				require.NoError(t, err)
				require.Len(t, listOut.Grants, 1)
				assert.Equal(t, ssoadmintypes.GrantTypeRefreshToken, listOut.Grants[0].GrantType)

				_, err = client.DeleteApplicationGrant(
					t.Context(),
					&ssoadminsdk.DeleteApplicationGrantInput{
						ApplicationArn: appArn,
						GrantType:      ssoadmintypes.GrantTypeRefreshToken,
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "application configuration",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				inst, err := client.CreateInstance(
					t.Context(),
					&ssoadminsdk.CreateInstanceInput{Name: aws.String("s9-cfg-inst")},
				)
				require.NoError(t, err)

				appOut, err := client.CreateApplication(t.Context(), &ssoadminsdk.CreateApplicationInput{
					InstanceArn:            inst.InstanceArn,
					ApplicationProviderArn: aws.String("arn:aws:sso::aws:applicationProvider/custom"),
					Name:                   aws.String("s9-cfg-application"),
				})
				require.NoError(t, err)
				appArn := appOut.ApplicationArn

				_, err = client.PutApplicationAssignmentConfiguration(
					t.Context(),
					&ssoadminsdk.PutApplicationAssignmentConfigurationInput{
						ApplicationArn:     appArn,
						AssignmentRequired: aws.Bool(false),
					},
				)
				require.NoError(t, err)

				getAssignCfg, err := client.GetApplicationAssignmentConfiguration(
					t.Context(),
					&ssoadminsdk.GetApplicationAssignmentConfigurationInput{ApplicationArn: appArn},
				)
				require.NoError(t, err)
				assert.False(t, aws.ToBool(getAssignCfg.AssignmentRequired))

				_, err = client.PutApplicationSessionConfiguration(
					t.Context(),
					&ssoadminsdk.PutApplicationSessionConfigurationInput{
						ApplicationArn:                         appArn,
						UserBackgroundSessionApplicationStatus: ssoadmintypes.UserBackgroundSessionApplicationStatusEnabled,
					},
				)
				require.NoError(t, err)

				getSessionCfg, err := client.GetApplicationSessionConfiguration(
					t.Context(),
					&ssoadminsdk.GetApplicationSessionConfigurationInput{ApplicationArn: appArn},
				)
				require.NoError(t, err)
				assert.Equal(
					t,
					ssoadmintypes.UserBackgroundSessionApplicationStatusEnabled,
					getSessionCfg.UserBackgroundSessionApplicationStatus,
				)
			},
		},
		{
			name: "trusted token issuers",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				inst, err := client.CreateInstance(
					t.Context(),
					&ssoadminsdk.CreateInstanceInput{Name: aws.String("s9-tti-inst")},
				)
				require.NoError(t, err)

				createOut, err := client.CreateTrustedTokenIssuer(
					t.Context(),
					&ssoadminsdk.CreateTrustedTokenIssuerInput{
						InstanceArn:            inst.InstanceArn,
						Name:                   aws.String("s9-tti"),
						TrustedTokenIssuerType: ssoadmintypes.TrustedTokenIssuerTypeOidcJwt,
						TrustedTokenIssuerConfiguration: &ssoadmintypes.TrustedTokenIssuerConfigurationMemberOidcJwtConfiguration{
							Value: ssoadmintypes.OidcJwtConfiguration{
								ClaimAttributePath:         aws.String("email"),
								IdentityStoreAttributePath: aws.String("emails.value"),
								IssuerUrl:                  aws.String("https://issuer.example.com"),
								JwksRetrievalOption:        ssoadmintypes.JwksRetrievalOptionOpenIdDiscovery,
							},
						},
					},
				)
				require.NoError(t, err)
				ttiArn := createOut.TrustedTokenIssuerArn

				descOut, err := client.DescribeTrustedTokenIssuer(
					t.Context(),
					&ssoadminsdk.DescribeTrustedTokenIssuerInput{
						TrustedTokenIssuerArn: ttiArn,
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "s9-tti", aws.ToString(descOut.Name))

				_, err = client.UpdateTrustedTokenIssuer(
					t.Context(),
					&ssoadminsdk.UpdateTrustedTokenIssuerInput{
						TrustedTokenIssuerArn: ttiArn,
						Name:                  aws.String("s9-tti-renamed"),
					},
				)
				require.NoError(t, err)

				descAfter, err := client.DescribeTrustedTokenIssuer(
					t.Context(),
					&ssoadminsdk.DescribeTrustedTokenIssuerInput{
						TrustedTokenIssuerArn: ttiArn,
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "s9-tti-renamed", aws.ToString(descAfter.Name))

				listOut, err := client.ListTrustedTokenIssuers(
					t.Context(),
					&ssoadminsdk.ListTrustedTokenIssuersInput{
						InstanceArn: inst.InstanceArn,
					},
				)
				require.NoError(t, err)
				require.Len(t, listOut.TrustedTokenIssuers, 1)

				_, err = client.DeleteTrustedTokenIssuer(
					t.Context(),
					&ssoadminsdk.DeleteTrustedTokenIssuerInput{
						TrustedTokenIssuerArn: ttiArn,
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "instance access control attribute configuration",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				inst, err := client.CreateInstance(
					t.Context(),
					&ssoadminsdk.CreateInstanceInput{Name: aws.String("s9-iaca-inst")},
				)
				require.NoError(t, err)

				_, err = client.CreateInstanceAccessControlAttributeConfiguration(
					t.Context(),
					&ssoadminsdk.CreateInstanceAccessControlAttributeConfigurationInput{
						InstanceArn: inst.InstanceArn,
						InstanceAccessControlAttributeConfiguration: &ssoadmintypes.InstanceAccessControlAttributeConfiguration{
							AccessControlAttributes: []ssoadmintypes.AccessControlAttribute{
								{
									Key: aws.String("department"),
									Value: &ssoadmintypes.AccessControlAttributeValue{
										Source: []string{"${path:enterprise.department}"},
									},
								},
							},
						},
					},
				)
				require.NoError(t, err)

				descOut, err := client.DescribeInstanceAccessControlAttributeConfiguration(
					t.Context(),
					&ssoadminsdk.DescribeInstanceAccessControlAttributeConfigurationInput{
						InstanceArn: inst.InstanceArn,
					},
				)
				require.NoError(t, err)
				require.Len(
					t,
					descOut.InstanceAccessControlAttributeConfiguration.AccessControlAttributes,
					1,
				)
				assert.Equal(
					t,
					"department",
					aws.ToString(
						descOut.InstanceAccessControlAttributeConfiguration.AccessControlAttributes[0].Key,
					),
				)

				_, err = client.UpdateInstanceAccessControlAttributeConfiguration(
					t.Context(),
					&ssoadminsdk.UpdateInstanceAccessControlAttributeConfigurationInput{
						InstanceArn: inst.InstanceArn,
						InstanceAccessControlAttributeConfiguration: &ssoadmintypes.InstanceAccessControlAttributeConfiguration{
							AccessControlAttributes: []ssoadmintypes.AccessControlAttribute{
								{
									Key: aws.String("team"),
									Value: &ssoadmintypes.AccessControlAttributeValue{
										Source: []string{"${path:enterprise.team}"},
									},
								},
							},
						},
					},
				)
				require.NoError(t, err)

				descAfter, err := client.DescribeInstanceAccessControlAttributeConfiguration(
					t.Context(),
					&ssoadminsdk.DescribeInstanceAccessControlAttributeConfigurationInput{
						InstanceArn: inst.InstanceArn,
					},
				)
				require.NoError(t, err)
				require.Len(
					t,
					descAfter.InstanceAccessControlAttributeConfiguration.AccessControlAttributes,
					1,
				)
				assert.Equal(
					t,
					"team",
					aws.ToString(
						descAfter.InstanceAccessControlAttributeConfiguration.AccessControlAttributes[0].Key,
					),
				)

				_, err = client.DeleteInstanceAccessControlAttributeConfiguration(
					t.Context(),
					&ssoadminsdk.DeleteInstanceAccessControlAttributeConfigurationInput{
						InstanceArn: inst.InstanceArn,
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "instance describe and update",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				inst, err := client.CreateInstance(
					t.Context(),
					&ssoadminsdk.CreateInstanceInput{Name: aws.String("s9-inst-desc")},
				)
				require.NoError(t, err)

				descOut, err := client.DescribeInstance(t.Context(), &ssoadminsdk.DescribeInstanceInput{
					InstanceArn: inst.InstanceArn,
				})
				require.NoError(t, err)
				assert.Equal(t, "s9-inst-desc", aws.ToString(descOut.Name))

				_, err = client.UpdateInstance(t.Context(), &ssoadminsdk.UpdateInstanceInput{
					InstanceArn: inst.InstanceArn,
					Name:        aws.String("s9-inst-desc-renamed"),
				})
				require.NoError(t, err)

				descAfter, err := client.DescribeInstance(t.Context(), &ssoadminsdk.DescribeInstanceInput{
					InstanceArn: inst.InstanceArn,
				})
				require.NoError(t, err)
				assert.Equal(t, "s9-inst-desc-renamed", aws.ToString(descAfter.Name))
			},
		},
		{
			name: "regions",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				inst, err := client.CreateInstance(
					t.Context(),
					&ssoadminsdk.CreateInstanceInput{Name: aws.String("s9-region-inst")},
				)
				require.NoError(t, err)

				addOut, err := client.AddRegion(t.Context(), &ssoadminsdk.AddRegionInput{
					InstanceArn: inst.InstanceArn,
					RegionName:  aws.String("eu-west-1"),
				})
				require.NoError(t, err)
				assert.Equal(t, ssoadmintypes.RegionStatusAdding, addOut.Status)

				descOut, err := client.DescribeRegion(t.Context(), &ssoadminsdk.DescribeRegionInput{
					InstanceArn: inst.InstanceArn,
					RegionName:  aws.String("eu-west-1"),
				})
				require.NoError(t, err)
				assert.Equal(t, "eu-west-1", aws.ToString(descOut.RegionName))
				assert.Equal(t, ssoadmintypes.RegionStatusActive, descOut.Status)

				listOut, err := client.ListRegions(
					t.Context(),
					&ssoadminsdk.ListRegionsInput{InstanceArn: inst.InstanceArn},
				)
				require.NoError(t, err)
				require.Len(t, listOut.Regions, 1)
				assert.Equal(t, "eu-west-1", aws.ToString(listOut.Regions[0].RegionName))

				removeOut, err := client.RemoveRegion(t.Context(), &ssoadminsdk.RemoveRegionInput{
					InstanceArn: inst.InstanceArn,
					RegionName:  aws.String("eu-west-1"),
				})
				require.NoError(t, err)
				assert.Equal(t, ssoadmintypes.RegionStatusRemoving, removeOut.Status)
			},
		},
		{
			name: "tags",
			run: func(t *testing.T) {
				t.Helper()

				client := newRealClient(t)

				inst, err := client.CreateInstance(
					t.Context(),
					&ssoadminsdk.CreateInstanceInput{Name: aws.String("s9-tag-inst")},
				)
				require.NoError(t, err)

				_, err = client.TagResource(t.Context(), &ssoadminsdk.TagResourceInput{
					InstanceArn: inst.InstanceArn,
					ResourceArn: inst.InstanceArn,
					Tags:        []ssoadmintypes.Tag{{Key: aws.String("k9"), Value: aws.String("v9")}},
				})
				require.NoError(t, err)

				listOut, err := client.ListTagsForResource(
					t.Context(),
					&ssoadminsdk.ListTagsForResourceInput{
						InstanceArn: inst.InstanceArn,
						ResourceArn: inst.InstanceArn,
					},
				)
				require.NoError(t, err)
				require.Len(t, listOut.Tags, 1)

				_, err = client.UntagResource(t.Context(), &ssoadminsdk.UntagResourceInput{
					InstanceArn: inst.InstanceArn,
					ResourceArn: inst.InstanceArn,
					TagKeys:     []string{"k9"},
				})
				require.NoError(t, err)

				listAfter, err := client.ListTagsForResource(
					t.Context(),
					&ssoadminsdk.ListTagsForResourceInput{
						InstanceArn: inst.InstanceArn,
						ResourceArn: inst.InstanceArn,
					},
				)
				require.NoError(t, err)
				assert.Empty(t, listAfter.Tags)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
