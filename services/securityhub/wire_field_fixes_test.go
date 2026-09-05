package securityhub_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"
	securityhubtypes "github.com/aws/aws-sdk-go-v2/service/securityhub/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

// TestGetAdministratorAndMasterAccount_MemberStatus guards against
// GetAdministratorAccount/GetMasterAccount emitting the fabricated
// "RelationshipStatus" key: the real GetAdministratorAccountOutput.Administrator/
// GetMasterAccountOutput.Master are both *types.Invitation
// (securityhub@v1.75.4 api_op_GetAdministratorAccount.go /
// api_op_GetMasterAccount.go), whose status member is "MemberStatus" -- the
// same real field ListInvitations' Invitation model already names
// correctly, a sibling trap this handler had not yet picked up. Before the
// fix, a real client's typed .MemberStatus field was always empty
// regardless of backend state.
func TestGetAdministratorAndMasterAccount_MemberStatus(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

	_, err := client.AcceptAdministratorInvitation(t.Context(), &securityhubsdk.AcceptAdministratorInvitationInput{
		AdministratorId: aws.String("111111111111"),
		InvitationId:    aws.String("invitation-1"),
	})
	require.NoError(t, err)

	adminOut, err := client.GetAdministratorAccount(t.Context(), &securityhubsdk.GetAdministratorAccountInput{})
	require.NoError(t, err)
	require.NotNil(t, adminOut.Administrator)
	assert.Equal(t, "ENABLED", aws.ToString(adminOut.Administrator.MemberStatus))
	assert.Equal(t, "111111111111", aws.ToString(adminOut.Administrator.AccountId))

	//nolint:staticcheck // GetMasterAccount is deprecated by AWS in favor of GetAdministratorAccount but
	// is still a real, served op (opGetMasterAccount) this handler routes -- in this issue's L+D+G scope.
	masterOut, err := client.GetMasterAccount(t.Context(), &securityhubsdk.GetMasterAccountInput{})
	require.NoError(t, err)
	require.NotNil(t, masterOut.Master)
	assert.Equal(t, "ENABLED", aws.ToString(masterOut.Master.MemberStatus))
}

// TestListOrganizationAdminAccounts_FeatureEcho guards against
// ListOrganizationAdminAccounts dropping the real, always-echoed "Feature"
// response member (securityhub@v1.75.4 api_op_ListOrganizationAdminAccounts.go:
// "Defaults to Security Hub CSPM if not specified"). This backend doesn't
// track admin accounts per-feature, so the echo isn't filtered by it, only
// reflected back -- still a real gap, since a real client's typed .Feature
// field was previously always empty regardless of the request.
func TestListOrganizationAdminAccounts_FeatureEcho(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

	// Default (unset Feature) echoes the real default value.
	out, err := client.ListOrganizationAdminAccounts(
		t.Context(), &securityhubsdk.ListOrganizationAdminAccountsInput{},
	)
	require.NoError(t, err)
	assert.Equal(t, securityhubtypes.SecurityHubFeatureSecurityHub, out.Feature)

	// Explicit Feature echoes back what was sent.
	out, err = client.ListOrganizationAdminAccounts(t.Context(), &securityhubsdk.ListOrganizationAdminAccountsInput{
		Feature: securityhubtypes.SecurityHubFeatureSecurityHubV2,
	})
	require.NoError(t, err)
	assert.Equal(t, securityhubtypes.SecurityHubFeatureSecurityHubV2, out.Feature)
}

// TestListConnectorsV2_ProviderSummaryShape guards against ListConnectorsV2
// emitting a flat "Provider"/"ConnectorStatus" shape where the real
// types.ConnectorSummary (securityhub@v1.75.4 types.go:14833-14871) requires
// a nested, required ProviderSummary{ConnectorStatus,ProviderConfiguration,
// ProviderName} object -- a real client's typed .ProviderSummary field was
// always the zero value regardless of backend state. Mirrors the already-
// correct V1 CspmConnector sibling (ListConnectors' ProviderSummary, see
// connectors_v2_test.go's V1 "list" step).
func TestListConnectorsV2_ProviderSummaryShape(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

	_, err := client.CreateConnectorV2(t.Context(), &securityhubsdk.CreateConnectorV2Input{
		Name: aws.String("test-connector-v2"),
		Provider: &securityhubtypes.ProviderConfigurationMemberJiraCloud{
			Value: securityhubtypes.JiraCloudProviderConfiguration{
				ProjectKey: aws.String("SEC"),
			},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListConnectorsV2(t.Context(), &securityhubsdk.ListConnectorsV2Input{})
	require.NoError(t, err)
	require.Len(t, listOut.Connectors, 1)

	summary := listOut.Connectors[0]
	require.NotNil(t, summary.ProviderSummary, "ProviderSummary is required on the real ConnectorSummary shape")
	assert.Equal(t, "JIRACLOUD", string(summary.ProviderSummary.ProviderName))
	assert.Equal(t, "ACTIVE", string(summary.ProviderSummary.ConnectorStatus))
	assert.Equal(t, "test-connector-v2", aws.ToString(summary.Name))
}

// TestBatchEnableStandards_ReachesReady is a real-SDK-client regression test
// for gopherstack-muzq: BatchEnableStandards stamped StandardsStatus PENDING
// and nothing else in this backend ever advanced it -- EnableHub's own
// default-standards subscriptions are stamped the terminal READY directly,
// which made the contrast easy to miss (a package where every explicitly-
// requested standard stalls looks internally consistent). A client polling
// GetEnabledStandards for readiness never saw a terminal status.
func TestBatchEnableStandards_ReachesReady(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

	enableOut, err := client.BatchEnableStandards(t.Context(), &securityhubsdk.BatchEnableStandardsInput{
		StandardsSubscriptionRequests: []securityhubtypes.StandardsSubscriptionRequest{
			{
				StandardsArn: aws.String(
					"arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0",
				),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, enableOut.StandardsSubscriptions, 1)
	require.Equal(t, securityhubtypes.StandardsStatusPending, enableOut.StandardsSubscriptions[0].StandardsStatus)

	subArn := enableOut.StandardsSubscriptions[0].StandardsSubscriptionArn

	getOut, err := client.GetEnabledStandards(t.Context(), &securityhubsdk.GetEnabledStandardsInput{
		StandardsSubscriptionArns: []string{aws.ToString(subArn)},
	})
	require.NoError(t, err)
	require.Len(t, getOut.StandardsSubscriptions, 1)
	assert.Equal(
		t, securityhubtypes.StandardsStatusReady, getOut.StandardsSubscriptions[0].StandardsStatus,
		"GetEnabledStandards must reap PENDING to READY on poll",
	)
}

// TestBatchGetSecurityControls_UnprocessedErrorCode_InvalidInputEnum guards
// against handleBatchGetSecurityControls emitting the free-form string
// "InvalidInput" under UnprocessedSecurityControl.ErrorCode, whose real type
// is types.UnprocessedErrorCode (securityhub@v1.75.4 types/types.go:19946),
// an enum whose members are upper-snake-case ("INVALID_INPUT", enums.go:2086)
// -- not the mixed-case string BatchUpdateFindings' *string ErrorCode uses.
// A typed client decodes the wrong value without error, so only comparing
// against the real constant (not a bare string) catches every non-member.
func TestBatchGetSecurityControls_UnprocessedErrorCode_InvalidInputEnum(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

	out, err := client.BatchGetSecurityControls(t.Context(), &securityhubsdk.BatchGetSecurityControlsInput{
		SecurityControlIds: []string{"no-such-control"},
	})
	require.NoError(t, err)
	require.Len(t, out.UnprocessedIds, 1)
	assert.Equal(t, securityhubtypes.UnprocessedErrorCodeInvalidInput, out.UnprocessedIds[0].ErrorCode)
}

// TestBatchAutomationRules_UnprocessedErrorCode_DecodesAsInt32 guards against
// BatchGetAutomationRules/BatchDeleteAutomationRules/BatchUpdateAutomationRules
// emitting a STRING under UnprocessedAutomationRule.ErrorCode; the real member
// is *int32 (securityhub@v1.75.4 types/types.go:19904, mirroring cloudfront's
// identically-shaped CustomErrorResponse.ErrorCode, "The HTTP status code").
// Before the fix, a real client's deserializer hard-fails on this field
// ("expected Integer to be json.Number, got string instead") -- confirmed by
// driving the real client against the unfixed handler -- so require.NoError
// on each call is itself part of the regression check, not just the value.
func TestBatchAutomationRules_UnprocessedErrorCode_DecodesAsInt32(t *testing.T) {
	t.Parallel()

	unknownArn := "arn:aws:securityhub:us-east-1:000000000000:automation-rule/does-not-exist"

	t.Run("get", func(t *testing.T) {
		t.Parallel()

		backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
		client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

		out, err := client.BatchGetAutomationRules(t.Context(), &securityhubsdk.BatchGetAutomationRulesInput{
			AutomationRulesArns: []string{unknownArn},
		})
		require.NoError(t, err)
		require.Len(t, out.UnprocessedAutomationRules, 1)
		require.NotNil(t, out.UnprocessedAutomationRules[0].ErrorCode)
		assert.Equal(t, int32(http.StatusNotFound), *out.UnprocessedAutomationRules[0].ErrorCode)
	})

	t.Run("delete", func(t *testing.T) {
		t.Parallel()

		backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
		client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

		out, err := client.BatchDeleteAutomationRules(t.Context(), &securityhubsdk.BatchDeleteAutomationRulesInput{
			AutomationRulesArns: []string{unknownArn},
		})
		require.NoError(t, err)
		require.Len(t, out.UnprocessedAutomationRules, 1)
		require.NotNil(t, out.UnprocessedAutomationRules[0].ErrorCode)
		assert.Equal(t, int32(http.StatusNotFound), *out.UnprocessedAutomationRules[0].ErrorCode)
	})

	t.Run("update", func(t *testing.T) {
		t.Parallel()

		backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
		client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

		out, err := client.BatchUpdateAutomationRules(t.Context(), &securityhubsdk.BatchUpdateAutomationRulesInput{
			UpdateAutomationRulesRequestItems: []securityhubtypes.UpdateAutomationRulesRequestItem{
				{RuleArn: aws.String(unknownArn)},
			},
		})
		require.NoError(t, err)
		require.Len(t, out.UnprocessedAutomationRules, 1)
		require.NotNil(t, out.UnprocessedAutomationRules[0].ErrorCode)
		assert.Equal(t, int32(http.StatusNotFound), *out.UnprocessedAutomationRules[0].ErrorCode)
	})
}

// TestDeclineDeleteInvitations_UnprocessedAccounts_NoInventedErrorFields_RealClient
// guards against handleDeclineInvitations/handleDeleteInvitations's
// unprocessed-account entries fabricating "ErrorCode"/"ErrorMessage" keys.
// DeclineInvitationsOutput/DeleteInvitationsOutput's UnprocessedAccounts is
// []types.Result (securityhub@v1.75.4 types/types.go:18271), which declares
// only AccountId and ProcessingResult -- a typed client silently discards the
// unknown keys and never observes them as a zero value, so only the raw body
// proves they were ever emitted.
func TestDeclineDeleteInvitations_UnprocessedAccounts_NoInventedErrorFields_RealClient(t *testing.T) {
	t.Parallel()

	t.Run("decline", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodPost, "/invitations/decline", map[string]any{
			"AccountIds": []any{"999999999999"},
		})
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		body := rec.Body.String()
		assert.NotContains(t, body, `"ErrorCode"`, "types.Result has no ErrorCode member")
		assert.NotContains(t, body, `"ErrorMessage"`, "types.Result has no ErrorMessage member")
		assert.Contains(t, body, `"ProcessingResult"`)
	})

	t.Run("delete", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, http.MethodPost, "/invitations/delete", map[string]any{
			"AccountIds": []any{"999999999999"},
		})
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		body := rec.Body.String()
		assert.NotContains(t, body, `"ErrorCode"`, "types.Result has no ErrorCode member")
		assert.NotContains(t, body, `"ErrorMessage"`, "types.Result has no ErrorMessage member")
		assert.Contains(t, body, `"ProcessingResult"`)
	})
}
