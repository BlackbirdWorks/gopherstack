package bedrock_test

import (
	"errors"
	"net/http"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrocksdk "github.com/aws/aws-sdk-go-v2/service/bedrock"
	"github.com/aws/aws-sdk-go-v2/service/bedrock/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/bedrock"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
)

func apiErrorCode(err error) string {
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
		return apiErr.ErrorCode()
	}

	return ""
}

func TestAutomatedReasoningTestCase_StoresBodyAndValidates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in       func(arn *string) *bedrocksdk.CreateAutomatedReasoningPolicyTestCaseInput
		name     string
		wantCode string
	}{
		{
			name: "stored",
			in: func(arn *string) *bedrocksdk.CreateAutomatedReasoningPolicyTestCaseInput {
				return &bedrocksdk.CreateAutomatedReasoningPolicyTestCaseInput{
					PolicyArn: arn, GuardContent: aws.String("claim"), QueryContent: aws.String("question?"),
					ExpectedAggregatedFindingsResult: types.AutomatedReasoningCheckResultSatisfiable,
					ConfidenceThreshold:              aws.Float64(0.7),
				}
			},
		},
		{
			name: "unknown_expected_result",
			in: func(arn *string) *bedrocksdk.CreateAutomatedReasoningPolicyTestCaseInput {
				return &bedrocksdk.CreateAutomatedReasoningPolicyTestCaseInput{
					PolicyArn: arn, GuardContent: aws.String("claim"),
					ExpectedAggregatedFindingsResult: types.AutomatedReasoningCheckResult("MAYBE"),
				}
			},
			wantCode: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)

			policy, err := client.CreateAutomatedReasoningPolicy(t.Context(),
				&bedrocksdk.CreateAutomatedReasoningPolicyInput{Name: aws.String("arp")})
			require.NoError(t, err)

			tc, err := client.CreateAutomatedReasoningPolicyTestCase(t.Context(), tt.in(policy.PolicyArn))
			if tt.wantCode != "" {
				require.Equal(t, tt.wantCode, apiErrorCode(err))

				return
			}

			require.NoError(t, err)

			got, err := client.GetAutomatedReasoningPolicyTestCase(
				t.Context(),
				&bedrocksdk.GetAutomatedReasoningPolicyTestCaseInput{
					PolicyArn:  policy.PolicyArn,
					TestCaseId: tc.TestCaseId,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "claim", aws.ToString(got.TestCase.GuardContent))
			assert.Equal(t, "question?", aws.ToString(got.TestCase.QueryContent))
			assert.Equal(
				t,
				types.AutomatedReasoningCheckResultSatisfiable,
				got.TestCase.ExpectedAggregatedFindingsResult,
			)
			assert.InDelta(t, 0.7, aws.ToFloat64(got.TestCase.ConfidenceThreshold), 0.0001)
		})
	}
}

func TestAutomatedReasoning_ClientRequestTokenReplay(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	policy, err := client.CreateAutomatedReasoningPolicy(ctx,
		&bedrocksdk.CreateAutomatedReasoningPolicyInput{Name: aws.String("arp")})
	require.NoError(t, err)

	version := func(token, hash string) (*bedrocksdk.CreateAutomatedReasoningPolicyVersionOutput, error) {
		return client.CreateAutomatedReasoningPolicyVersion(ctx, &bedrocksdk.CreateAutomatedReasoningPolicyVersionInput{
			PolicyArn:                 policy.PolicyArn,
			LastUpdatedDefinitionHash: aws.String(hash),
			ClientRequestToken:        aws.String(token),
		})
	}

	v1, err := version("v-token", "h1")
	require.NoError(t, err)

	v1Again, err := version("v-token", "h1")
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(v1.Version), aws.ToString(v1Again.Version), "same token replays the same version")

	_, err = version("v-token", "other-hash")
	require.Equal(t, "ConflictException", apiErrorCode(err))

	tcIn := func(token, guard string) *bedrocksdk.CreateAutomatedReasoningPolicyTestCaseInput {
		return &bedrocksdk.CreateAutomatedReasoningPolicyTestCaseInput{
			PolicyArn: policy.PolicyArn, GuardContent: aws.String(guard), ClientRequestToken: aws.String(token),
			ExpectedAggregatedFindingsResult: types.AutomatedReasoningCheckResultValid,
		}
	}

	tc1, err := client.CreateAutomatedReasoningPolicyTestCase(ctx, tcIn("tc-token", "g"))
	require.NoError(t, err)

	tc2, err := client.CreateAutomatedReasoningPolicyTestCase(ctx, tcIn("tc-token", "g"))
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(tc1.TestCaseId), aws.ToString(tc2.TestCaseId))

	_, err = client.CreateAutomatedReasoningPolicyTestCase(ctx, tcIn("tc-token", "different"))
	require.Equal(t, "ConflictException", apiErrorCode(err))

	list, err := client.ListAutomatedReasoningPolicyTestCases(ctx,
		&bedrocksdk.ListAutomatedReasoningPolicyTestCasesInput{PolicyArn: policy.PolicyArn})
	require.NoError(t, err)
	assert.Len(t, list.TestCases, 1, "the replay created nothing new")

	wfIn := func(token string) *bedrocksdk.StartAutomatedReasoningPolicyBuildWorkflowInput {
		return &bedrocksdk.StartAutomatedReasoningPolicyBuildWorkflowInput{
			PolicyArn:          policy.PolicyArn,
			BuildWorkflowType:  types.AutomatedReasoningPolicyBuildWorkflowTypeIngestContent,
			ClientRequestToken: aws.String(token),
			SourceContent: &types.AutomatedReasoningPolicyBuildWorkflowSource{
				PolicyDefinition: &types.AutomatedReasoningPolicyDefinition{Version: aws.String("1")},
			},
		}
	}

	wf1, err := client.StartAutomatedReasoningPolicyBuildWorkflow(ctx, wfIn("wf-token"))
	require.NoError(t, err)

	wf2, err := client.StartAutomatedReasoningPolicyBuildWorkflow(ctx, wfIn("wf-token"))
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(wf1.BuildWorkflowId), aws.ToString(wf2.BuildWorkflowId))

	wf3, err := client.StartAutomatedReasoningPolicyBuildWorkflow(ctx, wfIn("another-token"))
	require.NoError(t, err)
	assert.NotEqual(t, aws.ToString(wf1.BuildWorkflowId), aws.ToString(wf3.BuildWorkflowId))
}

type kmsSibling struct{ h *kmsbackend.Handler }

func (k kmsSibling) GetKMSHandler() service.Registerable { return k.h }

func TestKMSKeyMembers_RequireExistingKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		keyID    func(realARN string) string
		wantCode string
	}{
		{name: "existing_key_arn", keyID: func(a string) string { return a }},
		{
			name:     "unknown_key",
			keyID:    func(string) string { return "11111111-2222-3333-4444-555555555555" },
			wantCode: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			kmsH := kmsbackend.NewHandler(kmsbackend.NewInMemoryBackend())
			key, err := kmsH.Backend.CreateKey(t.Context(), &kmsbackend.CreateKeyInput{})
			require.NoError(t, err)

			backend := bedrock.NewInMemoryBackend("123456789012", "us-east-1")
			backend.SetAppConfig(kmsSibling{h: kmsH})
			client := newTestBedrockClient(t, bedrock.NewHandler(backend))

			out, err := client.CreateGuardrail(t.Context(), &bedrocksdk.CreateGuardrailInput{
				Name:                    aws.String("gr"),
				BlockedInputMessaging:   aws.String("in"),
				BlockedOutputsMessaging: aws.String("out"),
				KmsKeyId:                aws.String(tt.keyID(key.KeyMetadata.Arn)),
			})

			if tt.wantCode != "" {
				require.Equal(t, tt.wantCode, apiErrorCode(err))

				return
			}

			require.NoError(t, err)

			got, err := client.GetGuardrail(
				t.Context(),
				&bedrocksdk.GetGuardrailInput{GuardrailIdentifier: out.GuardrailId},
			)
			require.NoError(t, err)
			assert.Equal(t, key.KeyMetadata.Arn, aws.ToString(got.KmsKeyArn))
		})
	}
}

func TestListFoundationModelAgreementOffers_OfferType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		offerType types.OfferType
		wantErr   bool
	}{
		{name: "none"},
		{name: "all", offerType: types.OfferTypeAll},
		{name: "public", offerType: types.OfferTypePublic},
		{name: "invalid", offerType: types.OfferType("PRIVATE"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)

			out, err := client.ListFoundationModelAgreementOffers(
				t.Context(),
				&bedrocksdk.ListFoundationModelAgreementOffersInput{
					ModelId: aws.String("anthropic.claude-v2"), OfferType: tt.offerType,
				},
			)

			if tt.wantErr {
				require.Equal(t, "ValidationException", apiErrorCode(err))

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, out.Offers)
		})
	}
}

func TestAutomatedReasoningTestCase_RequiresGuardContent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	policy, err := h.Backend.CreateAutomatedReasoningPolicy("tc-required", "", nil)
	require.NoError(t, err)

	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/automated-reasoning-policies/"+url.PathEscape(policy.PolicyArn)+"/test-cases",
		map[string]any{"expectedAggregatedFindingsResult": "VALID"},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
