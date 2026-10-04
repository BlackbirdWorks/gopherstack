package iam

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSimulateCustomPolicy_AggregatesPerAction checks one top-level result per action with per-resource decisions.
func TestSimulateCustomPolicy_AggregatesPerAction(t *testing.T) {
	t.Parallel()

	const (
		resA = "arn:aws:s3:::a/key"
		resB = "arn:aws:s3:::b/key"
		resC = "arn:aws:s3:::c/key"
	)

	allowA := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"` + resA + `"}]}`
	allowAB := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`
	denyB := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:GetObject","Resource":"` + resB + `"}]}`

	tests := []struct {
		wantPer   map[string]types.PolicyEvaluationDecisionType
		name      string
		wantTop   types.PolicyEvaluationDecisionType
		policies  []string
		resources []string
	}{
		{
			name: "all_allowed", policies: []string{allowAB}, resources: []string{resA, resB},
			wantTop: types.PolicyEvaluationDecisionTypeAllowed,
			wantPer: map[string]types.PolicyEvaluationDecisionType{
				resA: types.PolicyEvaluationDecisionTypeAllowed, resB: types.PolicyEvaluationDecisionTypeAllowed,
			},
		},
		{
			name: "implicit_deny_wins_over_allow", policies: []string{allowA}, resources: []string{resA, resC},
			wantTop: types.PolicyEvaluationDecisionTypeImplicitDeny,
			wantPer: map[string]types.PolicyEvaluationDecisionType{
				resA: types.PolicyEvaluationDecisionTypeAllowed, resC: types.PolicyEvaluationDecisionTypeImplicitDeny,
			},
		},
		{
			name: "explicit_deny_wins", policies: []string{allowAB, denyB}, resources: []string{resA, resB, resC},
			wantTop: types.PolicyEvaluationDecisionTypeExplicitDeny,
			wantPer: map[string]types.PolicyEvaluationDecisionType{
				resA: types.PolicyEvaluationDecisionTypeAllowed,
				resB: types.PolicyEvaluationDecisionTypeExplicitDeny,
				resC: types.PolicyEvaluationDecisionTypeAllowed,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newSigningCertTestClient(t, NewHandler(NewInMemoryBackend()))

			out, err := client.SimulateCustomPolicy(t.Context(), &iamsdk.SimulateCustomPolicyInput{
				PolicyInputList: tt.policies,
				ActionNames:     []string{"s3:GetObject", "s3:PutObject"},
				ResourceArns:    tt.resources,
			})
			require.NoError(t, err)
			require.Len(t, out.EvaluationResults, 2)

			get := out.EvaluationResults[0]
			assert.Equal(t, "s3:GetObject", aws.ToString(get.EvalActionName))
			assert.Equal(t, tt.wantTop, get.EvalDecision)
			require.Len(t, get.ResourceSpecificResults, len(tt.resources))

			got := map[string]types.PolicyEvaluationDecisionType{}
			for _, r := range get.ResourceSpecificResults {
				got[aws.ToString(r.EvalResourceName)] = r.EvalResourceDecision
			}
			assert.Equal(t, tt.wantPer, got)

			put := out.EvaluationResults[1]
			assert.Equal(t, "s3:PutObject", aws.ToString(put.EvalActionName))
			assert.Equal(t, types.PolicyEvaluationDecisionTypeImplicitDeny, put.EvalDecision)
		})
	}
}
