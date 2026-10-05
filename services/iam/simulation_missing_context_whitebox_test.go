package iam

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimulateCustomPolicy_MissingContextValues(t *testing.T) {
	t.Parallel()

	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*",` +
		`"Condition":{"StringEquals":{"aws:SourceVpc":"vpc-1"},"StringLike":{"s3:prefix":"a*"}}}]}`

	tests := []struct {
		name    string
		action  string
		entries []types.ContextEntry
		want    []string
	}{
		{name: "none_supplied", action: "s3:GetObject", want: []string{"aws:SourceVpc", "s3:prefix"}},
		{
			name: "one_supplied", action: "s3:GetObject",
			entries: []types.ContextEntry{{
				ContextKeyName:   aws.String("aws:SourceVpc"),
				ContextKeyType:   types.ContextKeyTypeEnumString,
				ContextKeyValues: []string{"vpc-1"},
			}},
			want: []string{"s3:prefix"},
		},
		{name: "statement_not_applicable", action: "s3:PutObject", want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newSigningCertTestClient(t, NewHandler(NewInMemoryBackend()))

			out, err := client.SimulateCustomPolicy(t.Context(), &iamsdk.SimulateCustomPolicyInput{
				PolicyInputList: []string{policy},
				ActionNames:     []string{tt.action},
				ResourceArns:    []string{"arn:aws:s3:::b/k"},
				ContextEntries:  tt.entries,
			})
			require.NoError(t, err)
			require.Len(t, out.EvaluationResults, 1)

			res := out.EvaluationResults[0]
			require.Len(t, res.ResourceSpecificResults, 1)

			for _, got := range [][]string{res.MissingContextValues, res.ResourceSpecificResults[0].MissingContextValues} {
				if len(tt.want) == 0 {
					assert.Empty(t, got)
				} else {
					assert.Equal(t, tt.want, got)
				}
			}
		})
	}
}
