package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestStartDataQualityRulesetEvaluationRun_ClientTokenReplay(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	_, err := backend.CreateDataQualityRuleset("dq-token", "Rules = [ IsComplete \"id\" ]", nil)
	require.NoError(t, err)

	client := newTestGlueClient(t, glue.NewHandler(backend))
	ctx := t.Context()

	start := func(token *string) string {
		out, startErr := client.StartDataQualityRulesetEvaluationRun(
			ctx,
			&gluesdk.StartDataQualityRulesetEvaluationRunInput{
				ClientToken:  token,
				Role:         aws.String("arn:aws:iam::000000000000:role/dq"),
				RulesetNames: []string{"dq-token"},
				DataSource: &types.DataSource{
					GlueTable: &types.GlueTable{DatabaseName: aws.String("db"), TableName: aws.String("tbl")},
				},
			},
		)
		require.NoError(t, startErr)

		return aws.ToString(out.RunId)
	}

	first := start(aws.String("token-a"))

	tests := []struct {
		token    *string
		name     string
		wantSame bool
	}{
		{name: "same token replays", token: aws.String("token-a"), wantSame: true},
		{name: "other token starts new run", token: aws.String("token-b")},
		{name: "no token starts new run"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := start(tt.token)
			assert.Equal(t, tt.wantSame, got == first)
		})
	}
}
