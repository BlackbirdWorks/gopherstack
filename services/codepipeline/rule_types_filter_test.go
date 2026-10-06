package codepipeline_test

import (
	"testing"

	cpsdk "github.com/aws/aws-sdk-go-v2/service/codepipeline"
	"github.com/aws/aws-sdk-go-v2/service/codepipeline/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

// TestRealClient_ListRuleTypesOwnerFilter covers ruleOwnerFilter (serializers.go:5019, codepipeline@v1.49.4).
func TestRealClient_ListRuleTypesOwnerFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		owner types.RuleOwner
		empty bool
	}{
		{name: "no_filter"},
		{name: "aws", owner: types.RuleOwnerAws},
		{name: "third_party", owner: types.RuleOwner("ThirdParty"), empty: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCodePipelineClient(
				t,
				codepipeline.NewHandler(codepipeline.NewInMemoryBackend("000000000000", "us-east-1")),
			)
			out, err := client.ListRuleTypes(t.Context(), &cpsdk.ListRuleTypesInput{RuleOwnerFilter: tt.owner})
			require.NoError(t, err)

			if tt.empty {
				require.Empty(t, out.RuleTypes)
			} else {
				require.NotEmpty(t, out.RuleTypes)
			}
		})
	}
}
