package lambda_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/stretchr/testify/require"
)

func TestUpdate_RevisionIDOmittedVsEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		revision func(current *string) *string
		name     string
		wantErr  bool
	}{
		{name: "omitted_applies", revision: func(*string) *string { return nil }},
		{name: "matching_applies", revision: func(cur *string) *string { return cur }},
		{
			name:     "explicit_empty_precondition_failed",
			revision: func(*string) *string { return aws.String("") },
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			client := newTestLambdaClient(t, h)
			createDroppedMembersFunction(t, client, "fn")

			_, err := client.PublishVersion(t.Context(), &lambdasdk.PublishVersionInput{FunctionName: aws.String("fn")})
			require.NoError(t, err)

			alias, err := client.CreateAlias(t.Context(), &lambdasdk.CreateAliasInput{
				FunctionName: aws.String("fn"), Name: aws.String("a"), FunctionVersion: aws.String("1"),
			})
			require.NoError(t, err)

			_, aliasErr := client.UpdateAlias(t.Context(), &lambdasdk.UpdateAliasInput{
				FunctionName: aws.String("fn"), Name: aws.String("a"),
				Description: aws.String("d"), RevisionId: tt.revision(alias.RevisionId),
			})

			fn, err := client.GetFunctionConfiguration(t.Context(), &lambdasdk.GetFunctionConfigurationInput{
				FunctionName: aws.String("fn"),
			})
			require.NoError(t, err)

			_, cfgErr := client.UpdateFunctionConfiguration(t.Context(), &lambdasdk.UpdateFunctionConfigurationInput{
				FunctionName: aws.String("fn"),
				Description:  aws.String("cfg"),
				RevisionId:   tt.revision(fn.RevisionId),
			})

			if tt.wantErr {
				require.Error(t, aliasErr)
				require.Error(t, cfgErr)

				return
			}

			require.NoError(t, aliasErr)
			require.NoError(t, cfgErr)
		})
	}
}
