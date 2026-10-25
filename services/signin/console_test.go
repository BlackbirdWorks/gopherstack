package signin_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	signinsdk "github.com/aws/aws-sdk-go-v2/service/signin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsoleAuthorizationConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		target    *string
		wantID    string
		wantScope string
		wantCode  string
	}{
		{name: "default account", wantID: testAccount, wantScope: "ACCOUNT"},
		{name: "explicit account", target: aws.String(testAccount), wantID: testAccount, wantScope: "ACCOUNT"},
		{name: "organization", target: aws.String("o-abcde12345"), wantID: "o-abcde12345", wantScope: "ORGANIZATION"},
		{name: "root", target: aws.String("r-ab12"), wantID: "r-ab12", wantScope: "ROOT"},
		{name: "other account", target: aws.String("999999999999"), wantCode: "AccessDeniedException"},
		{name: "malformed", target: aws.String("nonsense-id"), wantCode: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newFixture(t).self(t)

			_, err := c.GetConsoleAuthorizationConfiguration(t.Context(), &signinsdk.GetConsoleAuthorizationConfigurationInput{TargetId: tt.target})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				_, err = c.PutConsoleAuthorizationConfiguration(t.Context(), &signinsdk.PutConsoleAuthorizationConfigurationInput{TargetId: tt.target})
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			_, err = c.DeleteConsoleAuthorizationConfiguration(t.Context(), &signinsdk.DeleteConsoleAuthorizationConfigurationInput{TargetId: tt.target})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			put, err := c.PutConsoleAuthorizationConfiguration(t.Context(), &signinsdk.PutConsoleAuthorizationConfigurationInput{TargetId: tt.target})
			require.NoError(t, err)
			assert.Equal(t, tt.wantID, aws.ToString(put.TargetId))
			assert.Equal(t, tt.wantScope, aws.ToString(put.Scope))
			assert.True(t, aws.ToBool(put.ConsoleAuthorizationEnabled))

			get, err := c.GetConsoleAuthorizationConfiguration(t.Context(), &signinsdk.GetConsoleAuthorizationConfigurationInput{TargetId: tt.target})
			require.NoError(t, err)
			assert.True(t, aws.ToBool(get.ConsoleAuthorizationEnabled))

			del, err := c.DeleteConsoleAuthorizationConfiguration(t.Context(), &signinsdk.DeleteConsoleAuthorizationConfigurationInput{TargetId: tt.target})
			require.NoError(t, err)
			assert.False(t, aws.ToBool(del.ConsoleAuthorizationEnabled))
			assert.Equal(t, tt.wantScope, aws.ToString(del.Scope))

			_, err = c.GetConsoleAuthorizationConfiguration(t.Context(), &signinsdk.GetConsoleAuthorizationConfigurationInput{TargetId: tt.target})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		})
	}
}
