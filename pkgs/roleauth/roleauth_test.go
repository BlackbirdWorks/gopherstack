package roleauth_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

type denyAll struct{}

func (denyAll) AuthorizeRole(_, _, _, _ string) error { return roleauth.ErrAccessDenied }

func TestTargetAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		arn    string
		action string
		ok     bool
	}{
		{"lambda", "arn:aws:lambda:us-east-1:1:function:f", "lambda:InvokeFunction", true},
		{"sqs", "arn:aws:sqs:us-east-1:1:q", "sqs:SendMessage", true},
		{"bus", "arn:aws:events:us-east-1:1:event-bus/b", "events:PutEvents", true},
		{"api_destination", "arn:aws:events:us-east-1:1:api-destination/d/x", "events:InvokeApiDestination", true},
		{"execute_api", "arn:aws:execute-api:us-east-1:1:api/prod/POST/x", "execute-api:Invoke", true},
		{"unknown", "arn:aws:s3:::b", "", false},
		{"malformed", "nope", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			action, ok := roleauth.TargetAction(tt.arn)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.action, action)
		})
	}
}

func TestAuthorizeNilAllows(t *testing.T) {
	t.Parallel()

	require.NoError(t, roleauth.Authorize(nil, "p", "r", "a", "x"))
	assert.ErrorIs(t, roleauth.Authorize(denyAll{}, "p", "r", "a", "x"), roleauth.ErrAccessDenied)
}
