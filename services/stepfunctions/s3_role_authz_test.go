package stepfunctions_test

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

type recordingAuthorizer struct {
	err   error
	calls []string
	mu    sync.Mutex
}

func (r *recordingAuthorizer) AuthorizeRole(principal, _, action, resource string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls = append(r.calls, principal+" "+action+" "+resource)

	return r.err
}

func TestMapItemReader_S3RoleAuthorization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		authErr   error
		name      string
		wantErr   string
		wantState string
	}{
		{name: "allowed", wantState: "SUCCEEDED"},
		{
			name: "denied", authErr: roleauth.ErrAccessDenied, wantState: "FAILED",
			wantErr: "States.ItemReaderFailed",
		},
		{
			name: "untrusted", authErr: roleauth.ErrNotAssumable, wantState: "FAILED",
			wantErr: "States.ItemReaderFailed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				auth := &recordingAuthorizer{err: tt.authErr}
				b := stepfunctions.NewInMemoryBackend()
				b.SetS3Reader(&fakeS3Reader{data: []byte(`[{"n":1}]`)})
				b.SetRoleAuthorizer(auth)

				def := `{"StartAt":"M","States":{"M":{"Type":"Map","ItemReader":{` +
					`"Resource":"arn:aws:states:::s3:getObject",` +
					`"Parameters":{"Bucket":"bkt","Key":"items.json"}},` +
					`"ItemProcessor":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`

				name := "s3-authz-" + tt.name
				sm, err := b.CreateStateMachine(context.Background(), name, def, validRoleARN, "STANDARD")
				require.NoError(t, err)

				exec, err := b.StartExecution(sm.StateMachineArn, "e", "{}")
				require.NoError(t, err)

				synctest.Wait()

				got, err := b.DescribeExecution(exec.ExecutionArn)
				require.NoError(t, err)
				require.Equal(t, tt.wantState, got.Status, "cause=%s", got.Cause)

				if tt.wantErr != "" {
					assert.Equal(t, tt.wantErr, got.Error)
					assert.Contains(t, got.Cause, tt.authErr.Error())
				}

				auth.mu.Lock()
				defer auth.mu.Unlock()

				require.NotEmpty(t, auth.calls)
				assert.Equal(t, "states.amazonaws.com s3:GetObject arn:aws:s3:::bkt/items.json", auth.calls[0])
			})
		})
	}
}
