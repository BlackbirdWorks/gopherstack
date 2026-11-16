package gcpsecretmanager_test

import (
	"errors"
	"sync"
	"testing"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errNoUser = errors.New("no such user")

type pwCall struct{ project, instance, user, password string }

type fakeSQL struct {
	err   error
	calls []pwCall
	mu    sync.Mutex
}

func (f *fakeSQL) SetUserPassword(project, instance, user, password string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, pwCall{project, instance, user, password})

	return f.err
}

func TestManagedRotationSetsSQLPassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setterErr error
		name      string
		wantCalls int
		wantCode  codes.Code
	}{
		{name: "applied", wantCalls: 2, wantCode: codes.OK},
		{name: "setter fails", setterErr: errNoUser, wantCalls: 1, wantCode: codes.FailedPrecondition},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			fake := &fakeSQL{err: tt.setterErr}
			e.handler.Backend.SetSQLPasswordSetter(fake)

			ctx := t.Context()
			sql := e.secret(t, "sql", cloudSQL)

			v1, err := e.client.EnableManagedRotation(ctx, enableReq(sql.GetName()))
			if tt.wantCode != codes.OK {
				assert.Equal(t, tt.wantCode, status.Code(err))
				require.Len(t, fake.calls, tt.wantCalls)

				return
			}

			require.NoError(t, err)

			_, err = e.client.RotateSecret(ctx, &secretmanagerpb.RotateSecretRequest{Parent: sql.GetName()})
			require.NoError(t, err)
			require.Len(t, fake.calls, tt.wantCalls)

			p1, err := e.client.AccessSecretVersion(
				ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: v1.GetName()})
			require.NoError(t, err)
			assert.Equal(t, pwCall{"test-project", "db1", "app", string(p1.GetPayload().GetData())}, fake.calls[0])
			assert.NotEqual(t, fake.calls[0].password, fake.calls[1].password)

			e2 := newEnv(t)
			fake2 := &fakeSQL{}
			e2.handler.Backend.SetSQLPasswordSetter(fake2)
			require.NoError(t, e2.handler.Restore(ctx, e.handler.Snapshot(ctx)))

			_, err = e2.client.RotateSecret(ctx, &secretmanagerpb.RotateSecretRequest{Parent: sql.GetName()})
			require.NoError(t, err)
			require.Len(t, fake2.calls, 1)
			assert.Equal(t, "db1", fake2.calls[0].instance)
			assert.Equal(t, "app", fake2.calls[0].user)
		})
	}
}
