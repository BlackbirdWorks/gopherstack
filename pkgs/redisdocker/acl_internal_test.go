package redisdocker

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errEcho = errors.New("ERR modifier rejected")

type echoAdmin struct{}

func (echoAdmin) Do(_ context.Context, args ...any) (any, error) {
	return nil, fmt.Errorf("%w: %v", errEcho, args)
}

func (echoAdmin) Close() error { return nil }

func TestApplyACLErrorNamesUserNotPassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		user   string
		policy ACLPolicy
	}{
		{
			name:   "listed user",
			user:   "app",
			policy: ACLPolicy{Users: []ACLUser{{Name: "app", Passwords: []string{"s3cret-pw"}}}},
		},
		{name: "default password", user: "default", policy: ACLPolicy{DefaultPassword: "s3cret-pw"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			g := &Group{
				engine: &Engine{cfg: Config{Dial: func(Target) Admin { return echoAdmin{} }}},
				nodes:  []*node{{}},
			}

			err := g.ApplyACL(context.Background(), tc.policy)
			require.ErrorIs(t, err, ErrACL)
			assert.Contains(t, err.Error(), "set acl user "+tc.user)
			assert.NotContains(t, err.Error(), "s3cret-pw")
		})
	}
}
