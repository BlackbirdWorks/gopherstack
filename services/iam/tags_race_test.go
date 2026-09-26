package iam_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// TestGetResourceConcurrentWithUntag proves Get/List for roles, users, and
// policies must not hand back a Tags map Untag mutates in place.
func TestGetResourceConcurrentWithUntag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *iam.InMemoryBackend) (id string)
		reader func(b *iam.InMemoryBackend, id string)
		mutate func(b *iam.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "GetRole races UntagRole",
			setup: func(t *testing.T, b *iam.InMemoryBackend) string {
				t.Helper()

				r, err := b.CreateRole("race-role", "/", "", "")
				require.NoError(t, err)
				require.NoError(t, b.TagRole(r.RoleName, map[string]string{"env": "prod"}))

				return r.RoleName
			},
			reader: func(b *iam.InMemoryBackend, id string) {
				r, err := b.GetRole(id)
				if err != nil {
					return
				}

				for k := range r.Tags {
					_ = k
				}
			},
			mutate: func(b *iam.InMemoryBackend, id string) {
				_ = b.TagRole(id, map[string]string{"env": "prod"})
				_ = b.UntagRole(id, []string{"env"})
			},
		},
		{
			name: "GetUser races UntagUser",
			setup: func(t *testing.T, b *iam.InMemoryBackend) string {
				t.Helper()

				u, err := b.CreateUser("race-user", "/", "")
				require.NoError(t, err)
				require.NoError(t, b.TagUser(u.UserName, map[string]string{"env": "prod"}))

				return u.UserName
			},
			reader: func(b *iam.InMemoryBackend, id string) {
				u, err := b.GetUser(id)
				if err != nil {
					return
				}

				for k := range u.Tags {
					_ = k
				}
			},
			mutate: func(b *iam.InMemoryBackend, id string) {
				_ = b.TagUser(id, map[string]string{"env": "prod"})
				_ = b.UntagUser(id, []string{"env"})
			},
		},
		{
			name: "GetPolicy races UntagPolicy",
			setup: func(t *testing.T, b *iam.InMemoryBackend) string {
				t.Helper()

				p, err := b.CreatePolicy("race-policy", "/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`)
				require.NoError(t, err)
				require.NoError(t, b.TagPolicy(p.Arn, map[string]string{"env": "prod"}))

				return p.Arn
			},
			reader: func(b *iam.InMemoryBackend, id string) {
				p, err := b.GetPolicy(id)
				if err != nil {
					return
				}

				for k := range p.Tags {
					_ = k
				}
			},
			mutate: func(b *iam.InMemoryBackend, id string) {
				_ = b.TagPolicy(id, map[string]string{"env": "prod"})
				_ = b.UntagPolicy(id, []string{"env"})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			id := tt.setup(t, b)

			const iterations = 500

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()

				for range iterations {
					tt.reader(b, id)
				}
			}()

			go func() {
				defer wg.Done()

				for range iterations {
					tt.mutate(b, id)
				}
			}()

			wg.Wait()
		})
	}
}
