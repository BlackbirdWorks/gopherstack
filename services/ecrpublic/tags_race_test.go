package ecrpublic_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecrpublic"
)

// TestRepositoryTagsConcurrentWithUntagResource proves Describe/Create/Delete
// must not hand back a Repository whose Tags map UntagResource mutates in place.
func TestRepositoryTagsConcurrentWithUntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reader func(b *ecrpublic.InMemoryBackend, name, arn string)
		name   string
	}{
		{
			name: "DescribeRepositories all",
			reader: func(b *ecrpublic.InMemoryBackend, _, _ string) {
				repos, err := b.DescribeRepositories("", nil)
				if err != nil {
					return
				}

				for _, r := range repos {
					for k := range r.Tags {
						_ = k
					}
				}
			},
		},
		{
			name: "DescribeRepositories by name",
			reader: func(b *ecrpublic.InMemoryBackend, name, _ string) {
				repos, err := b.DescribeRepositories("", []string{name})
				if err != nil {
					return
				}

				for _, r := range repos {
					for k := range r.Tags {
						_ = k
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ecrpublic.NewInMemoryBackend(testAccountID, testRegion)

			tags := map[string]string{"team": "video", "env": "prod"}

			repo, err := b.CreateRepository("race-repo", nil, tags)
			require.NoError(t, err)

			const iterations = 500

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()

				for range iterations {
					tt.reader(b, repo.RepositoryName, repo.RepositoryArn)
				}
			}()

			go func() {
				defer wg.Done()

				for range iterations {
					_ = b.TagResource(repo.RepositoryArn, map[string]string{"env": "prod"})
					_ = b.UntagResource(repo.RepositoryArn, []string{"env"})
				}
			}()

			wg.Wait()
		})
	}
}
