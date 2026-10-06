package detective_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/detective"
)

func TestUpdateDatasourcePackages_RepeatKeepsCollectionStart(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		packages []string
	}{
		{name: "restarting a started package", packages: []string{"DETECTIVE_CORE"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := detective.NewInMemoryBackend("000000000000", "us-east-1")
			g, err := b.CreateGraph(nil)
			require.NoError(t, err)
			_, _, err = b.CreateMembers(g.Arn, []detective.Account{{AccountID: "222233334444"}}, "")
			require.NoError(t, err)

			require.NoError(t, b.UpdateDatasourcePackages(g.Arn, tc.packages))
			first, _, err := b.BatchGetGraphMemberDatasources(g.Arn, []string{"222233334444"})
			require.NoError(t, err)

			require.NoError(t, b.UpdateDatasourcePackages(g.Arn, tc.packages))
			second, _, err := b.BatchGetGraphMemberDatasources(g.Arn, []string{"222233334444"})
			require.NoError(t, err)

			pkg := tc.packages[0]
			assert.Equal(t, first[0].IngestHistory[pkg]["STARTED"], second[0].IngestHistory[pkg]["STARTED"])
		})
	}
}
