package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	supportbackend "github.com/blackbirdworks/gopherstack/services/support"
)

func TestQuotasSupportAdapter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resolved bool
	}{
		{name: "open_case", resolved: false},
		{name: "resolved_case", resolved: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sup := supportbackend.NewInMemoryBackend()
			a := quotasSupportAdapter{support: sup}

			id, err := a.OpenCase("subject", "body")
			require.NoError(t, err)
			assert.NotEmpty(t, id)
			assert.False(t, a.CaseResolved(id))

			if tt.resolved {
				cases := sup.DescribeCases(nil, false)
				require.Len(t, cases, 1)
				_, err = sup.ResolveCase(cases[0].CaseID)
				require.NoError(t, err)
				assert.True(t, a.CaseResolved(id))
			}

			assert.False(t, a.CaseResolved("missing"))
		})
	}
}
